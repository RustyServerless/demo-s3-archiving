import Logging
import NIOCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Tuned for the default 512 MB / arm64 Lambda configuration. Together
// these caps keep total in-flight memory at ~80 MiB, well within budget.
enum Tunables {
    /// Total bytes of in-flight downloads. With files averaging ~5 MB,
    /// this gates concurrency to ~4 simultaneous downloads.
    static let maxDownloadsMemory: Int = 20 * 1024 * 1024   // 20 MiB

    /// Cap on concurrent `S3.uploadPart` calls.
    static let maxConcurrentUploads: Int = 3

    /// Multipart-upload part size. S3 allows 5 MiB–5 GiB per part; 10 MiB
    /// keeps the part count moderate (~1500 for a 15 GiB archive)
    /// without buffering too much.
    static let chunkSize: Int = 10 * 1024 * 1024            // 10 MiB

    /// Maximum number of sealed-but-not-yet-uploaded chunks held by the
    /// `ChunkProducer`. Caps the producer→uploader path at
    /// `bufferChunksCount × chunkSize` = 20 MiB.
    static let bufferChunksCount: Int = 2
}

struct FileInfo: Sendable {
    let name: String
    let key: String
    let size: Int
}

struct JobInfo: Codable, Sendable {
    let bucket_name: String
    let files_prefix: String
    let archive_key: String
}

/// Counting semaphore over a byte budget. Throttles the total bytes of
/// in-flight downloads. `acquire(_:)` suspends when the budget is
/// exhausted; `release(_:)` wakes the front waiter (or several, in
/// order) once enough capacity is back.
actor ByteSemaphore {
    private let capacity: Int
    private var available: Int
    private var waiters: [(needed: Int, cont: CheckedContinuation<Void, Never>)] = []

    init(capacity: Int) {
        self.capacity = capacity
        self.available = capacity
    }

    func acquire(_ amount: Int) async {
        // Cap the request at full capacity — a single file may exceed the
        // budget on its own; we still want it to make progress (it will
        // block other downloads while it holds the whole budget).
        let needed = min(amount, capacity)
        if available >= needed {
            available -= needed
            return
        }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            waiters.append((needed, cont))
        }
        available -= needed
    }

    func release(_ amount: Int) {
        let toRelease = min(amount, capacity)
        available += toRelease
        // Wake waiters in FIFO order so a large waiter at the head
        // doesn't get starved by an unbounded stream of smaller ones.
        while let head = waiters.first, available >= head.needed {
            waiters.removeFirst()
            head.cont.resume()
        }
    }
}

// ----- Listing -----

func listFiles(s3: S3, bucket: String, filesPrefix: String, logger: Logger) async throws -> [FileInfo] {
    let prefix = filesPrefix + "/"
    let request = S3.ListObjectsV2Request(bucket: bucket, prefix: prefix)
    var files: [FileInfo] = []
    for try await page in s3.listObjectsV2Paginator(request, logger: logger) {
        for object in page.contents ?? [] {
            guard let key = object.key, let size = object.size, !key.hasSuffix("/") else { continue }
            guard key.hasPrefix(prefix) else { continue }
            let name = String(key.dropFirst(prefix.count))
            if name.isEmpty { continue }
            files.append(FileInfo(name: name, key: key, size: Int(size)))
        }
    }
    return files
}

// ----- Multipart upload helpers -----

struct MultipartUpload: Sendable {
    let bucket: String
    let key: String
    let uploadId: String
}

func startMultipartUpload(s3: S3, bucket: String, key: String, logger: Logger) async throws -> MultipartUpload {
    let response = try await s3.createMultipartUpload(
        S3.CreateMultipartUploadRequest(bucket: bucket, contentType: "application/zip", key: key),
        logger: logger
    )
    guard let uploadId = response.uploadId else {
        throw ArchivingError.missingUploadId
    }
    return MultipartUpload(bucket: bucket, key: key, uploadId: uploadId)
}

func uploadPart(
    s3: S3,
    upload: MultipartUpload,
    partNumber: Int,
    data: ByteBuffer,
    logger: Logger
) async throws -> S3.CompletedPart {
    // `AWSHTTPBody(buffer:)` is zero-copy: Soto wraps the ByteBuffer
    // directly and AsyncHTTPClient streams it onto the wire without
    // re-copying. The `bytes:` overload would copy `Data` into a fresh
    // ByteBuffer first.
    let response = try await s3.uploadPart(
        S3.UploadPartRequest(
            body: AWSHTTPBody(buffer: data),
            bucket: upload.bucket,
            contentLength: Int64(data.readableBytes),
            key: upload.key,
            partNumber: partNumber,
            uploadId: upload.uploadId
        ),
        logger: logger
    )
    guard let etag = response.eTag else {
        throw ArchivingError.missingETag(partNumber: partNumber)
    }
    return S3.CompletedPart(eTag: etag, partNumber: partNumber)
}

func completeMultipartUpload(
    s3: S3,
    upload: MultipartUpload,
    parts: [S3.CompletedPart],
    logger: Logger
) async throws {
    let sorted = parts.sorted { ($0.partNumber ?? 0) < ($1.partNumber ?? 0) }
    _ = try await s3.completeMultipartUpload(
        S3.CompleteMultipartUploadRequest(
            bucket: upload.bucket,
            key: upload.key,
            multipartUpload: S3.CompletedMultipartUpload(parts: sorted),
            uploadId: upload.uploadId
        ),
        logger: logger
    )
}

func abortMultipartUpload(s3: S3, upload: MultipartUpload, logger: Logger) async {
    do {
        _ = try await s3.abortMultipartUpload(
            S3.AbortMultipartUploadRequest(
                bucket: upload.bucket,
                key: upload.key,
                uploadId: upload.uploadId
            ),
            logger: logger
        )
    } catch {
        logger.error("abort multipart upload failed: \(error)")
    }
}

// ----- Errors -----

enum ArchivingError: Error, CustomStringConvertible {
    case missingUploadId
    case missingETag(partNumber: Int)
    case downloadShortRead(key: String, expected: Int, got: Int)

    var description: String {
        switch self {
        case .missingUploadId: return "S3 createMultipartUpload returned no uploadId"
        case .missingETag(let n): return "uploadPart \(n) returned no ETag"
        case .downloadShortRead(let k, let e, let g): return "short read on \(k): expected \(e), got \(g)"
        }
    }
}
