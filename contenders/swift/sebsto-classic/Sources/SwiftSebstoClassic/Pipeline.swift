import Logging
import NIOCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Tunables. Initial values mirrored the Rust reference; first benchmark run
// showed the Swift port at 372 s vs Rust's 215 s with peak 452 MB / 512 MB,
// indicating the upload side was under-provisioned (~40 MB/s sustained vs
// Rust's ~70 MB/s). Bumped concurrency to better saturate the symmetric
// 600 Mbps Lambda link while staying under the 512 MB memory ceiling.
enum Tunables {
    static let maxDownloadsMemory: Int = 30 * 1024 * 1024  // 30 MiB
    static let maxConcurrentUploads: Int = 6
    static let chunkSize: Int = 8 * 1024 * 1024             // 8 MiB
    static let bufferChunksCount: Int = 4                   // ChunkProducer in-flight ceiling
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

// Counting semaphore: throttles total in-flight bytes for downloads. Async by
// design — `acquire` suspends when the budget is exhausted.
actor ByteSemaphore {
    private let capacity: Int
    private var available: Int
    private var waiters: [(needed: Int, cont: CheckedContinuation<Void, Never>)] = []

    init(capacity: Int) {
        self.capacity = capacity
        self.available = capacity
    }

    func acquire(_ amount: Int) async {
        // Cap the request at full capacity — a single file may be larger than
        // the budget; we still want the download to make progress (it'll
        // block other downloads while it owns the whole budget).
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
        // Wake the front waiter if its request now fits. We don't reorder,
        // so a huge waiter won't starve smaller ones — that matches the
        // Rust tokio Semaphore semantics for `acquire_many_owned`.
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
    data: Data,
    logger: Logger
) async throws -> S3.CompletedPart {
    let response = try await s3.uploadPart(
        S3.UploadPartRequest(
            body: AWSHTTPBody(bytes: data),
            bucket: upload.bucket,
            contentLength: Int64(data.count),
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
