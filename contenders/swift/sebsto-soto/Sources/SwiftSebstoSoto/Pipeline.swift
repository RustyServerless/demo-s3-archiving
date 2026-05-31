import Logging
import NIOCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Tunables.
//
// History:
//   - Run-5: downloader-bound (1256 s of summed download work vs 4 s
//     zipper). Per-task GET ~12 MB/s vs Rust's ~35 MB/s.
//   - Run-7: raised AsyncHTTPClient pool ceiling 8 → 32 (in main.swift).
//   - Run-6: tried `maxDownloadsMemory` 20 → 40 MiB; OOM'd at 511 MB.
//   - C1+C2.5: ByteBuffer end-to-end on upload + pre-sized download
//     buffer; reclaimed RSS to ~417 MB.
//   - **C3 reverted**: bumping budget to 32 MiB doubled mean in-flight
//     (1.96 → 4.16) but only saved 2.2 s wall-clock — and Max Memory
//     Used jumped 417 → 490 MB. Cold-2 OOM-killed. The S3 bandwidth was
//     already saturated; more concurrency just spreads it thinner
//     (per-task p50 doubled 376 ms → 626 ms). Net: tiny speed win, big
//     OOM risk. Reverted to 20 MiB.
// Lambda configures vCPU proportionally to memory: at 1769 MB you get
// 1 full vCPU, so at 512 MB you get ~0.29 vCPU and at 1024 MB ~0.58.
// More memory also means more headroom for in-flight downloads. We
// scale `maxDownloadsMemory` accordingly, reading the actual memory
// allocation from the standard Lambda runtime env var
// `AWS_LAMBDA_FUNCTION_MEMORY_SIZE` (set by the Lambda service —
// documented at
// https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html).
//
// Heuristic: keep ~4% of the configured memory available for downloads.
// At 512 MB → 20 MiB. At 1024 MB → 40 MiB. At 1769 MB → ~70 MiB.
// Falls back to 20 MiB on local builds where the env var is absent.
let lambdaMemoryMB: Int = {
    guard let s = ProcessInfo.processInfo.environment["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"],
          let n = Int(s) else { return 512 }
    return n
}()

enum Tunables {
    // ~4% of configured Lambda memory, clamped to a 20 MiB floor.
    static let maxDownloadsMemory: Int = max(20, lambdaMemoryMB * 4 / 100) * 1024 * 1024
    static let maxConcurrentUploads: Int = 3
    static let chunkSize: Int = 10 * 1024 * 1024            // 10 MiB
    // R1: matches Rust reference (BUFFER_CHUNKS_COUNT=2). Caps producer→
    // uploader path at 2 × 10 MiB = 20 MiB.
    static let bufferChunksCount: Int = 2                   // ChunkProducer in-flight ceiling
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
    data: ByteBuffer,
    logger: Logger
) async throws -> S3.CompletedPart {
    // AWSHTTPBody(buffer:) is zero-copy — Soto wraps the ByteBuffer
    // directly and AsyncHTTPClient streams it without re-copying.
    // See PERF-PLAN.md H2 finding (verified by reading
    // soto-core/Sources/SotoCore/HTTP/AWSHTTPBody.swift:40).
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
