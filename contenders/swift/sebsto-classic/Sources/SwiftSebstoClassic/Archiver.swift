import Logging
import NIOCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One downloaded file's bytes plus the byte-budget release amount.
//
// Carries a ByteBuffer (not Data) to keep the bytes in NIO's native
// representation all the way through the pipeline. The downloader iterates
// `response.body` which already yields ByteBuffer; converting to Data was
// causing two allocations + a Sequence iteration per ~64 KB NIO frame
// (~250k iterations across the run). ByteBuffer.writeBuffer is a memcpy
// into a pre-sized backing store — one operation per frame, no iteration.
//
// CRC32 is also computed in the downloader, in parallel across N tasks,
// rather than in the single-threaded zipper.
struct DownloadedFile: Sendable {
    let name: String
    let buffer: ByteBuffer
    let crc32: UInt32
    let releaseBytes: Int
}

// Single-producer / single-consumer async channel that delivers downloaded
// files to the zipper in arrival order. The downloader's byte-budget
// semaphore already bounds memory; this channel is just a queue.
final class FileChannel: @unchecked Sendable {
    let stream: AsyncStream<DownloadedFile>
    private let continuation: AsyncStream<DownloadedFile>.Continuation

    init() {
        var c: AsyncStream<DownloadedFile>.Continuation!
        self.stream = AsyncStream { c = $0 }
        self.continuation = c
    }
    func send(_ item: DownloadedFile) { continuation.yield(item) }
    func finish() { continuation.finish() }
}

// Top-level entry point: lists files, starts the multipart upload, runs the
// download/zip/upload pipeline. Aborts the upload on failure.
func runArchiveJob(s3: S3, job: JobInfo, logger: Logger) async throws {
    let files = try await listFiles(
        s3: s3,
        bucket: job.bucket_name,
        filesPrefix: job.files_prefix,
        logger: logger
    )
    logger.info("archive: \(files.count) source objects")

    let upload = try await startMultipartUpload(
        s3: s3,
        bucket: job.bucket_name,
        key: job.archive_key,
        logger: logger
    )
    logger.info("archive: multipart upload started (id=\(upload.uploadId))")

    do {
        let parts = try await runPipeline(
            s3: s3,
            bucket: job.bucket_name,
            files: files,
            upload: upload,
            logger: logger
        )
        try await completeMultipartUpload(s3: s3, upload: upload, parts: parts, logger: logger)
        logger.info("archive: completed (\(parts.count) parts)")
    } catch {
        logger.error("archive: failed, aborting multipart upload: \(error)")
        await abortMultipartUpload(s3: s3, upload: upload, logger: logger)
        throw error
    }
}

// Three-stage pipeline using Swift's structured concurrency. Each stage is a
// child task; when any throws, the others are cancelled.
private func runPipeline(
    s3: S3,
    bucket: String,
    files: [FileInfo],
    upload: MultipartUpload,
    logger: Logger
) async throws -> [S3.CompletedPart] {
    let producer = ChunkProducer(
        chunkSize: Tunables.chunkSize,
        maxInFlight: Tunables.bufferChunksCount
    )
    let byteBudget = ByteSemaphore(capacity: Tunables.maxDownloadsMemory)
    let fileChannel = FileChannel()

    async let downloadDone: Void = runDownloadStage(
        s3: s3,
        bucket: bucket,
        files: files,
        byteBudget: byteBudget,
        out: fileChannel,
        logger: logger
    )
    async let zipDone: Void = runZipStage(
        files: files,
        in: fileChannel,
        producer: producer,
        byteBudget: byteBudget,
        logger: logger
    )
    async let uploadResult: [S3.CompletedPart] = runUploadStage(
        s3: s3,
        producer: producer,
        upload: upload,
        logger: logger
    )

    try await downloadDone
    try await zipDone
    return try await uploadResult
}

// ----- Stage A: downloader -----

private func runDownloadStage(
    s3: S3,
    bucket: String,
    files: [FileInfo],
    byteBudget: ByteSemaphore,
    out: FileChannel,
    logger: Logger
) async throws {
    try await withThrowingTaskGroup(of: Void.self) { group in
        for file in files {
            // Acquire the byte budget *before* spawning so the for-loop itself
            // applies backpressure on file count: at most ~maxDownloadsMemory
            // worth of files are concurrently in-flight.
            await byteBudget.acquire(file.size)
            group.addTask {
                let (buffer, crc) = try await downloadFile(
                    s3: s3, bucket: bucket, key: file.key,
                    expectedSize: file.size, logger: logger
                )
                out.send(DownloadedFile(name: file.name, buffer: buffer, crc32: crc, releaseBytes: file.size))
            }
        }
        try await group.waitForAll()
        out.finish()
    }
}

// Streams chunks from getObject() into a pre-sized ByteBuffer. Computes
// CRC32 inline as bytes flow in — parallelizes the CRC work across N
// download tasks instead of bottlenecking it in the single zipper.
//
// Uses ByteBuffer (not Data) to keep the bytes in NIO's native form: the
// hot per-frame path becomes `out.writeBuffer(&frame)` (one memcpy, no
// Sequence iteration), versus the previous `Data.append(contentsOf:)` which
// allocated and copied byte-by-byte.
private func downloadFile(
    s3: S3,
    bucket: String,
    key: String,
    expectedSize: Int,
    logger: Logger
) async throws -> (ByteBuffer, UInt32) {
    let response = try await s3.getObject(
        S3.GetObjectRequest(bucket: bucket, key: key),
        logger: logger
    )
    var out = ByteBufferAllocator().buffer(capacity: expectedSize)
    var crc = CRC32()
    for try await frameBuffer in response.body {
        var frame = frameBuffer
        // CRC over the frame's readable bytes without copying.
        frame.readableBytesView.withContiguousStorageIfAvailable { ptr in
            crc.update(ptr)
        }
        // Memcpy into the per-file accumulator. One op per frame.
        out.writeBuffer(&frame)
    }
    if out.readableBytes != expectedSize {
        throw ArchivingError.downloadShortRead(key: key, expected: expectedSize, got: out.readableBytes)
    }
    return (out, crc.value)
}

// ----- Stage B: zipper -----

private func runZipStage(
    files: [FileInfo],
    in fileChannel: FileChannel,
    producer: ChunkProducer,
    byteBudget: ByteSemaphore,
    logger: Logger
) async throws {
    var entries: [ZipEntry] = []
    entries.reserveCapacity(files.count)
    var offset: UInt64 = 0
    var processed = 0

    for await file in fileChannel.stream {
        let bodySize = UInt64(file.buffer.readableBytes)
        let lfh = ZipHeaders.localFileHeader(name: file.name)
        let lfhOffset = offset
        let dd = ZipHeaders.dataDescriptor(crc32: file.crc32, size: bodySize)

        // One actor hop for the LFH + body + descriptor instead of three.
        // The body goes through as a ByteBuffer so the producer can memcpy
        // the readable bytes directly into its chunk store with no copy
        // through Data / Sequence iteration.
        await producer.appendCompound(lfh: lfh, body: file.buffer, dataDescriptor: dd)
        offset += UInt64(lfh.count) + bodySize + UInt64(dd.count)
        await byteBudget.release(file.releaseBytes)

        entries.append(ZipEntry(
            name: file.name,
            crc32: file.crc32,
            size: bodySize,
            localHeaderOffset: lfhOffset
        ))
        processed += 1
        if processed % 200 == 0 {
            logger.info("zip: \(processed)/\(files.count) entries")
        }
    }

    // Central directory + ZIP64 EOCD + locator + EOCD.
    let cdOffset = offset
    var cd = Data()
    cd.reserveCapacity(entries.count * 80)
    for entry in entries {
        cd.append(ZipHeaders.centralDirectoryHeader(entry))
    }
    await producer.append(cd)
    let cdSize = UInt64(cd.count)
    offset += cdSize

    let zip64Eocd = ZipHeaders.zip64EndOfCentralDirectory(
        entryCount: UInt64(entries.count),
        cdSize: cdSize,
        cdOffset: cdOffset
    )
    let zip64EocdOffset = offset
    await producer.append(zip64Eocd)
    offset += UInt64(zip64Eocd.count)

    await producer.append(ZipHeaders.zip64EndOfCentralDirectoryLocator(zip64EocdOffset: zip64EocdOffset))
    await producer.append(ZipHeaders.endOfCentralDirectory())

    await producer.finish()
}

// ----- Stage C: uploader -----

private func runUploadStage(
    s3: S3,
    producer: ChunkProducer,
    upload: MultipartUpload,
    logger: Logger
) async throws -> [S3.CompletedPart] {
    var completed: [S3.CompletedPart] = []
    try await withThrowingTaskGroup(of: S3.CompletedPart.self) { group in
        var inFlight = 0
        for await chunk in producer.stream {
            if inFlight >= Tunables.maxConcurrentUploads {
                if let p = try await group.next() {
                    completed.append(p)
                    inFlight -= 1
                }
            }
            group.addTask {
                let cp = try await uploadPart(
                    s3: s3,
                    upload: upload,
                    partNumber: chunk.partNumber,
                    data: chunk.data,
                    logger: logger
                )
                await producer.releaseSlot()
                return cp
            }
            inFlight += 1
        }
        while let p = try await group.next() {
            completed.append(p)
        }
    }
    return completed
}
