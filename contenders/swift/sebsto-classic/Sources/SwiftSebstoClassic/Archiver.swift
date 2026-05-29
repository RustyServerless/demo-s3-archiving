import Logging
import NIOCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One downloaded file's bytes plus the byte-budget release amount.
// CRC32 is now computed in the downloader (parallel, scaling with N download
// tasks), not in the zipper (single-threaded bottleneck).
struct DownloadedFile: Sendable {
    let name: String
    let bytes: Data
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
                let data = try await downloadFile(s3: s3, bucket: bucket, key: file.key, expectedSize: file.size, logger: logger)
                // Compute CRC32 here (in parallel with all other downloads)
                // rather than in the single-threaded zipper. With N download
                // tasks this trades one bottleneck for N-way parallelism.
                var crc = CRC32()
                data.withUnsafeBytes { rawBuf in
                    crc.update(rawBuf.bindMemory(to: UInt8.self))
                }
                out.send(DownloadedFile(name: file.name, bytes: data, crc32: crc.value, releaseBytes: file.size))
            }
        }
        try await group.waitForAll()
        out.finish()
    }
}

// Streams chunks from getObject() into a pre-sized Data — mirrors the Rust
// author's "no body.collect()" trick to avoid AWS SDK 3× memory amplification.
private func downloadFile(
    s3: S3,
    bucket: String,
    key: String,
    expectedSize: Int,
    logger: Logger
) async throws -> Data {
    let response = try await s3.getObject(
        S3.GetObjectRequest(bucket: bucket, key: key),
        logger: logger
    )
    var data = Data()
    data.reserveCapacity(expectedSize)
    for try await buffer in response.body {
        var b = buffer
        if let bytes = b.readBytes(length: b.readableBytes) {
            data.append(contentsOf: bytes)
        }
    }
    if data.count != expectedSize {
        throw ArchivingError.downloadShortRead(key: key, expected: expectedSize, got: data.count)
    }
    return data
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
        let lfh = ZipHeaders.localFileHeader(name: file.name)
        let lfhOffset = offset
        let dd = ZipHeaders.dataDescriptor(crc32: file.crc32, size: UInt64(file.bytes.count))

        // Single actor hop per file instead of three: pass LFH + body + data
        // descriptor as one batched call. Saves ~2 actor crossings per file
        // = ~6000 fewer suspension points across 3000 files.
        await producer.appendMany([lfh, file.bytes, dd])
        offset += UInt64(lfh.count) + UInt64(file.bytes.count) + UInt64(dd.count)
        await byteBudget.release(file.releaseBytes)

        entries.append(ZipEntry(
            name: file.name,
            crc32: file.crc32,
            size: UInt64(file.bytes.count),
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
