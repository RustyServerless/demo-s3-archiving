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

    // Pre-reserve the per-stage sample arrays so a STATS=1 run doesn't
    // include array reallocation in its measurements. estimatedParts is a
    // ceiling — the real archive lands a bit under (chunkSize=10 MiB,
    // ~17 GiB total → ~1700 parts).
    let totalBytes = files.reduce(0) { $0 + $1.size }
    let estimatedParts = max(1, totalBytes / Tunables.chunkSize + 8)
    let stats = Stats(estimatedFiles: files.count, estimatedParts: estimatedParts)
    do {
        let parts = try await runPipeline(
            s3: s3,
            bucket: job.bucket_name,
            files: files,
            upload: upload,
            stats: stats,
            logger: logger
        )
        try await completeMultipartUpload(s3: s3, upload: upload, parts: parts, logger: logger)
        logger.info("archive: completed (\(parts.count) parts)")
        stats.report(logger: logger)
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
    stats: Stats,
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
        stats: stats,
        logger: logger
    )
    async let zipDone: Void = runZipStage(
        files: files,
        in: fileChannel,
        producer: producer,
        byteBudget: byteBudget,
        stats: stats,
        logger: logger
    )
    async let uploadResult: [S3.CompletedPart] = runUploadStage(
        s3: s3,
        producer: producer,
        upload: upload,
        stats: stats,
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
    stats: Stats,
    logger: Logger
) async throws {
    try await withThrowingTaskGroup(of: Void.self) { group in
        for file in files {
            // Acquire the byte budget *before* spawning so the for-loop itself
            // applies backpressure on file count: at most ~maxDownloadsMemory
            // worth of files are concurrently in-flight.
            await byteBudget.acquire(file.size)
            group.addTask {
                stats.incrementInFlight()
                let t0: UInt64 = Stats.enabled ? monoNs() : 0
                let (buffer, crc) = try await downloadFile(
                    s3: s3, bucket: bucket, key: file.key,
                    expectedSize: file.size, stats: stats, logger: logger
                )
                if Stats.enabled { stats.record(.downloadFile, ns: monoNs() - t0) }
                stats.decrementInFlight()
                out.send(DownloadedFile(name: file.name, buffer: buffer, crc32: crc, releaseBytes: file.size))
            }
        }
        try await group.waitForAll()
        out.finish()
    }
}

// Streams the body into a pre-sized ByteBuffer, then runs CRC32 once
// over the full readable view at end-of-file.
//
// History:
//   - Run 10/11: streaming with per-frame CRC (multiple short ccrc32
//     calls per file) — the ARMv8 __crc32{b,h,w,d} intrinsics are
//     fastest on long contiguous runs.
//   - Run 13 (C2): switched to `response.body.collect(upTo:)` — single
//     code path, single CRC pass, but +13 MB peakRSS vs C1 because
//     `collect` grows its accumulator by doubling and the freed
//     intermediate buffers don't return to the OS (NIO/glibc arena
//     retention).
//   - Run 14 (C2.5, this code): pre-allocate the destination
//     ByteBuffer at exactly `expectedSize` (no doubling-growth, no
//     intermediate frees), stream into it, CRC once at end. Keeps C2's
//     code-simplification wins (single path, whole-file CRC) while
//     eliminating C2's allocation churn.
//
// Memory model is now deterministic: each in-flight download owns
// exactly one expectedSize-byte ByteBuffer. With maxDownloadsMemory =
// 20 MiB ÷ ~5 MB ≈ 4 concurrent downloads, that's ~20 MiB of body
// storage at any moment, predictable for raising the byte budget in C3.
private func downloadFile(
    s3: S3,
    bucket: String,
    key: String,
    expectedSize: Int,
    stats: Stats,
    logger: Logger
) async throws -> (ByteBuffer, UInt32) {
    let response = try await s3.getObject(
        S3.GetObjectRequest(bucket: bucket, key: key),
        logger: logger
    )
    var out = ByteBufferAllocator().buffer(capacity: expectedSize)
    for try await frameBuffer in response.body {
        var frame = frameBuffer
        out.writeBuffer(&frame)
    }
    if out.readableBytes != expectedSize {
        throw ArchivingError.downloadShortRead(key: key, expected: expectedSize, got: out.readableBytes)
    }
    var crc = CRC32()
    if Stats.enabled {
        let t0 = monoNs()
        out.readableBytesView.withContiguousStorageIfAvailable { ptr in
            crc.update(ptr)
        }
        stats.record(.downloadInFrame, ns: monoNs() - t0)
    } else {
        out.readableBytesView.withContiguousStorageIfAvailable { ptr in
            crc.update(ptr)
        }
    }
    return (out, crc.value)
}

// ----- Stage B: zipper -----

private func runZipStage(
    files: [FileInfo],
    in fileChannel: FileChannel,
    producer: ChunkProducer,
    byteBudget: ByteSemaphore,
    stats: Stats,
    logger: Logger
) async throws {
    var entries: [ZipEntry] = []
    entries.reserveCapacity(files.count)
    var offset: UInt64 = 0
    var processed = 0

    // Time the zipper *waits* on the next downloaded file (zipperQueueWait):
    // high p50 = downloader bottleneck. Time inside the chunk producer
    // (zipperAppend): high p50 = uploader bottleneck pushing back through
    // the producer.
    var queueWaitStart: UInt64 = Stats.enabled ? monoNs() : 0
    for await file in fileChannel.stream {
        if Stats.enabled { stats.record(.zipperQueueWait, ns: monoNs() - queueWaitStart) }

        let bodySize = UInt64(file.buffer.readableBytes)
        let lfh = ZipHeaders.localFileHeader(name: file.name)
        let lfhOffset = offset
        let dd = ZipHeaders.dataDescriptor(crc32: file.crc32, size: bodySize)

        let appendStart: UInt64 = Stats.enabled ? monoNs() : 0
        await producer.appendCompound(lfh: lfh, body: file.buffer, dataDescriptor: dd)
        if Stats.enabled {
            stats.record(.zipperAppend, ns: monoNs() - appendStart)
            // appendCompound today decomposes to 3 nested `await append(...)`
            // calls — 3 actor hops per file. Counter proves it (or proves
            // a future fix). Tests H3 in PERF-PLAN.md.
            stats.bumpProducerHops(3)
        }

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
        if Stats.enabled { queueWaitStart = monoNs() }
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
    stats: Stats,
    logger: Logger
) async throws -> [S3.CompletedPart] {
    var completed: [S3.CompletedPart] = []
    try await withThrowingTaskGroup(of: S3.CompletedPart.self) { group in
        var inFlight = 0
        // Time the uploader waits for the next sealed chunk: high p50 here
        // = chunks aren't ready (zipper-bound). Low p50 = upload pool is
        // saturated and chunks queue up.
        var queueWaitStart: UInt64 = Stats.enabled ? monoNs() : 0
        for await chunk in producer.stream {
            if Stats.enabled { stats.record(.uploaderQueueWait, ns: monoNs() - queueWaitStart) }

            if inFlight >= Tunables.maxConcurrentUploads {
                if let p = try await group.next() {
                    completed.append(p)
                    inFlight -= 1
                }
            }
            group.addTask {
                let t0: UInt64 = Stats.enabled ? monoNs() : 0
                let cp = try await uploadPart(
                    s3: s3,
                    upload: upload,
                    partNumber: chunk.partNumber,
                    data: chunk.data,
                    logger: logger
                )
                if Stats.enabled { stats.record(.uploadPart, ns: monoNs() - t0) }
                await producer.releaseSlot()
                return cp
            }
            inFlight += 1
            if Stats.enabled { queueWaitStart = monoNs() }
        }
        while let p = try await group.next() {
            completed.append(p)
        }
    }
    return completed
}
