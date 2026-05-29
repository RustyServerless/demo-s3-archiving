import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Async byte sink that buckets writes into fixed-size chunks and emits them
// for upload. Replaces the Rust reference's SlabRing — same role (decouples
// the synchronous ZIP writer from the asynchronous multipart uploader, with
// backpressure when the uploader lags) but uses Swift's native AsyncStream +
// a counting semaphore instead of a hand-rolled busy-spinning ring.
//
// Memory model: at most `maxInFlight` chunks of `chunkSize` bytes are
// outstanding (= built but not yet uploaded). With chunkSize=10 MiB and
// maxInFlight=4 that's a 40 MiB ceiling for the producer→uploader path.
actor ChunkProducer {
    let chunkSize: Int
    private let maxInFlight: Int

    private var buffer: Data
    private var nextPartNumber: Int = 1
    private var inFlight: Int = 0
    private var slotWaiters: [CheckedContinuation<Void, Never>] = []
    private var closed = false

    private let continuation: AsyncStream<UploadChunk>.Continuation
    nonisolated let stream: AsyncStream<UploadChunk>

    struct UploadChunk: Sendable {
        let partNumber: Int
        let data: Data
    }

    init(chunkSize: Int = 10 * 1024 * 1024, maxInFlight: Int = 4) {
        self.chunkSize = chunkSize
        self.maxInFlight = maxInFlight
        self.buffer = Data()
        self.buffer.reserveCapacity(chunkSize)
        var continuationOut: AsyncStream<UploadChunk>.Continuation!
        self.stream = AsyncStream { c in continuationOut = c }
        self.continuation = continuationOut
    }

    // Append raw bytes to the producer. Suspends if the in-flight ceiling is
    // hit, providing the backpressure that protects Lambda memory.
    func append(_ bytes: Data) async {
        let total = bytes.count
        let room = chunkSize - buffer.count
        // Hot path: the whole input fits in the current chunk. One memcpy
        // via pointer + Data.append(_:count:); no Sequence iteration.
        if total <= room {
            bytes.withUnsafeBytes { raw in
                if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                    buffer.append(base, count: total)
                }
            }
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
            return
        }
        // Spill path: input crosses one or more chunk boundaries.
        var cursor = 0
        while cursor < total {
            let stillRoom = chunkSize - buffer.count
            let take = Swift.min(total - cursor, stillRoom)
            bytes.withUnsafeBytes { raw in
                if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                    buffer.append(base + cursor, count: take)
                }
            }
            cursor += take
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
        }
    }

    // ByteBuffer-aware append: memcpy a NIO ByteBuffer's readable bytes
    // into the chunk store without going through Data / Sequence iteration.
    // Used for file payloads on the hot path (5 MB per call × 3000 files).
    func append(_ byteBuf: ByteBuffer) async {
        let total = byteBuf.readableBytes
        guard total > 0 else { return }
        let room = chunkSize - buffer.count

        // Hot path: file fits in current chunk.
        if total <= room {
            byteBuf.withUnsafeReadableBytes { raw in
                if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                    buffer.append(base, count: total)
                }
            }
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
            return
        }
        // Spill path.
        var cursor = 0
        while cursor < total {
            let stillRoom = chunkSize - buffer.count
            let take = Swift.min(total - cursor, stillRoom)
            byteBuf.withUnsafeReadableBytes { raw in
                if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                    buffer.append(base + cursor, count: take)
                }
            }
            cursor += take
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
        }
    }

    // Coalesced LFH + body + data descriptor: one actor hop instead of
    // three for every file. The body comes in as ByteBuffer (zero-copy from
    // the downloader's accumulator), the headers as small Data (encoded by
    // ZipHeaders).
    func appendCompound(lfh: Data, body: ByteBuffer, dataDescriptor: Data) async {
        await append(lfh)
        await append(body)
        await append(dataDescriptor)
    }

    // Mark the producer as done. Emits any remaining partial chunk and closes
    // the stream so the consumer's for-await loop terminates.
    func finish() async {
        if !buffer.isEmpty {
            await emitFullChunk()
        }
        closed = true
        continuation.finish()
    }

    // Called by the uploader when it has finished sending a chunk so the
    // producer can continue building the next ones.
    func releaseSlot() {
        if inFlight > 0 { inFlight -= 1 }
        if let w = slotWaiters.first {
            slotWaiters.removeFirst()
            w.resume()
        }
    }

    private func emitFullChunk() async {
        await waitForSlot()
        let chunk = UploadChunk(partNumber: nextPartNumber, data: buffer)
        nextPartNumber += 1
        inFlight += 1
        buffer = Data()
        buffer.reserveCapacity(chunkSize)
        continuation.yield(chunk)
    }

    private func waitForSlot() async {
        if inFlight < maxInFlight { return }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            slotWaiters.append(cont)
        }
    }
}
