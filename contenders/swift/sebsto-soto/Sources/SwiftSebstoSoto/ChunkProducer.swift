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
//
// Storage: NIO `ByteBuffer` end-to-end. Soto's `AWSHTTPBody(buffer:)`
// accepts a ByteBuffer zero-copy (see PERF-PLAN.md / RESULTS.md H2 finding);
// using `Data` here would force a Data → ByteBuffer copy on every uploadPart
// call — at 10 MiB × ~1500 parts that's ~15 GiB of avoidable copies and the
// associated NIO ByteBufferAllocator arena pressure (a contributor to the
// warm-run RSS climb).
actor ChunkProducer {
    let chunkSize: Int
    private let maxInFlight: Int

    private let allocator: ByteBufferAllocator
    private var buffer: ByteBuffer
    private var nextPartNumber: Int = 1
    private var inFlight: Int = 0
    private var slotWaiters: [CheckedContinuation<Void, Never>] = []
    private var closed = false

    private let continuation: AsyncStream<UploadChunk>.Continuation
    nonisolated let stream: AsyncStream<UploadChunk>

    struct UploadChunk: Sendable {
        let partNumber: Int
        let data: ByteBuffer
    }

    init(chunkSize: Int = 10 * 1024 * 1024, maxInFlight: Int = 4) {
        self.chunkSize = chunkSize
        self.maxInFlight = maxInFlight
        self.allocator = ByteBufferAllocator()
        self.buffer = self.allocator.buffer(capacity: chunkSize)
        var continuationOut: AsyncStream<UploadChunk>.Continuation!
        self.stream = AsyncStream { c in continuationOut = c }
        self.continuation = continuationOut
    }

    // Append a small Data (used for ZIP headers — LFH, DD, central directory
    // records, EOCDs). Headers are tiny (≤ ~150 B each) so withUnsafeBytes
    // overhead is negligible vs the 10 MiB chunk-size.
    func append(_ bytes: Data) async {
        let total = bytes.count
        guard total > 0 else { return }
        var cursor = 0
        while cursor < total {
            let stillRoom = chunkSize - buffer.readableBytes
            let take = Swift.min(total - cursor, stillRoom)
            bytes.withUnsafeBytes { raw in
                let base = raw.baseAddress!.assumingMemoryBound(to: UInt8.self)
                buffer.writeBytes(UnsafeBufferPointer(start: base + cursor, count: take))
            }
            cursor += take
            if buffer.readableBytes == chunkSize {
                await emitFullChunk()
            }
        }
    }

    // ByteBuffer append — zero-copy memcpy via NIO. Used for file payloads
    // on the hot path (5 MB per call × 3000 files).
    //
    // Hot path (file fits in current chunk): writeImmutableBuffer is one
    // memcpy. Spill path: readSlice splits the source into chunk-sized
    // pieces, each writeBuffer is one memcpy, no allocation per slice
    // (slices are views over the same backing storage).
    func append(_ byteBuf: ByteBuffer) async {
        let total = byteBuf.readableBytes
        guard total > 0 else { return }
        let room = chunkSize - buffer.readableBytes
        if total <= room {
            // Fast path: whole input fits.
            buffer.writeImmutableBuffer(byteBuf)
            if buffer.readableBytes == chunkSize {
                await emitFullChunk()
            }
            return
        }
        // Spill path.
        var src = byteBuf
        while src.readableBytes > 0 {
            let stillRoom = chunkSize - buffer.readableBytes
            let take = Swift.min(src.readableBytes, stillRoom)
            if var slice = src.readSlice(length: take) {
                buffer.writeBuffer(&slice)
            }
            if buffer.readableBytes == chunkSize {
                await emitFullChunk()
            }
        }
    }

    // Coalesced LFH + body + data descriptor: still 3 actor hops today
    // (one per `await append`), but the body path is the hot 5 MB one.
    // C4 in PERF-PLAN.md will collapse this into a single hop.
    func appendCompound(lfh: Data, body: ByteBuffer, dataDescriptor: Data) async {
        await append(lfh)
        await append(body)
        await append(dataDescriptor)
    }

    // Mark the producer as done. Emits any remaining partial chunk and closes
    // the stream so the consumer's for-await loop terminates.
    func finish() async {
        if buffer.readableBytes > 0 {
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
        buffer = allocator.buffer(capacity: chunkSize)
        continuation.yield(chunk)
    }

    private func waitForSlot() async {
        if inFlight < maxInFlight { return }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            slotWaiters.append(cont)
        }
    }
}
