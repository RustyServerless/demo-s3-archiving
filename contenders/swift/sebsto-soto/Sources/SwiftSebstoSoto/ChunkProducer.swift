import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Async byte sink that buckets writes into fixed-size chunks and emits
// them on a `nonisolated AsyncStream<UploadChunk>` for the uploader.
// Decouples the synchronous ZIP writer from the asynchronous multipart
// uploader, with backpressure when the uploader can't keep up.
//
// Memory model: at most `maxInFlight` chunks of `chunkSize` bytes are
// outstanding (built but not yet uploaded). With `chunkSize` = 10 MiB
// and `maxInFlight` = 2 that's a 20 MiB ceiling for the producer→
// uploader path.
//
// Storage is NIO `ByteBuffer` end-to-end — `Soto.AWSHTTPBody(buffer:)`
// wraps a ByteBuffer zero-copy on the wire. Going through `Data` would
// force a Data→ByteBuffer copy on every `uploadPart` call, which is
// ~15 GiB of avoidable copies and ByteBufferAllocator arena churn for
// a 15 GiB archive split into 10 MiB parts.
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

    init(chunkSize: Int = 10 * 1024 * 1024, maxInFlight: Int = 2) {
        self.chunkSize = chunkSize
        self.maxInFlight = maxInFlight
        self.allocator = ByteBufferAllocator()
        self.buffer = self.allocator.buffer(capacity: chunkSize)
        var continuationOut: AsyncStream<UploadChunk>.Continuation!
        self.stream = AsyncStream { c in continuationOut = c }
        self.continuation = continuationOut
    }

    /// Append a small `Data` blob — used for ZIP headers (local file header,
    /// data descriptor, central directory records, EOCDs).
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

    /// Append a `ByteBuffer` — used for the hot file-body path. The fast
    /// path (input fits in the current chunk) is a single memcpy via
    /// `writeImmutableBuffer`. The spill path (input crosses one or more
    /// chunk boundaries) splits the source into chunk-sized slices —
    /// `readSlice` is zero-copy because slices view the same backing
    /// storage; each `writeBuffer` is then one memcpy.
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

    /// Append the three byte ranges that make up one ZIP entry —
    /// local file header, body, then data descriptor — in order.
    func appendCompound(lfh: Data, body: ByteBuffer, dataDescriptor: Data) async {
        await append(lfh)
        await append(body)
        await append(dataDescriptor)
    }

    /// Mark the producer as done. Emits any remaining partial chunk and
    /// closes the stream so the consumer's `for await` loop terminates.
    func finish() async {
        if buffer.readableBytes > 0 {
            await emitFullChunk()
        }
        closed = true
        continuation.finish()
    }

    /// Called by the uploader when it has finished sending a chunk so the
    /// producer can build the next ones.
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
