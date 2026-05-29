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

        // Hot path: the whole input fits in the current chunk (covers every
        // header / data descriptor / sub-chunk file). One memcpy via
        // withUnsafeBytes + Data.append(_:count:), no slicing.
        if total <= room {
            bytes.withUnsafeBytes { raw in
                if let base = raw.baseAddress {
                    buffer.append(base.assumingMemoryBound(to: UInt8.self), count: total)
                }
            }
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
            return
        }

        // Spill path: input crosses one or more chunk boundaries. memcpy each
        // crossing slice via the same unsafe pointer pattern. We re-enter
        // withUnsafeBytes per slice rather than across the await, since the
        // pointer borrow can't span actor suspensions.
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

    // Coalesced append: same semantics as N consecutive `append` calls but
    // one actor hop instead of N. Used by the zipper to push LFH + body +
    // data descriptor in one go.
    func appendMany(_ blobs: [Data]) async {
        for blob in blobs {
            await append(blob)
        }
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
