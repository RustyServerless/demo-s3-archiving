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
        var remaining = bytes[...]
        while !remaining.isEmpty {
            let room = chunkSize - buffer.count
            let take = Swift.min(remaining.count, room)
            buffer.append(contentsOf: remaining.prefix(take))
            remaining = remaining.dropFirst(take)
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
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
