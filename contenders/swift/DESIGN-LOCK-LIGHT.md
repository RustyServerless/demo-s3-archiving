# Design — re-evaluating NIOLock and Apple Span for the Swift contenders

Status: proposal, awaiting the 2c8102a (ByteBuffer-on-classic) benchmark
Scope: `swift-sebsto-classic` (3-stage pipeline) and `swift-sebsto` (predicted-layout)

## 1. Executive summary

The right next move is the same in both contenders: **stop using actors as locks**.
The contenders are not waiting on I/O on the hot path — they are paying a Swift
concurrency hop (~1 µs of enqueue/await/resume on the cooperative pool) for
operations whose critical section is a `memcpy` of a few KB. Swap each "small,
synchronous, ordered mutation" actor for a `final class … : Sendable` whose
state lives in `NIOLockedValueBox<State>`. For sebsto-classic, this is a
single-box rewrite of `ChunkProducer` with an `AsyncStream` on the output edge.
For sebsto, this is a per-part array `[NIOLockedValueBox<PartSlot>]` so two
downloaders writing different parts never serialise on the same primitive.
**Apple Span is not the lever** in either contender — see §3 — but the move to
NIOLock makes Span available locally if we want it later.

## 2. NIOLock vs `Synchronization.Mutex` — pick one

I'd use `NIOLockedValueBox<State>`. Reasoning, grounded in the actual sources we
fetched:

- **Underlying primitive.** `NIOLock.swift` declares
  `typealias LockPrimitive = pthread_mutex_t` on Linux/Darwin (Windows uses
  `SRWLOCK`, FreeBSD uses the optional form). It is *not* a futex wrapper and
  it is *not* `os_unfair_lock`. The doc comment on the file explicitly says
  "A threading lock based on `libpthread` instead of `libdispatch`." Lock /
  unlock go through `pthread_mutex_lock` / `pthread_mutex_unlock` via the
  internal `LockOperations` enum, with debug builds setting
  `PTHREAD_MUTEX_ERRORCHECK`. On Linux glibc, an uncontended
  `pthread_mutex_lock` is a single `lock cmpxchg` (the futex syscall only fires
  on contention), so we are looking at ~10–30 ns per `withLockedValue` on
  aarch64, versus ~1 µs minimum for an `await` on an actor that lives on the
  cooperative pool. Roughly two orders of magnitude.

- **Storage shape.** `NIOLockedValueBox<Value>` is a `public struct` whose only
  field is `internal let _storage: LockStorage<Value>`. `LockStorage` is a
  `final class` inheriting from `ManagedBuffer<Value, LockPrimitive>` so the
  pthread mutex is allocated *inline* with the value in a single trailing-
  elements buffer — one allocation, no second indirection on the hot path.
  Critically, the box itself is a struct, but it has reference semantics; that
  is exactly what we want for a chunk-builder that several downloader tasks
  share.

- **Sendable.** `extension NIOLockedValueBox: @unchecked Sendable where Value:
  Sendable {}`. The box is sendable across actor boundaries iff the wrapped
  state is sendable. For us, the state is `(ByteBuffer, Int /* partNumber */,
  Int /* inFlight */, …)` — all `Sendable`. No special handling needed.

- **`~Copyable`?** No. `NIOLockedValueBox` is not `~Copyable`; it relies on its
  reference-typed storage class. The `Synchronization.Mutex<Value>` shipped in
  Swift 6 *is* `~Copyable`, which is theoretically nicer (compile-time
  prevention of accidental copies of the lock). I still prefer `NIOLockedValueBox`
  for two reasons: (a) we are deeply in the NIO world already (ByteBuffer is
  the carrier; AsyncHTTPClient and Soto pull NIOConcurrencyHelpers in
  transitively), so we add no dependency surface, and (b) `Mutex<Value>` being
  `~Copyable` makes it awkward to share by reference — you typically end up
  wrapping it in a class anyway, which is exactly what `NIOLockedValueBox`
  already is. The `Mutex`/`~Copyable` ergonomic tax exceeds the type-safety
  benefit for our use case.

- **Pathological case for both.** A producer holding the lock during a
  multi-MB memcpy stalls *every other producer that wants that same lock*.
  In sebsto-classic, the central-chunk lock is contended by every downloader,
  so a 5 MB memcpy at ~10 GB/s = 500 µs of held lock per file. With 3000
  files, that's 1.5 s of *ideal* serial-memcpy time — fine, that's effectively
  the irreducible work of the zipper stage. The risk is *not* the memcpy
  itself but *not yielding* to the cooperative pool while holding the lock:
  do not call `await` inside `withLockedValue`. The closure type rules that
  out anyway (it's `(inout Value) throws -> T`, not `async`), so the compiler
  protects us.

- **Ordering.** `NIOLock` provides mutual exclusion only — no FIFO. For the
  sebsto v2 predicted-layout design, that is fine: each part is independent
  and is sealed when its byte counter reaches the part size. For
  sebsto-classic the zipper stage is *single-threaded already* (it consumes
  `fileChannel.stream` in arrival order before calling the chunk producer),
  so the producer never needs FIFO across producers — only one caller writes
  to it at a time. The `AsyncStream` on the output edge preserves emit order
  by construction (it is a single-writer `Continuation`).

## 3. Apple Span — honest re-evaluation

Span is the wrong tool to chase here, and I want to be explicit about why so we
stop coming back to it. `Span<UInt8>`, `RawSpan`, and `MutableSpan<UInt8>` are
`~Escapable`: they live for one synchronous scope and cannot be stored, captured
in an escaping closure, or returned without lifetime-dependency annotations.
The previous "they can't cross actor boundaries" framing was right but framed
the wrong axis. The real question is: *inside* a synchronous critical section
(NIOLock's closure body, or `ByteBuffer.withUnsafeReadableBytes`), would using
Span instead of `UnsafeRawBufferPointer` save us anything? The answer is "not
on the hot path." `NIOCore.ByteBuffer` does not yet expose a stable
`readableBytesSpan` API in the version we pin (it's available on newer NIO,
and only as a thin wrapper over the same underlying pointer); `Data.span`
similarly delegates. The codegen for `withUnsafeBytes { ptr in
buffer.append(ptr.baseAddress!, count: n) }` is already a direct memcpy with
no allocation — Span would save *zero* runtime instructions in that closure.

Where Span is *interesting* but not worth the churn now: it would let us write
a `func append(_ span: RawSpan)` signature on the new lock-backed
`ChunkProducer` instead of `func append(_ buf: UnsafeRawBufferPointer)` or
`func append(_ buf: ByteBuffer)`. That is a *type-safety* win (no
`assumingMemoryBound`, no closure pyramid), not a *throughput* win. The
SE-0447 promise is "the same codegen as `UnsafeBufferPointer`, with safety";
Apple is explicit that Span is not faster than what we already have, just
safer. Concrete answer to the four sub-questions: (1) yes, we *could* use
Span inside `withLockedValue`; (2) yes, `MutableSpan<UInt8>` over the chunk
backing storage + `RawSpan` from the source could replace the `withUnsafeBytes`
closure; (3) no allocation is eliminated — `Data.append(_:count:)` already
does in-place memcpy; (4) it is syntactic sugar for our hot path. **Defer
Span until after the NIOLock change lands and is measured.** If at that point
the hot path is still allocation-bound somewhere we missed, Span becomes
relevant; today it is not.

## 4. Concrete redesign for sebsto-classic (with 2c8102a as the baseline)

Assumes the in-flight 2c8102a change (ByteBuffer-backed memcpy on append) holds
up. Goal: remove the actor hop on every `append` / `appendCompound` call.
Currently the zip stage does `await producer.appendCompound(...)` once per
file (3000 × 1 hop) plus `await producer.append(cd)` for the central
directory. That is small (3001 hops total), so the *direct* win in
sebsto-classic from removing actor hops is modest — the *indirect* win is
that we stop allocating a `Task` continuation per call. Pseudocode:

```swift
final class ChunkProducer: Sendable {
    private struct State {
        var buffer: ByteBuffer            // pre-sized to chunkSize
        var nextPartNumber: Int
        var inFlight: Int
        var slotWaiters: [CheckedContinuation<Void, Never>]
        var closed: Bool
    }
    private let state: NIOLockedValueBox<State>
    private let continuation: AsyncStream<UploadChunk>.Continuation
    let stream: AsyncStream<UploadChunk>
    let chunkSize: Int
    let maxInFlight: Int

    // Hot path: synchronous, no `await`. Returns the chunk to emit (if any)
    // so that yield + slot-wait happen *outside* the lock.
    func append(_ src: ByteBuffer) async {
        var pending: UploadChunk? = nil
        state.withLockedValue { s in
            s.buffer.writeImmutableBuffer(src)         // memcpy, no alloc
            if s.buffer.readableBytes >= chunkSize {
                pending = self.cutChunk(&s)            // see below; no I/O
            }
        }
        if let chunk = pending {
            await self.waitForSlot()                   // suspends if needed
            continuation.yield(chunk)
        }
    }

    private func cutChunk(_ s: inout State) -> UploadChunk {
        let slice = s.buffer.readSlice(length: chunkSize)!  // pointer move
        let cp = UploadChunk(partNumber: s.nextPartNumber, body: slice)
        s.nextPartNumber += 1
        // do NOT touch inFlight here; that's done after waitForSlot
        return cp
    }
    // releaseSlot, finish, waitForSlot — all withLockedValue + resume waiters
    // outside the closure
}
```

Key invariants: (1) the lock is held only across the memcpy and the
ByteBuffer slice math; (2) `await` only happens *after* `withLockedValue`
returns; (3) `UploadChunk.body` carries a `ByteBuffer` (zero-copy slice into
the producer's accumulator), not `Data`. The S3 upload path already accepts
`AWSHTTPBody(buffer:)`.

Expected gain on sebsto-classic: small — single-digit seconds — because the
actor wasn't the bottleneck there. The bottleneck was per-frame `Data.append`
in the downloader, which 2c8102a addresses. Do this rewrite anyway because
it's a *pre-requisite* for the sebsto v2 redesign below; sharing the design
keeps the contenders comparable.

## 5. Concrete redesign for sebsto v2 (predicted-layout)

This is where the lock-light pattern earns its keep. Today's bottleneck is
per-NIO-frame `await partActor.write(offset:bytes:)`: ~250k frames × ~1 µs =
~250 ms of *measurable* hop overhead, plus ~250k continuation allocations
that the GC/ARC has to chase. Replace the central actor with a fixed-size
array of independent locks, one per planned part:

```swift
struct PartSlot {                       // wrapped in NIOLockedValueBox
    var buffer: ByteBuffer              // pre-sized to part size
    var bytesWritten: Int               // 0..<partSize
    var sealed: Bool
}

final class PartTable: Sendable {
    let slots: [NIOLockedValueBox<PartSlot>]   // .count == plannedParts
    let partSize: Int
    let onSealed: @Sendable (Int /* partIndex */, ByteBuffer) -> Void

    // Called by downloader tasks — synchronous on the hot path.
    func write(partIndex: Int, partOffset: Int, frame: ByteBuffer) {
        var sealedBuf: ByteBuffer? = nil
        slots[partIndex].withLockedValue { slot in
            slot.buffer.setBuffer(frame, at: partOffset)   // memcpy
            slot.bytesWritten += frame.readableBytes
            if slot.bytesWritten == partSize && !slot.sealed {
                slot.sealed = true
                sealedBuf = slot.buffer
            }
        }
        if let buf = sealedBuf { onSealed(partIndex, buf) }
    }
}
```

Per-task local accumulator: each downloader task owns a `ByteBuffer`
accumulator for *its current file*, fills it from `response.body` with no
locking at all (the task is the sole writer), and only calls
`PartTable.write` when it has a coalesced slab of, say, 1 MB to commit. This
turns ~250k lock acquisitions into ~3000 × (avg parts per file) ≈ 5–10k.
At ~30 ns each, that is microseconds, not milliseconds.

Why this is correct without ordering: predicted layout means each downloader
already knows the absolute byte offset of every byte it will write (LFH +
body + DD slots are pre-computed in the plan). Two downloaders never write
to the same offset; the only contention point is two downloaders that share
a part (a file straddles a part boundary, or two small files land in the
same part). The per-part lock isolates that contention to exactly the parts
that need it. Sealing is monotonic: the slot transitions to `sealed = true`
exactly once, when the writer that completes the byte count observes it.

The `onSealed` callback hands the part off to the upload stage via an
`AsyncStream.Continuation` (the upload stage drains in any order — S3
multipart accepts parts out of order as long as the final `CompleteMultipart`
manifest is sorted, which it already is in `Pipeline.swift`).

## 6. Risks and open questions

- **2c8102a may already close most of the gap on classic.** If the
  ByteBuffer-on-classic run comes back at, say, 240 s, the marginal value of
  the NIOLock rewrite on classic is small and we should redirect that work
  to v2. Wait for the number before rewriting classic.
- **Per-task accumulator memory.** With N download tasks each holding up to
  the largest-file size in flight, the working set grows. Cap it (already
  done via `ByteSemaphore`) and ensure the accumulator is a *slice* of the
  byte budget, not on top of it.
- **`ByteBuffer.setBuffer(_:at:)` requires the destination to already be
  sized.** Pre-allocate each `PartSlot.buffer` with `writeRepeatingByte(0,
  count: partSize)` (or reserve + advance writer index) at plan time so
  random-offset writes are valid. This is a one-time cost per part.
- **Lock held during a multi-MB memcpy.** If a downloader hands a single
  10 MB frame to `setBuffer`, that's ~1 ms of held lock. Cap commit size at
  the per-task accumulator boundary (1 MB) to keep tail latency bounded.
- **What to measure first**, in order: (1) 2c8102a end-to-end on classic;
  (2) `perf record` on classic to confirm the residual is no longer in
  `Data.append` / actor hop; (3) prototype `PartTable` for v2 on a single
  10-file job and compare frame-write latency vs the actor version with a
  histogram, *not* a wall-clock; (4) only then run a full 3000-file
  benchmark.
- **Don't bring in Span yet.** Re-evaluate after (1)–(3). If the residual
  is still in user-mode allocation, Span enters the picture; if it's in
  S3 throughput, we have nothing left to optimise here.
