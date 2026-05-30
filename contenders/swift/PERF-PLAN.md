# Swift contender — performance plan (`sebsto-soto`)

Living document. Sibling to `DESIGN.md` (what the code does) and `RESULTS.md`
(numbered run log + lessons). This file is **the plan**: how we go from the
current 1.72×–1.78× gap-to-Rust to something better, without trusting any
prior hypothesis until measured fresh.

Scope: **Soto variant only** (`contenders/swift/sebsto-soto/`). The AWS SDK
sibling branch is out of scope for this plan.

## Working principles

1. **Verify before changing.** Every previous hypothesis in `RESULTS.md`
   is treated as unverified. We re-measure on the current code and current
   instrumentation budget before touching anything.
2. **One change per A/B.** No combined diffs. If two suspected wins ship
   together and the run improves, we cannot attribute the delta.
3. **Each change is judged against a locked baseline.** Phase A produces
   that baseline; Phase C consumes it.
4. **Track every attempted change in `RESULTS.md`** — including the ones we
   revert. The dead-end log is part of the value of this work.
5. **Memory is a hard ceiling.** 512 MB Lambda; Run 6 OOM'd at 511 MB.
   Any change that frees headroom is itself a win because it unlocks more
   in-flight downloads later.

## Suspicions on the table (going in)

User's two upfront suspicions, plus what falls out from a fresh code read:

- **`Data` and copy objects on the hot path.** ChunkProducer's internal
  buffer is `Data` (not `ByteBuffer`); each emitted 10 MiB chunk is then
  passed to `AWSHTTPBody(bytes: data)` — possibly another copy. Per-frame
  zip headers also accumulate in `Data` via `appendLE` (multiple tiny
  appends per record × ~3000 entries).
- **Actors on the hot path.** `ByteSemaphore`, `ChunkProducer`, and
  `Stats` are all actors. `ChunkProducer.appendCompound` is *advertised*
  in code comments as one actor hop per file but is implemented as three
  (`await append(lfh); await append(body); await append(dd)`). That's
  ~9000 hops / file path, plus ~3000 byteBudget hops, plus ~9000 stats
  hops (paid even when STATS=0 because the guard sits inside the actor).
- **Concurrency cap is the byte budget, not the connection pool.** With
  `maxDownloadsMemory = 20 MiB` and ~5 MB files, roughly 4 concurrent
  downloads are in flight on average — not 32. The Run 7 pool=32 win
  may have been a side-effect (fewer transient pool waits during bursts),
  not a structural change.
- **`Stats` actor mutates a `[Stage: [UInt64]]` dictionary** even when
  enabled — every record allocates if the per-stage array grows. We
  should pre-reserve.

None of these are verified yet. That's Phase B.

## Plan

### Phase A — Trustworthy measurement

Goal: produce a baseline + tooling that future changes can be judged
against. No optimization yet.

#### A1. Make Stats truly zero-cost when off

Today:

```swift
actor Stats {
    func record(_ stage: Stage, ns: UInt64) {
        guard statsEnabled else { return }
        samples[stage, default: []].append(ns)
    }
}
// Caller:
let t0 = monoNs()
… work …
await stats.record(.downloadFile, ns: monoNs() - t0)
```

The `await stats.record(...)` pays a full actor hop *before* the guard
runs. With STATS=0 we still hop ~9000+ times across the run.

Change to:

```swift
final class Stats: @unchecked Sendable {
    private let lock = NIOLock()
    private var samples: [Stage: [UInt64]] = [:]
    static let enabled: Bool = …  // computed once at cold start

    @inline(__always)
    func record(_ stage: Stage, ns: UInt64) {
        guard Stats.enabled else { return }
        lock.withLock { samples[stage, default: []].append(ns) }
    }
}
```

…and at every call site:

```swift
if Stats.enabled {
    let t0 = monoNs()
    … work …
    stats.record(.downloadFile, ns: monoNs() - t0)
}
```

Yes, the `if` is duplicated. That's the price of zero overhead when off.
Optionally wrap in a `#if DEBUG_STATS` macro later — for now the explicit
guard is the simplest correct thing.

Pre-reserve `samples[stage]` with `reserveCapacity(files.count)` for the
file-scoped stages and `reserveCapacity(estimatedParts)` for the upload
stage so the `Array` doesn't reallocate during a hot run.

#### A2. New instruments

Add the things we currently can't see:

| Instrument | What it tells us | How |
|---|---|---|
| **In-flight download gauge** | Time-weighted mean + max concurrent download tasks. Tests H1 (byte budget gates concurrency to ~4). | Atomic counter incremented in the download task body, sampled by a small periodic task or recorded as deltas. Time-weighted mean = ∫ inFlight dt / runtime. |
| **Per-frame downloadFile breakdown** | Splits one file's download time into `between-frame` (waiting on AsyncSequence next) vs `in-frame` (CRC + memcpy). Tests H4 (AsyncSequence overhead vs work). | Wrap the `for try await frame in response.body` body with two timers; emit aggregates per file (sum, count). |
| **`ru_maxrss` peak RSS** | Direct read of Lambda VM peak. Confirms that "Max Memory Used" in CloudWatch matches what the process saw. | `getrusage(RUSAGE_SELF, &ru)` at end of run; on Linux `ru_maxrss` is in KB. |
| **`mallinfo2` allocation deltas** | Heap usage between stage boundaries. Tells us whether `Data` churn is real. | Linux glibc only (we're on AL2023). Snapshot at: start, after listFiles, after each 200 files, after pipeline, end. Diff `uordblks`. Macro-gate the call so macOS builds keep compiling. |

All gated on `Stats.enabled`. Reporting goes through the same `report()`.

#### A3. Capture the clean baseline

After A1 + A2 deploy, run **3 cold + 3 warm** invocations with `STATS=1`
on the existing 3000-file / 15 GB bucket. Cold = first invocation after
`update-function-configuration` (bumping the env var forces a new
sandbox). Warm = back-to-back invocations of the same Lambda.

Record in `RESULTS.md` as Run 10 (six-row block) with full stat dump
including the new instruments. Lock these numbers.

**Acceptance**: A1 should not move the wall-clock by more than ±2 s
(STATS=0 path is now strictly cheaper than before).

### Phase B — Verify each hypothesis on the new baseline

For each suspicion, write the experiment, predict the outcome, run, and
record either CONFIRMED / REJECTED in `RESULTS.md`. *Reject the
hypothesis at the first contradicting observation.*

| # | Hypothesis | Measurement | Predicts (if true) |
|---|---|---|---|
| H1 | Byte budget (20 MiB) gates concurrency to ~4, not pool=32 | A2 in-flight gauge | mean in-flight ≈ 4, max ≤ 5 |
| H2 | `AWSHTTPBody(bytes: Data)` copies on every uploadPart | Read Soto `AWSHTTPBody` source; A2 mallinfo delta over the upload stage | uordblks growth ≈ 10 MiB × parts on the upload critical path |
| H3 | `appendCompound` does 3 actor hops/file, not 1 | Add a hop counter inside ChunkProducer | counter == files × 3 |
| H4 | Per-frame AsyncSequence next() overhead dominates downloadFile | A2 per-frame breakdown | between-frame >> in-frame |
| H5 | `Data.appendLE` causes per-byte allocation | mallinfo delta around a 1000-call appendLE microbench in a unit test (or counted alloc traces on a single LFH build) | >1 alloc per appendLE |
| H6 | Zipper actor cost is ~0.1% of run | Re-confirm Run-5 finding on new baseline | zipperAppend stage sum < 5 s |
| H7 | Per-task GET throughput is the floor (~12 MB/s) regardless of pool | A2 in-frame throughput from per-frame instrument | in-frame bytes/sec ≈ 12 MB/s |

Don't bundle hypotheses. If one experiment rules out two suspicions,
that's fine — but each gets its own row.

### Phase C — Apply changes the data justifies

Only the ones B confirmed. One PR per change. Each PR re-runs Phase A's
baseline harness; the diff is the win.

Likely candidates, listed but **not pre-ordered** (Phase B will
re-rank):

1. **ChunkProducer buffer = `ByteBuffer`** (instead of `Data`), and use
   `AWSHTTPBody(byteBuffer: …)` on uploadPart. Eliminates the upload-path
   copy if H2 confirms.
2. **Single-hop `appendCompound`** — write LFH + body + DD inside one
   actor method, no nested `await`s. Cheap if H3 confirms.
3. **Drop actor on ChunkProducer** — replace with `NIOLockedValueBox<State>`
   driving a `nonisolated` AsyncStream. Hot path becomes one lock
   acquire per file instead of N actor hops.
4. **Drop actor on ByteSemaphore** — same treatment if A2 + H1 say it's
   on the file-frequency path.
5. **Free memory headroom → raise `maxDownloadsMemory`.** If 1+2+3
   reclaim peak RSS, raise the budget (20 → 32 MiB) — direct attack on
   H1. Stops short of 40 MiB (Run 6 OOM).
6. **Headers built directly into the producer's backing buffer** via
   `ByteBuffer.writeInteger(_:endianness:)` instead of `Data + appendLE`.
   Only if H5 lands.
7. **Pre-allocate `samples[stage]` arrays** even when STATS=1 — avoids
   array reallocation skewing instrumented runs.

After each change:

- Add a numbered run to `RESULTS.md` (Run 11, 12, …).
- Update the "What we learned" / "Things tried but reverted" sections.
- Update `DESIGN.md` if the architecture changed.
- Update this file's status section below.

## Status

| Phase | Step | Status |
|---|---|---|
| A | A1 — Stats zero-cost-off | not started |
| A | A2 — new instruments | not started |
| A | A3 — baseline run | not started |
| B | H1–H7 | not started |
| C | TBD | gated on B |

(Update this table as work progresses. The detailed run log lives in
`RESULTS.md`.)

## Out of scope (for now)

- AWS SDK for Swift port — kept on its sibling branch, won't be
  re-tested until Soto is exhausted.
- Predicted-layout ZIP — already disproved (Run 3 of `swift-sebsto`,
  532 s vs 372 s classic). Don't revisit unless we have a structurally
  different reason to.
- HTTP/2 — S3 is HTTP/1.1 only.
- Memory bumps above 512 MB — that changes the contender's price ceiling
  and is a different submission.
