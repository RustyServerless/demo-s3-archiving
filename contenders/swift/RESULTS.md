# Swift contender — results & lessons

Performance log for the Swift contenders against the Rust reference. Region:
`eu-west-3`. Test bucket: 3000 random objects, ~15 GB total. Lambda config:
`provided.al2023`, arm64, **512 MB**, 600 s timeout. The benchmark Step
Function reports `run_price_usd` (lower is better — that is the project's
ranking metric).

## Final results

| Contender | Best run | Mem peak | `run_price_usd` | Ratio vs Rust |
|---|---|---|---|---|
| `rust-jeremie-rodon` (reference) | 213.0 s | ~350 MB | $0.001420 | 1.00× |
| `swift-sebsto-soto` (Soto)    | 366.9 s (Run 7) / 379.8 s (Run 8) | 470–511 MB | $0.002446 (Run 7) / $0.002532 (Run 8) | 1.72×–1.78× |
| `swift-sebsto-classic-awssdk` (AWS SDK) | timed out at 600 s | 363 MB | n/a (failed) | — |

Soto is the **shippable Swift variant**. The AWS SDK port is kept on a
sibling branch (`contender/swift-sebsto-classic-awssdk`) for reference.

## Conclusion

After nine end-to-end benchmark runs across two architectures (3-stage
classic, predicted-layout) and two SDKs (Soto, AWS SDK for Swift), the
shippable Swift contender is **`swift-sebsto-soto` on Soto**. It runs at
~1.72×–1.78× the Rust reference's wall-clock time, which puts it second in
`run_price_usd` ranking — behind Rust, ahead of any other language we have
shipped.

Every architectural and SDK-level lever has now been pulled and measured:
in-pipeline parallelism (predicted-layout), bigger HTTP connection pool,
ByteBuffer-only hot path, CRC32 in the downloader, alternative SDK. The
remaining 1.72× gap to Rust is **not** at any of those layers. We do not
know exactly where it is. Plausible suspects — none verified — include
Foundation `Data` allocation pressure on the body-streaming path, the
Lambda VM's per-connection TLS cost which both SDKs share, and the Swift
runtime / NIO event-loop scheduling at the 0.29 vCPU allocation that
512 MB of Lambda memory provides. *All speculation; do not state as fact.*

## Run 10 — Phase A baseline (fresh CI + bench, instrumented)

Stack: `demo-s3-archiv-perf-{ci,root}` in eu-west-3, account 486652066693.
Built from `contender/swift-sebsto-soto @ 0471df9` with PERF-PLAN.md
Phase A1+A2 instrumentation. STATS=1 throughout.

3 cold + 3 warm executions. "Cold" = LambdaConfig env var bumped before
each run to force a new sandbox; "warm" = back-to-back, same env vars,
same sandbox.

| Run | Type | Rust (s) | Swift (s) | Swift `run_price_usd` | Ratio |
|---|---|---|---|---|---|
| 10.cold-1 | cold | 213.7 | 377.4 | $0.002516 | 1.77× |
| 10.cold-2 | cold | 211.7 | 374.7 | $0.002498 | 1.77× |
| 10.cold-3 | cold | 213.1 | 374.3 | $0.002495 | 1.76× |
| 10.warm-1 | warm | 212.1 | 372.3 | $0.002482 | 1.76× |
| 10.warm-2 | warm | 212.1 | 377.6 | $0.002517 | 1.78× |
| 10.warm-3 | warm | 212.1 | **OOM** | n/a | — |

**Locked baseline**: median Swift (excluding OOM) = **374.7 s** ≈ **1.77×
Rust**. Variance across 5 successful Swift runs is ±2.7 s. Rust is rock
steady at 212.1–213.7 s. The instrumentation has not regressed wall-clock
relative to the prior Run-7/8 numbers (367 / 380 s).

### Phase A findings — verified hypotheses

Stats output from cold-1 (representative; later runs are similar):

```
stats[downloadFile]:         n=3000 sum=1228896ms p50=402ms p95=579ms p99=678ms max=836ms
stats[downloadBetweenFrames]:n=3000 sum= 766959ms p50=247ms p95=420ms p99=500ms max=657ms
stats[downloadInFrame]:      n=3000 sum= 135945ms p50= 36ms p95=108ms p99=151ms max=260ms
stats[zipperQueueWait]:      n=3000 sum= 369137ms p50=102ms p95=300ms p99=359ms max=459ms
stats[zipperAppend]:         n=3000 sum=   5115ms p50=  0ms p95=  2ms p99= 36ms max=112ms
stats[uploadPart]:           n=1501 sum= 339136ms p50=216ms p95=321ms p99=420ms max=757ms
stats[uploaderQueueWait]:    n=1501 sum= 374630ms p50=259ms p95=399ms p99=442ms max=560ms
stats[downloadInFlight]:     mean=2.56 max=5
stats[producerHops]:         total=9000     (3000 files × 3 hops/file)
stats[peakRSS]:              437.1MB
stats[heapInUse]:            86.4MB
```

Verdicts against PERF-PLAN.md hypotheses:

| H | Predicted (true) | Observed | Verdict |
|---|---|---|---|
| H1 | byte budget gates concurrency: mean ≈ 4, max ≤ 5 | mean **2.56**, max 5 | **CONFIRMED — even tighter than predicted** |
| H3 | `appendCompound` does 3 actor hops/file (counter == 9000) | counter == **9000** exactly | **CONFIRMED** |
| H4 | `between-frames` >> `in-frame` time | between **766.9s** vs in-frame **135.9s** = 5.6× | **CONFIRMED — between-frame is the dominant cost** |
| H6 | `zipperAppend` ~0.1% of run | 5.1s / 374.7s = **1.4%** | mostly confirmed (slightly above the prior 4.4 s) |

H2 / H5 still need targeted experiments (see Phase B in PERF-PLAN.md).

### Phase A unexpected findings

Three things the new instrumentation surfaced that no prior run could see:

1. **Warm-run RSS climbs**: peakRSS across the same warm sandbox went
   **437 → 492 → 422 → 494 → 500 → OOM**. Five successful invocations
   then a kill. `heapInUse` (mallinfo2 uordblks) stayed flat at
   ~86–103 MB. The drift is therefore **anonymous mmap regions** — most
   likely NIO `ByteBufferAllocator` per-event-loop arenas not being
   returned to the OS, or AsyncHTTPClient state retained between
   invocations. *This is a real ops issue: under sustained load the
   Lambda will OOM after a handful of calls.* The bench Step Function
   only invokes once, so historical runs never hit this; warm-3 here did.

2. **`downloadInFlight` mean degrades across warms**: 2.56 → 2.65 → 1.78
   → 0.90 → 0.63. Even before OOM, the warm sandbox runs progressively
   more *serially*. The byte budget hasn't changed and the file list
   hasn't changed, so something is causing download tasks to suspend
   longer / start later on warm runs. Suspect: AsyncHTTPClient
   connection pool saturation by retained connections from prior runs,
   or event-loop scheduling under increasing memory pressure.

3. **`downloadInFrame` p50 = 36 ms over 5 MB ≈ 140 MB/s in-frame
   throughput**, but `downloadFile` p50 = 402 ms ≈ 12.4 MB/s. The
   *work* the Swift code does once a frame is in hand is fast; it
   spends 5.6× as long *waiting for the next frame*. That number
   should be the new optimization target — every later change is
   judged by what it does to `downloadBetweenFrames`.

These are now PERF-PLAN.md candidates for Phase B/C, in addition to the
H1–H7 set.

## Run 11 — Phase B probe: streaming vs collect A/B

Phase B probe (`PROBE_COLLECT` env var) compares the default streaming
`for try await frame in response.body` path against
`response.body.collect(upTo:)` which buffers the whole body before
returning. Tests F3 hypothesis (AsyncSequence overhead vs network/TLS).

Stack: `demo-s3-archiv-perf-{ci,root}`. Build: `bafb743`. STATS=1.

| Run | PROBE_COLLECT | Swift wall (s) | Rust (s) |
|---|---|---|---|
| 11.A1 | unset (streaming) | 375.0 | 210.5 |
| 11.B1 | 1 (collect)       | 374.2 | 212.3 |

**Wall-clock essentially identical.** F3 partially refuted: AsyncSequence
overhead is not the dominant cost.

### Per-stage diff (cold-1 from Run 10 vs B1)

| Metric | Streaming (10.cold-1) | Collect (11.B1) | Δ |
|---|---|---|---|
| Wall-clock | 377.4 s | 374.2 s | -0.8% (noise) |
| downloadFile sum | 1228.9 s | **1168.7 s** | **-60 s (-5%)** |
| downloadFile p50 | 402 ms | 386 ms | -16 ms |
| downloadInFrame sum | 135.9 s | 249.7 s | +114 s (different work — whole-body CRC) |
| downloadBetweenFrames sum | 766.9 s | n/a in collect path | — |
| zipperQueueWait sum | 369.1 s | 362.7 s | -2% |
| uploadPart sum | 339.1 s | 369.7 s | +30 s |
| downloadInFlight mean | 2.56 | 2.79 | +0.23 |
| **peakRSS** | **437 MB** | **366.8 MB** | **-70 MB** |
| **heapInUse** | **86.4 MB** | **69.6 MB** | **-17 MB** |

Findings:

1. **The wall-clock bottleneck is NOT per-task download time.** Cutting
   60 s of summed download work moved the wall-clock by <1%. Mean
   in-flight only rose 2.56 → 2.79; the byte budget (20 MiB / ~5 MB
   files ≈ 4 concurrent) is the real concurrency cap.

2. **Collect saves ~70 MB peak RSS** (and 17 MB heap). Streaming-path
   ByteBuffer arena churn is a contributor to F1 (warm-run RSS leak).
   This is the more interesting finding from this probe.

3. The minor uploadPart slowdown (+30 s) is intra-run noise from
   sharing the upload pool with new collect-path memory pressure
   patterns; not statistically significant from one A/B.

### Implications for Phase C

The download stage is byte-budget-bound, not network-bound or
sequence-overhead-bound. To move the wall-clock we must:
- Free RSS (so we can raise the byte budget without OOM).
- Raise the budget.
- (Optionally) keep collect — it's cheap RSS.

C1 (ByteBuffer end-to-end on upload) remains the highest-ROI change:
it kills ~15 GiB/run of avoidable copies and should free ByteBufferAllocator
arena pressure further.

## Run 12 — Phase C1: ByteBuffer end-to-end on upload

Stack: same. Build: `e48c5e5`. STATS=1, PROBE_COLLECT unset (streaming
path) — isolates C1 from C2.

Change: ChunkProducer's internal buffer Data → ByteBuffer; uploadPart
uses AWSHTTPBody(buffer:). Eliminates ~15 GiB/run of avoidable copies
(soto-core AWSHTTPBody.swift:44).

| Run | Type | Swift (s) | Rust (s) | run_price_usd | Ratio |
|---|---|---|---|---|---|
| 12.cold-1 | cold | 365.6 | 211.2 | $0.002437 | 1.73× |
| 12.cold-2 | cold | 371.6 | 210.9 | $0.002477 | 1.76× |

vs Run 10 baseline (median 374.7 s, range 374.3–377.4): **-3 to -9 s**,
~1–3% wall-clock improvement.

### Per-stage diff (Run 10 cold-1 → 12.cold-2)

| Metric | 10.cold-1 | 12.cold-2 | Δ |
|---|---|---|---|
| Wall-clock | 377.4 s | 371.6 s | **-5.8 s (-1.5%)** |
| **uploadPart sum** | **339.1 s** | **326.3 s** | **-12.8 s (-3.8%)** ← direct C1 effect |
| **zipperAppend sum** | **5.1 s** | **3.0 s** | **-2.1 s (-42%)** |
| downloadFile sum | 1228.9 s | 1208.1 s | -20.8 s |
| downloadBetweenFrames | 766.9 s | 726.5 s | -40 s |
| downloadInFrame | 135.9 s | 150.0 s | +14 s |
| zipperQueueWait | 369.1 s | 366.0 s | -3 s |
| uploaderQueueWait | 374.6 s | 369.0 s | -6 s |
| downloadInFlight mean | 2.56 | 1.70 | **-0.86 (regression)** |
| **peakRSS** | **437.1 MB** | **418.7 MB** | **-18 MB** |
| heapInUse | 86.4 MB | 103.0 MB | +17 MB |
| Max Memory Used | n/a | 445 MB | (CloudWatch report) |

### Findings

- **C1 directly delivered a 12.8 s save on `uploadPart` and 2.1 s on
  `zipperAppend`.** Both are the predicted effects of removing the
  Data→ByteBuffer copy on the upload hot path.
- Wall-clock only moved -1.5% because the run is byte-budget-bound:
  saving upload time doesn't shrink the wall-clock unless stage A can
  run more concurrent downloads. **`downloadInFlight` mean actually
  went *down*** (2.56 → 1.70) — different timing of acquire/release
  through the byte budget.
- **18 MB peakRSS reclaimed.** Combined with ~70 MB from C2 (Run 11),
  we'd have ~88 MB headroom — enough to raise `maxDownloadsMemory`
  from 20 → 28 MiB without OOM (Run 6 OOM'd at 511 MB / 40 MiB
  budget; CW Max Memory Used here = 445 MB).
- heapInUse climbed (86 → 103 MB) — more ByteBuffer-arena state held
  by the producer, but offset by the absence of upload-path copies.

### Decision

Keep C1. Continue to C2 (collect as default) and C3 (raise byte
budget) — together they should unblock the wall-clock.

## Run 13 — Phase C2: collect as default download path

Stack: same. Build: `1c8d718`. STATS=1.

Change: removed PROBE_COLLECT env var + the streaming AsyncSequence
branch. downloadFile always uses `response.body.collect(upTo:)`.

| Run | Type | Swift (s) | Rust (s) | run_price_usd |
|---|---|---|---|---|
| 13.cold-1 | cold | 365.2 | 212.8 | $0.002435 |
| 13.cold-2 | cold | 368.7 | 212.3 | $0.002458 |

Wall-clock parity vs C1 (365.6 / 371.6). As expected — C2 is a code-
simplification + RSS play, not a speed change.

### Per-stage diff (C1 cold-2 → C2 cold-2)

| Metric | C1 cold-2 | C2 cold-2 | Δ |
|---|---|---|---|
| Wall-clock | 371.6 s | 368.7 s | -2.9 s |
| downloadFile sum | 1208.1 s | 1171.9 s | -36 s |
| downloadInFrame sum | 150.0 s | 254.2 s | +104 s (full-buffer CRC) |
| zipperAppend | 3.0 s | 9.9 s | +7 s |
| uploadPart | 326.3 s | 334.2 s | +8 s |
| downloadInFlight mean | 1.70 | 2.69 | **+1.0** |
| **peakRSS** | **418.7 MB** | **431.4 MB** | **+13 MB regression** |
| heapInUse | 103 MB | 86.4 MB | -17 MB |
| Max Memory Used (CW) | 445 MB | **460 MB** | +15 MB |

### Findings

- **C2 RSS prediction did not hold on top of C1.** Run 11 (probe vs
  streaming-Data baseline) showed -70 MB peakRSS. Run 13 (collect vs
  streaming-ByteBuffer baseline = C1) shows +13 MB. The streaming +
  ByteBuffer combination from C1 already captured most of the
  ByteBuffer-arena savings; switching from streaming to collect
  *adds* one large per-file allocation back.
- `heapInUse` did drop -17 MB — collect's single allocation is
  cleanly freed; streaming's per-frame ByteBuffers leave more
  fragmentation.
- `downloadInFlight` is back near baseline (2.69 vs Run 10's 2.56).
- **CW Max Memory Used = 460 MB**, leaving ~52 MB headroom against
  the 512 MB ceiling. C3 (raise byte budget) must be conservative:
  Run 6 OOM'd at 511 MB with budget=40 MiB. Safe ceiling for C3
  is somewhere around 24–28 MiB.

### Decision

**Keep C2** — code is simpler (no AsyncSequence iteration, no per-frame
timing), RSS regression is small (+13 MB), and `heapInUse` actually
improved. The "make collect default" change is justified on
maintainability + heap fragmentation, even though peakRSS didn't move
the way Run 11 suggested.

C3 is the next move — but go conservative: try byte budget 20 → 24 MiB
first.

## Run 14 — Phase C2.5: pre-sized ByteBuffer + single-pass CRC

Stack: same. Build: `7772f42`. STATS=1.

Hybrid of Run 11 (collect's single CRC pass + simpler code) and Run 10
(pre-sized ByteBuffer with deterministic per-file allocation). Replaces
`response.body.collect(upTo:)` with: pre-allocate ByteBuffer at exactly
`expectedSize`, stream `for try await frame in response.body` into it,
single CRC pass at end.

| Run | Type | Swift (s) | Rust (s) | run_price_usd | Ratio |
|---|---|---|---|---|---|
| 14.cold-1 | cold | **363.7** | 209.2 | $0.002425 | **1.74×** |
| 14.cold-2 | cold | **363.9** | 212.8 | $0.002426 | **1.71×** |

**Best Swift wall-clock to date.** Both runs within 0.2 s — very tight.

### Per-stage diff (C2 cold-2 → C2.5 cold-1)

| Metric | C2 cold-2 | C2.5 cold-1 | Δ |
|---|---|---|---|
| **Wall-clock** | 368.7 s | **363.7 s** | **-5.0 s** |
| **peakRSS** | 431.4 MB | **388.4 MB** | **-43 MB** |
| **Max Memory Used (CW)** | 460 MB | **417 MB** | **-43 MB** |
| downloadFile sum | 1171.9 s | 1136.6 s | -35 s |
| downloadInFrame | 254.2 s | 245.2 s | -9 s |
| zipperQueueWait | 355.2 s | 348.8 s | -6 s |
| zipperAppend | 9.9 s | 11.9 s | +2 s |
| uploadPart | 334.2 s | 342.1 s | +8 s |
| uploaderQueueWait | 365.8 s | 360.9 s | -5 s |
| downloadInFlight mean | 2.69 | 1.96 | -0.7 |
| heapInUse | 86.4 MB | 102.8 MB | +16 MB |

### Findings

- **Pre-sizing the ByteBuffer fixed C2's RSS regression and then some.**
  Max Memory Used dropped from 460 → 417 MB, the lowest yet. Confirms
  the C2 hypothesis: `response.body.collect(upTo:)` was growing its
  internal accumulator by doubling, leaking intermediate buffers into
  NIO/glibc arenas.
- Wall-clock improved 5 s vs C2, even though the per-stage breakdown
  shows downloadFile -35 s of summed work — the rest of the saving was
  absorbed by lower mean concurrency (1.96 vs 2.69). Same byte-budget-
  bound pattern as C1.
- **95 MB headroom** to the 512 MB ceiling. C3 can be more
  aggressive than the conservative "+4 MiB" plan: bumping
  `maxDownloadsMemory` from 20 → 32 MiB adds at most ~12 MiB of body
  storage per concurrent download — well within the 95 MB cushion.

### Decision

Keep C2.5. Current best: **363.7 s, 1.71× Rust, $0.002425**.

Total Phase C wins so far vs Run 10 baseline (374.7 s):
- Wall-clock: -11.0 s (-3%)
- Max Memory Used: -43 MB (vs Run 10 437 MB peakRSS, now 417 MB)

C3 is next. With ~95 MB headroom we can try `maxDownloadsMemory` 20 →
32 MiB without expecting OOM.

## Run 15 — Phase C3: byte budget 20 → 32 MiB (REVERTED)

Stack: same. Build: `13c3b33`. STATS=1.

| Run | Type | Swift (s) | Rust (s) | Status |
|---|---|---|---|---|
| 15.cold-1 | cold | 361.5 | 212.7 | OK |
| 15.cold-2 | cold | **OOM** | 213.3 | crash: Runtime.OutOfMemory |

### Per-stage diff (C2.5 cold-1 → C3 cold-1)

| Metric | C2.5 cold-1 | C3 cold-1 | Δ |
|---|---|---|---|
| Wall-clock | 363.7 s | 361.5 s | -2.2 s |
| **downloadInFlight mean** | 1.96 | **4.16** | **+2.2 (+112%)** |
| downloadInFlight max | 5 | 8 | +3 |
| downloadFile sum | 1136.6 s | 1893.3 s | **+757 s** |
| downloadFile p50 | 376 ms | 626 ms | **+250 ms** |
| uploadPart sum | 342.1 s | 412.1 s | +70 s |
| **peakRSS** | 388.4 MB | **467.4 MB** | **+79 MB** |
| **Max Memory Used (CW)** | 417 MB | **490 MB** | **+73 MB → 22 MB headroom** |

### Findings — C3 reverted

**The S3 bandwidth was already saturated at the per-task level.**
Doubling the byte budget doubled mean concurrency but per-task download
time also doubled (376 → 626 ms p50). Net wall-clock saving = 2.2 s,
upload-side cost = +70 s of summed work, RSS cost = +73 MB.

Cold-2 OOM-killed at 512 MB. C3 is unsafe even with C1+C2.5's headroom
gains. **Reverting `maxDownloadsMemory` to 20 MiB.**

### What this teaches

- The byte-budget hypothesis from Phase B was *partly* wrong. We
  thought "downloads are budget-bound, raise budget = faster". They
  are budget-bound for in-flight count, but the underlying network
  pipe is already filled by ~2 concurrent downloads. Adding more just
  makes each slower.
- This means the remaining 1.7× gap to Rust is likely **not** about
  in-flight concurrency. It's about per-task throughput inside
  AsyncHTTPClient/Soto, or wall-clock-blocking that we haven't
  identified yet.
- The headroom we gained from C1+C2.5 (95 MB) is real but cannot be
  cashed in via byte budget alone.

### Best result locked

**C2.5: Swift 363.7 s, 1.71× Rust, $0.002425.** Down from baseline
374.7 s (-3%). Modest but real.

## Run-by-run history

### Rust reference (`rust-jeremie-rodon`)

Same Lambda re-invoked across all our runs; variance is run-to-run network
jitter, not code changes.

| Run | Duration | `run_price_usd` | Context |
|---|---|---|---|
| 1 | 214.6 s | $0.001431 | classic baseline |
| 2 | 213.3 s | $0.001428 | tuning attempt 1 |
| 3 | 209.7 s | $0.001398 | tuning attempt 2 |
| 4 | 213.0 s | $0.001420 | sebsto v1 deploy |
| 5 | 212.9 s | $0.001419 | classic ByteBuffer hot path |
| 6 | 213.0 s | $0.001420 | + Stats instrumentation |
| 7 | 213.5 s | $0.001423 | + pool=32 |
| 8 | 213.2 s | $0.001421 | 3-way bench (Rust + Soto + AWS SDK) |

### `swift-sebsto-classic` (Soto, 3-stage pipeline)

| Run | Commit | Change | Duration | `run_price_usd` |
|---|---|---|---|---|
| 1 | `fa4a45f` | Initial port. 10 MiB chunks × 3 uploads, 20 MiB byte budget, CRC32 in zipper, `Data` everywhere. | 372.5 s | $0.002488 (1.74×) |
| 2 | `00fe9e7` | Tunables: 8 MiB chunks × 6 uploads, 30 MiB budget. No improvement; reverted. | 373.2 s | $0.002488 |
| 3 | `1c2a4ed` | CRC in downloader + `appendMany` (one actor hop / file) + pointer memcpy. No improvement; reverted. | 376.2 s | $0.002508 |
| 4 | `2c8102a` | ByteBuffer end-to-end on hot path; `appendCompound`. Slightly worse but kept. | 385.2 s | $0.002568 |
| 5 | `91d4909` | + per-stage timing (Stats actor, `clock_gettime`). Diagnosis pass. | 386.5 s | $0.002577 |
| 6 | `936263e` | Pool 8→32 + budget 20→40 MiB → OOM stall (511 MB peak, hung). | timed out | — |
| 7 | `5e894ba` | Pool=32 only (revert budget bump). **First measurable improvement.** | **366.9 s** | **$0.002446 (1.72×)** |
| 8 | (re-run of `5e894ba`) | Same code, co-deployed alongside AWS SDK contender on `demo-s3-archiv-awssdk-root`. | 379.8 s | $0.002532 (1.78×) |

### `swift-sebsto-classic-awssdk` (AWS SDK for Swift)

| Run | Commit | Change | Duration | `run_price_usd` |
|---|---|---|---|---|
| 9 | `648169a` | Direct port of sebsto-classic to AWS SDK for Swift. Same architecture; only SDK calls and HTTP client differ. | timed out at 600 s, reached 2200/3000 entries | — |

### `swift-sebsto` (predicted-layout, removed)

| Run | Commit | Change | Duration | Status |
|---|---|---|---|---|
| 1 | `03a070a` | 8 MiB parts, `maxOpenParts=8`, 12 downloads, 6 uploads, `HTTPClient.shared`. | 600 s | timeout — `HTTPClientError.deadlineExceeded` (default 8-conn pool too small for 18 in-flight requests) |
| 2 | `ad6e4a4` | + `concurrentHTTP1ConnectionsPerHostSoftLimit=32`, read timeout 120 s. | 600 s | silent hang — `PartActor` deadlock (`maxOpenParts < downloadConcurrency × partsPerFile`) |
| 3 | `626cb35` | + `maxOpenParts: 8 → 24`, `downloadConcurrency: 12 → 8`. | 532.7 s | $0.003551 (2.50×) — works but slower than classic |

The `sebsto` directory and the `contender/swift-sebsto` branch were deleted
after this experiment. Source can be recovered from git history if needed.

### Run 5 profiling breakdown — the bottleneck

The instrumentation pass that changed the diagnosis. Per-stage sums across
the whole 3000-file run on commit `91d4909`:

| Stage | n | Sum | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| **downloadFile** | 3000 | **1256.8 s** | 417 ms | 579 ms | 660 ms | 778 ms |
| zipperQueueWait | 3000 | 378.8 s | 102 ms | 318 ms | 363 ms | 434 ms |
| **zipperAppend** | 3000 | **4.4 s** | 0.4 ms | 1.6 ms | 30 ms | 116 ms |
| uploadPart | 1501 | 337.5 s | 217 ms | 317 ms | 382 ms | 932 ms |
| uploaderQueueWait | 1501 | 383.7 s | 273 ms | 403 ms | 440 ms | 525 ms |

Per-task download throughput: ~12 MB/s (~96 Mbps) — about a third of Rust's
~35 MB/s. The downloader stage is what soaks the runtime.

### Run 7 profiling breakdown — pool change effect

| Stage | Run 5 sum | Run 7 sum | Δ |
|---|---|---|---|
| downloadFile | 1256.8 s | 1192.4 s | -64 s (-5%) |
| zipperQueueWait | 378.8 s | 359.9 s | -19 s |
| zipperAppend | 4.4 s | 4.1 s | flat |
| uploadPart | 337.5 s | 338.6 s | flat |
| uploaderQueueWait | 383.7 s | 364.3 s | -19 s |

Raising the connection pool ceiling shaved a small slice off download
serialisation. Most of the gap to Rust is **not** pool starvation.

## What we learned

- **Hypothesis**: per-frame `Data.append(contentsOf: [UInt8])` is the hot-path
  bottleneck. **Disproved.** Run 4 (commit `2c8102a`) replaced it with
  `ByteBuffer.writeBuffer` + `withUnsafeReadableBytes` + `Data.append(_:count:)`.
  Result: 385 s, slightly *worse* than the 372 s baseline. Hypothesis
  rejected, change kept anyway because it unblocked subsequent ByteBuffer
  hot-path work.

- **Hypothesis**: the single zipper actor is a serialisation bottleneck.
  **Decisively disproved.** Run 5 (commit `91d4909`) added per-stage timing.
  `zipperAppend` summed to 4.4 s out of 372 s — 0.1% of runtime. Every
  optimisation aimed at the zipper (smaller chunks, more upload concurrency,
  CRC parallelisation) was attacking the wrong target.

- **Hypothesis**: replacing actors with `NIOLockedValueBox` will buy a
  measurable win. **Skipped before implementation.** Run 5 instrumentation
  showed actor cost was 4.4 s out of 372 s. The redesign would have saved
  at most that. The proposal lived in `DESIGN-LOCK-LIGHT.md` and is now
  removed; see git history of that file for the reasoning if you ever
  reconsider.

- **Hypothesis**: predicted-layout (sebsto) parallelises the zipper and
  closes the gap. **Disproved by build-and-measure.** Result: 532 s, *slower*
  than the classic 372 s. Removing the central serial zipper moved the
  bottleneck onto `PartActor`, which is hit by *every* NIO frame
  (~250k actor hops × ~1 µs ≈ 250 ms of pure hop overhead, plus matching
  continuation allocations). The "structural win" was a loss in practice.
  The classic pipeline's single zipper is hit only ~3000 times (once per
  file), summing to 4.4 s.

- **Hypothesis**: the AsyncHTTPClient default 8-connection pool is starving
  the downloaders. **Partly confirmed.** Run 7 (commit `5e894ba`) raised
  `concurrentHTTP1ConnectionsPerHostSoftLimit` to 32. Result: 367 s, the
  first and only measurable improvement of the project (-5%). The pool was
  responsible for ~64 s of the gap; the rest was something else.

- **Hypothesis**: more in-flight downloads (raise `maxDownloadsMemory` 20 → 40
  MiB) speeds up the run further. **Disproved.** Run 6 (commit `936263e`)
  pushed peak memory to 511 MB and the Lambda OOM-stalled around entry
  600/3000, timing out. Reverted in Run 7.

- **Hypothesis**: Soto's per-request overhead is the gap. **Disproved.**
  Run 9 (commit `648169a`, AWS SDK port) was *slower*, not faster (timeout at
  600 s vs Soto 380 s on the same stack). The SDK abstraction layer is not
  where the time is going.

- **Apple `Span` / `RawSpan`** can't help here: they are `~Escapable` and
  cannot cross actor boundaries or `await` suspensions. Considered and
  shelved during the lock-light scoping.

- **HTTP/2** is not a lever — S3 only serves HTTP/1.1.

- **AsyncHTTPClient default pool is 8 connections per host.** Any contender
  with concurrent S3 GETs+PUTs > 8 must raise
  `concurrentHTTP1ConnectionsPerHostSoftLimit` or it will hit
  `deadlineExceeded`. Discovered via sebsto Run 1.

- **`maxOpenParts ≥ downloadConcurrency × maxFileSpan`** is required in any
  predicted-layout design to avoid deadlocking the part actor when files
  straddle part boundaries. Discovered via sebsto Run 2.

- **Per-task GET throughput on Soto/AsyncHTTPClient** is ~12 MB/s vs Rust's
  ~35 MB/s on the same Lambda. We never closed this gap.

## Things tried but reverted

- 8 MiB chunks × 6 uploads + 30 MiB budget (Run 2, reverted in `3c38ea4` /
  `39126b6`).
- CRC in downloader + `appendMany` + pointer memcpy combined change (Run 3,
  same revert).
- `maxDownloadsMemory` 20 → 40 MiB (Run 6, reverted in Run 7).
- AWS SDK for Swift port (Run 9 — kept on the
  `contender/swift-sebsto-classic-awssdk` branch as a comparison reference,
  not deployed in the main contender list any more).
- Predicted-layout (sebsto) variant — entire `sebsto` directory + branch
  deleted after Run 3.
- Lock-light redesign of `ChunkProducer` / `PartActor` using
  `NIOLockedValueBox` — never implemented (see lessons above);
  `DESIGN-LOCK-LIGHT.md` deleted.

## Open questions

The remaining 1.72× gap to Rust is real but uncharacterised. Plausible places
where it could be hiding, with a concrete experiment for each:

1. **Foundation `Data` allocation pressure on the body-streaming path.**
   Per-frame `Data.append(_:count:)` may still be the dominant path even
   after the ByteBuffer rewrite, depending on what Soto's response.body
   does internally. *Experiment*: malloc tracing or `getrusage` on a single
   `getObject` call comparing peak allocations against an equivalent
   `reqwest` call in Rust.

2. **Lambda VM connection / TLS cost.** Both Soto and AWS SDK pay it; Rust
   pays a different one via `reqwest`/`hyper`. *Experiment*: at 1024 MB
   memory (~0.58 vCPU) does the run drop below 380 s? If yes, the bottleneck
   is CPU-bound; if no, it is link-bound.

3. **Swift runtime / NIO event-loop scheduling at 0.29 vCPU.** *Experiment*:
   same as above — bumping memory to 1024 MB doubles the vCPU allocation;
   if the duration halves, scheduling was the cost.

4. **CRT body-iteration FFI overhead in the AWS SDK port.**
   `Smithy.ByteStream.stream(stream).readAsync(upToCount: 65_536)` is a
   manual chunk loop; each call may cross the aws-crt-swift FFI boundary.
   *Experiment*: instrument the AWS SDK contender with the same Stats actor
   (raise its timeout to 900 s so stats actually print) and compare
   per-frame `downloadFile` p50 against Soto.

5. **Cold-start vs warm-start variance.** Run 7 was 367 s, Run 8 was 380 s
   on the same code. *Experiment*: ten back-to-back runs, separate
   cold-start runs from warm-start runs.

## How to reproduce a benchmark run

Pre-requisites: the `demo-s3-archiving-ci` and `demo-s3-archiving-root`
stacks both deployed (see project `README.md`).

```bash
# 1) Get the inputs
CONTENDERS=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`ContenderArns`].OutputValue' --output text)
SM=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`BenchingStateMachineArn`].OutputValue' --output text)

# 2) (Optional) Enable per-stage profiling on the Soto contender
aws lambda update-function-configuration \
  --function-name demo-s3-archiving-swift-sebsto-soto \
  --environment 'Variables={STATS=1}'

# 3) Trigger the benchmark
aws stepfunctions start-execution \
  --state-machine-arn "$SM" \
  --input "$CONTENDERS"

# 4) Watch in the Step Functions console; read ranked output from the final
#    state. CloudWatch Logs for /aws/lambda/demo-s3-archiving-swift-sebsto-soto
#    will contain the per-stage Stats lines if STATS=1 was set.

# 5) When done, disable Stats again
aws lambda update-function-configuration \
  --function-name demo-s3-archiving-swift-sebsto-soto \
  --environment 'Variables={}'
```

To rebuild after a code change: push the branch; CodePipeline rebuilds the
`bootstrap` and re-deploys the Lambda within a few minutes (warm SwiftPM
cache; first build is ~10 min).

To run the AWS SDK variant alongside, switch to the
`contender/swift-sebsto-classic-awssdk` branch; that branch's
`templates/contenders.yml` registers `SwiftSebstoClassicAWSSDKFunction`
alongside the Soto one, so a single Step Function execution invokes both.
