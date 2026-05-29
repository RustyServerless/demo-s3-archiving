# Swift Contender Performance Log

A run-by-run record of measured performance for every Swift contender, with
the commit each measurement was taken at and what we tried. Region:
`eu-west-3`. Test bucket: 3000 random objects, ~15 GB total. Lambda config:
`provided.al2023`, arm64, **512 MB** memory, 600 s timeout.

The benchmark Step Function reports `run_price_usd` (real Lambda invocation
cost given memory + duration + ephemeral storage above the 512 MB free tier).
Lower is better; the project ranks contenders on this metric.

## Rust reference (`rust-jeremie-rodon`)

Same Lambda re-invoked across all our runs. Variance is run-to-run network
jitter, not code change.

| Run | Duration | `run_price_usd` | Branch / context              |
|-----|----------|-----------------|-------------------------------|
| 1   | 214.6 s  | $0.001431       | classic baseline run          |
| 2   | 213.3 s  | $0.001428       | classic + tuning attempt 1    |
| 3   | 209.7 s  | $0.001398       | classic + tuning attempt 2    |
| 4   | 213.0 s  | $0.001420       | sebsto v1 deploy              |
| 5   | 212.9 s  | $0.001419       | classic ByteBuffer hot path   |
| 6   | 213.0 s  | $0.001420       | classic + Stats instrumentation |
| 7   | 213.5 s  | $0.001423       | classic + pool=32 (sebsto OOM'd) |

## `swift-sebsto-classic` (3-stage pipeline: download → zipper actor → upload)

| Run | Commit    | Change                                                                                        | Duration | Mem peak | Status / `run_price_usd` |
|-----|-----------|-----------------------------------------------------------------------------------------------|----------|----------|--------------------------|
| 1   | `fa4a45f` | Initial port. 10 MiB chunks × 3 uploads, 20 MiB byte budget, CRC32 in zipper, `Data` everywhere. | 372.5 s  | 452 MB   | $0.002488 (1.74× Rust)   |
| 2   | `00fe9e7` | Tunables only: 8 MiB chunks × 6 uploads, 30 MiB byte budget.                                  | 373.2 s  | 504 MB   | $0.002488 (1.75× Rust)   |
| 3   | `1c2a4ed` | Run 2 + CRC32 in downloader (parallel) + `appendMany` (1 actor hop / file) + pointer memcpy in `ChunkProducer`. | 376.2 s | 511 MB   | $0.002508 (1.79× Rust) ← worse |
| —   | `3c38ea4`/`39126b6` | Reverted runs 2 & 3.                                                              |          |          |                          |
| 4   | `2c8102a` | ByteBuffer end-to-end on hot path: downloader yields `(ByteBuffer, UInt32)`; chunk producer copies via `withUnsafeReadableBytes` + `Data.append(_:count:)`; `appendCompound(lfh:body:dataDescriptor:)` for one actor hop per file. | 385.2 s  | (n/a)    | $0.002568 (1.81× Rust) ← worse |
| 5   | `91d4909` | Run 4 + per-stage timing instrumentation (Stats actor, clock_gettime). | 386.5 s  | (n/a)    | $0.002577 (1.81× Rust)         |
| 6   | `936263e` | Pool 8→32 + maxDownloadsMemory 20→40 MiB → OOM stall (511 MB peak, hung at 600/3000). | 600.0 s  | 511 MB   | timed out                       |
| 7   | `5e894ba` | Run 6 minus byte budget bump: keep pool=32, revert budget to 20 MiB. | **366.9 s** | (n/a) | **$0.002446 (1.72× Rust)** ← first measurable improvement |

### Run 7 profiling breakdown — pool change effect

| Stage              | Run 5 sum | Run 7 sum | Δ      | Note |
|--------------------|-----------|-----------|--------|------|
| downloadFile       | 1256.8 s  | 1192.4 s  | -64 s (-5%) | p50: 418 → 399 ms (-19 ms) |
| zipperQueueWait    | 378.8 s   | 359.9 s   | -19 s | Roughly tracks download |
| zipperAppend       | 4.4 s     | 4.1 s     | flat | Confirmed not the bottleneck |
| uploadPart         | 337.5 s   | 338.6 s   | flat | Pool change doesn't affect uploads |
| uploaderQueueWait  | 383.7 s   | 364.3 s   | -19 s | Tracks zipper |

Pool=32 saved exactly what the diagnosis predicted: a small slice of download serialization. **Most of the gap to Rust is per-request Soto overhead, not pool starvation.**

### Run 5 profiling breakdown — **the actual bottleneck found**

| Stage              | n     | Sum       | p50    | p95    | p99    | max    |
|--------------------|-------|-----------|--------|--------|--------|--------|
| **downloadFile**   | 3000  | **1256.8 s** | 417 ms | 579 ms | 660 ms | 778 ms |
| zipperQueueWait    | 3000  | 378.8 s   | 102 ms | 318 ms | 363 ms | 434 ms |
| **zipperAppend**   | 3000  | **4.4 s** | 0.4 ms | 1.6 ms | 30 ms  | 116 ms |
| uploadPart         | 1501  | 337.5 s   | 217 ms | 317 ms | 382 ms | 932 ms |
| uploaderQueueWait  | 1501  | 383.7 s   | 273 ms | 403 ms | 440 ms | 525 ms |

- **Per-task download throughput: ~12 MB/s = 96 Mbps.** With 10 concurrent downloaders, aggregate = ~32 MB/s actual vs the theoretical ~600 Mbps Lambda link could absorb. **This is the bottleneck.**
- **Per-part upload throughput: ~46 MB/s = 368 Mbps.** Uploads are 4× faster per task than downloads. Uploader is starved (`uploaderQueueWait` p50 = 273 ms).
- **Zipper actor cost: 4.4 s total.** ~0.1% of runtime. Every micro-optimization aimed at the zipper was attacking the wrong target.
- Per-file download time (~417 ms p50) is consistent with Soto's HTTP/1.1 connection setup + S3 first-byte latency dominating over the actual ~5 MB transfer time. Either AsyncHTTPClient pool is too small at default 8 (Soto + Lambda HTTP_PROXY interactions?), or per-request Soto overhead is high.

### `sebsto-classic` lessons

- **Upload concurrency is not the bottleneck.** Going 3 → 6 uploads with smaller chunks did nothing.
- **Moving CRC32 to the downloader alone doesn't help** — only helps when paired with reduced per-frame allocation cost.
- **`Data.append(contentsOf: [UInt8])` was *not* the bottleneck either.** Run 4 swapped the per-frame copy for `ByteBuffer.writeBuffer` + `withUnsafeReadableBytes` + `Data.append(_:count:)`. Result: 385 s (slightly *worse* than 372 s baseline). The hot-path inner loop is not where the time is going.
- **Three failed micro-optimization attacks suggest the bottleneck is structural**, somewhere in the Soto/AsyncHTTPClient stack or in the actor + AsyncStream dispatch we haven't measured yet.
- **Run 5 instrumentation revealed it**: per-task S3 GET throughput is ~12 MB/s. The downloader stage takes 1256 s of summed work; with 10 concurrent tasks that's ~125 s effective wall time — almost 60% of the 372 s runtime is just the downloader stage. Compare: Rust's per-task GET throughput is roughly 35 MB/s based on its 213 s total. **The Swift pipeline is downloader-bound**, and the downloader is bound by either Soto per-request overhead or AsyncHTTPClient connection pool sizing.
- **The single zipper actor is fine.** 4.4 s total over the run, 0.1% of runtime. All zipper-side optimization was wasted effort.
- **Apple `Span` / `RawSpan` can't replace actor patterns here.** They are `~Escapable`, can't cross actor boundaries or `await` suspensions.

## `swift-sebsto` (predicted-layout: PartActor random-access writer)

| Run | Commit    | Change                                                                                                                                | Duration | Mem peak | Status                |
|-----|-----------|---------------------------------------------------------------------------------------------------------------------------------------|----------|----------|-----------------------|
| 1   | `03a070a` | 8 MiB parts, `maxOpenParts=8`, 12 downloads, 6 uploads. `HTTPClient.shared`.                                                          | 600 s    | 470 MB   | timed out — `HTTPClientError.deadlineExceeded` (default 8-conn pool too small for 18 in-flight requests) |
| 2   | `ad6e4a4` | + `concurrentHTTP1ConnectionsPerHostSoftLimit=32`, read timeout 120 s.                                                                | 600 s    | 359 MB   | timed out, **silent hang** — `PartActor` deadlock (`maxOpenParts < downloadConcurrency × partsPerFile`) |
| 3   | `626cb35` | + `maxOpenParts: 8 → 24`, `downloadConcurrency: 12 → 8`.                                                                              | 532.7 s  | 500 MB   | $0.003551 (2.50× Rust) — works but slower than classic |

### `sebsto` lessons

- **Predicted-layout is not automatically faster.** Removing the central zipper moved the bottleneck to `PartActor`, where ~250k per-NIO-frame actor hops cost more than the classic's ~3000 per-file hops.
- **AsyncHTTPClient default pool is 8 connections per host.** With concurrent S3 GETs+PUTs > 8 you must raise `concurrentHTTP1ConnectionsPerHostSoftLimit` or hit `deadlineExceeded`.
- **`maxOpenParts ≥ downloadConcurrency × maxFileSpan`** is required to avoid `PartActor` deadlock when files straddle part boundaries.
- **HTTP/2 is not an option** — S3 only serves HTTP/1.1.

## Cross-cutting

- Actor hop cost (Swift 6.3, Graviton2): ~100 ns – several µs depending on contention and executor.
- `Synchronization.Mutex.withLock` (uncontended): ~10–20 ns.
- For high-fanout pipelines, **per-task local accumulation + bulk hand-off** beats central-actor-per-frame even with a Mutex underneath.

## Pending experiments

- **Try AWS SDK for Swift in place of Soto.** Run 7 confirmed the connection pool wasn't fully responsible — the per-request cost is itself the gap. The two SDKs use different HTTP clients (Soto: AsyncHTTPClient, AWS SDK: aws-crt-swift) and different request paths. Whether AWS SDK is faster on Lambda is an open question; this experiment answers it. Expected to either close most of the gap (if Soto-overhead dominated) or land in the same range (if AsyncHTTPClient/Lambda link is the floor for both).

## Deferred (unrelated to current bottleneck)

- **`sebsto` v2** (deferred): replace `PartActor` with `[NIOLockedValueBox<PartSlot>]` array indexed by part number + per-task local file accumulators. Per the `DESIGN-LOCK-LIGHT.md` design doc; predicted 220–260 s but unverified. Less interesting now that profiling shows the bottleneck is in the SDK request path, not the architecture.
