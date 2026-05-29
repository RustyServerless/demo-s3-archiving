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

## `swift-sebsto-classic` (3-stage pipeline: download → zipper actor → upload)

| Run | Commit    | Change                                                                                        | Duration | Mem peak | Status / `run_price_usd` |
|-----|-----------|-----------------------------------------------------------------------------------------------|----------|----------|--------------------------|
| 1   | `fa4a45f` | Initial port. 10 MiB chunks × 3 uploads, 20 MiB byte budget, CRC32 in zipper, `Data` everywhere. | 372.5 s  | 452 MB   | $0.002488 (1.74× Rust)   |
| 2   | `00fe9e7` | Tunables only: 8 MiB chunks × 6 uploads, 30 MiB byte budget.                                  | 373.2 s  | 504 MB   | $0.002488 (1.75× Rust)   |
| 3   | `1c2a4ed` | Run 2 + CRC32 in downloader (parallel) + `appendMany` (1 actor hop / file) + pointer memcpy in `ChunkProducer`. | 376.2 s | 511 MB   | $0.002508 (1.79× Rust) ← worse |
| —   | `3c38ea4`/`39126b6` | Reverted runs 2 & 3.                                                              |          |          |                          |
| 4   | `2c8102a` | ByteBuffer end-to-end on hot path: downloader yields `(ByteBuffer, UInt32)`; chunk producer copies via `withUnsafeReadableBytes` + `Data.append(_:count:)`; `appendCompound(lfh:body:dataDescriptor:)` for one actor hop per file. | 385.2 s  | (n/a)    | $0.002568 (1.81× Rust) ← worse |

### `sebsto-classic` lessons

- **Upload concurrency is not the bottleneck.** Going 3 → 6 uploads with smaller chunks did nothing.
- **Moving CRC32 to the downloader alone doesn't help** — only helps when paired with reduced per-frame allocation cost.
- **`Data.append(contentsOf: [UInt8])` was *not* the bottleneck either.** Run 4 swapped the per-frame copy for `ByteBuffer.writeBuffer` + `withUnsafeReadableBytes` + `Data.append(_:count:)`. Result: 385 s (slightly *worse* than 372 s baseline). The hot-path inner loop is not where the time is going.
- **Three failed micro-optimization attacks suggest the bottleneck is structural**, somewhere in the Soto/AsyncHTTPClient stack or in the actor + AsyncStream dispatch we haven't measured yet.
- **The classic single-zipper architecture caps at ~370–385 s** in this stack regardless of what we do inside the hot loop.
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

- **Profile the actual hot path.** Three runs of micro-optimizations on classic have all landed in 372–385 s. Before more design work, instrument the code with timing markers on each stage (download time per file, zipper time per file, upload time per part) to see where the 372 s actually goes. Without this, every "obvious" optimization keeps missing.
- **`sebsto` v2** (deferred): replace `PartActor` with `[NIOLockedValueBox<PartSlot>]` array indexed by part number + per-task local file accumulators. Per the `DESIGN-LOCK-LIGHT.md` design doc; predicted 220–260 s but unverified.

## Hypothesis log (where time *might* be going)

- **AsyncStream consumer-side polling cost** between the downloader → zipper and the zipper → uploader. Each `for await x in stream` is a per-element await that may be more expensive than direct producer-consumer.
- **Soto request body materialization**. `AWSHTTPBody(bytes: data)` may copy. Worth checking whether passing a `ByteBuffer` body avoids it.
- **AsyncHTTPClient's HTTP/1.1 keep-alive scheduling** when many concurrent uploads share a connection pool — the requests serialize behind connection availability even if our app code is parallel.
- **Lambda CPU-time accounting**: at 512 MB the Lambda has ~0.29 vCPU. If our pipeline is even slightly less CPU-efficient than Rust's, the time difference compounds.
