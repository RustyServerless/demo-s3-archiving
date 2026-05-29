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
| 5   | (TBD)    | (TBD)           | classic ByteBuffer hot path   |

## `swift-sebsto-classic` (3-stage pipeline: download → zipper actor → upload)

| Run | Commit    | Change                                                                                        | Duration | Mem peak | Status / `run_price_usd` |
|-----|-----------|-----------------------------------------------------------------------------------------------|----------|----------|--------------------------|
| 1   | `fa4a45f` | Initial port. 10 MiB chunks × 3 uploads, 20 MiB byte budget, CRC32 in zipper, `Data` everywhere. | 372.5 s  | 452 MB   | $0.002488 (1.74× Rust)   |
| 2   | `00fe9e7` | Tunables only: 8 MiB chunks × 6 uploads, 30 MiB byte budget.                                  | 373.2 s  | 504 MB   | $0.002488 (1.75× Rust)   |
| 3   | `1c2a4ed` | Run 2 + CRC32 in downloader (parallel) + `appendMany` (1 actor hop / file) + pointer memcpy in `ChunkProducer`. | 376.2 s | 511 MB   | $0.002508 (1.79× Rust) ← worse |
| —   | `3c38ea4`/`39126b6` | Reverted runs 2 & 3.                                                              |          |          |                          |
| 4   | `2c8102a` | ByteBuffer end-to-end on hot path: downloader yields `(ByteBuffer, UInt32)`; chunk producer copies via `withUnsafeReadableBytes` + `Data.append(_:count:)`; `appendCompound(lfh:body:dataDescriptor:)` for one actor hop per file. | (TBD)    | (TBD)    | (TBD)                    |

### `sebsto-classic` lessons

- **Upload concurrency is not the bottleneck.** Going 3 → 6 uploads with smaller chunks did nothing.
- **Moving CRC32 to the downloader alone doesn't help** — only helps when paired with reduced per-frame allocation cost.
- **The classic single-zipper architecture caps at ~370 s** with `Data`-based hot path. Hypothesis for run 4: the per-NIO-frame `Data.append(contentsOf: [UInt8])` (alloc + Sequence iteration) over ~250k frames is the real cost; `ByteBuffer.writeBuffer` should remove ~60% of that.
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

- **Run 5** (commit `2c8102a`) on classic: full pipeline benchmark with `ByteBuffer` hot path. Expected: 280–340 s (closing ~30–50% of the gap to Rust).
- **`sebsto` v2** (deferred): replace `PartActor` with `[Mutex<ByteBuffer>]` array indexed by part number + per-task local file accumulators (hop budget: ~12k Mutex acquires instead of ~250k actor hops). Predicted: 220–260 s.
