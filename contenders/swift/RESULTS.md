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
