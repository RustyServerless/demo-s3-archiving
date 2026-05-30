# Swift contender — `sebsto-classic` (Soto)

Architectural reference for the shippable Swift contender in this repo:
`contenders/swift/sebsto-classic/`. This is the variant that lands ZIPs end-to-end
and posts measurable `run_price_usd` numbers (~380 s / 1.72× the Rust reference).

This document describes **what the code does today**. For the run-by-run
performance log and lessons learned, see `RESULTS.md` (sibling file).

A second branch, `contender/swift-sebsto-classic-awssdk`, ships the same
architecture on top of the AWS SDK for Swift instead of Soto; it timed out at
600 s and is kept as a comparison point. See the *Sibling experiment* section
at the end of this doc.

A third variant, `swift-sebsto` (predicted-layout, parallel writer using a
`PartActor`), was built and benchmarked at 532 s — slower than `sebsto-classic`
— and removed from the repo. The full post-mortem is in `RESULTS.md`; see the
project's git history (`contender/swift-sebsto` branch, deleted) for the code.

## What the contender does

The repo is a Lambda S3-archiving benchmark. A Step Function invokes each
registered contender Lambda with `{ bucket_name, files_prefix, archive_key }`,
the Lambda must read ~3000 objects (~15 GB total) from
`s3://${bucket_name}/${files_prefix}/` and stream a flat **STORED ZIP** to
`s3://${bucket_name}/${archive_key}` within 600 s. A control Lambda then
re-hashes every entry's decompressed bytes and verifies the entry name matches
the SHA-256 of the content (entry name == basename == hex SHA-256). Contenders
are ranked by `run_price_usd` (ascending) — the real Lambda invocation price
given memory, duration, architecture, and ephemeral storage.

Per-Lambda configuration (see `templates/contenders.yml`):

| Setting | Value |
|---|---|
| Runtime | `provided.al2023` |
| Architecture | `arm64` (Graviton2) |
| Memory | `512 MB` (~0.29 vCPU) |
| Timeout | `600 s` |
| Handler | `swift.handler` (ignored by custom runtime; matches project convention) |

The reference Rust contender (`contenders/rust/jeremie-rodon/`) lands at ~213 s
with ~350 MB peak RSS. Soto-classic lands at ~380 s with ~470 MB peak.

## Architecture — three-stage streaming pipeline

A direct port of the Rust reference's `download → zip → upload` pipeline,
written in Swift 6 with structured concurrency. All three stages run as
sibling `async let` children of a single `runPipeline` task; if any stage
throws, the others are cancelled.

```
                                 ┌──────────────────────────────────────┐
ListObjectsV2                    │                                      │
   │    ┌────── byte budget ─────┼── 20 MiB ByteSemaphore ──┐           │
   ▼    │                        │                          │           │
[FileInfo …]                     │                          │           │
   │                             ▼                          │           │
   │   ┌──────────────────┐   FileChannel             ┌─────┴────────┐  │
   │   │  Stage A         │ AsyncStream<DownloadedFile>│              │  │
   ├──►│  Downloader      ├───────────────────────────►│  Stage B     │  │
   │   │  TaskGroup × N   │                            │  Zipper      │  │
   │   │  (per-task GET + │                            │  (single     │  │
   │   │   inline CRC32)  │                            │   consumer,  │  │
   │   └──────────────────┘                            │   serial)    │  │
   │                                                   └──────┬───────┘  │
   │                                                          │          │
   │                                                          ▼          │
   │                                                  ChunkProducer      │
   │                                                  (10 MiB chunks,    │
   │                                                   actor)            │
   │                                                          │          │
   │                                                          ▼          │
   │                                                  AsyncStream<       │
   │                                                    UploadChunk>     │
   │                                                          │          │
   │                                                  ┌───────┴────────┐ │
   │                                                  │  Stage C       │ │
   │                                                  │  Uploader      │ │
   │                                                  │  TaskGroup × 3 │ │
   │                                                  │  (S3 UploadPart)│
   │                                                  └────────────────┘ │
   │                                                                     │
   └─── ByteSemaphore.release ◄─ released by zipper after each file ─────┘
```

### Stage A — Downloader (`runDownloadStage` in `Archiver.swift`)

- A `withThrowingTaskGroup` of N worker tasks. Concurrency is implicitly bounded
  by the byte budget (see below): a new download task is `addTask`'d only after
  `byteBudget.acquire(file.size)` succeeds, so at any moment the in-flight set
  fits in 20 MiB.
- Each task: `s3.getObject` → iterate `response.body` (an
  `AsyncSequence<ByteBuffer>` from AsyncHTTPClient via Soto), accumulating
  frames into one pre-sized per-file `ByteBuffer` and updating a `CRC32` over
  each frame's bytes inline.
- On EOF, sends a `DownloadedFile { name, buffer, crc32, releaseBytes }` into
  `FileChannel` (an `AsyncStream<DownloadedFile>` wrapper).
- Computing CRC32 in the downloader (rather than in the single zipper) means
  CRC work is parallelised across N tasks. The CRC implementation is a tiny
  C shim that uses the ARMv8 `__crc32{b,h,w,d}` intrinsics — see `Sources/CCRC32/`.
- `ByteBuffer` (not `Data`) is the carrier all the way through: switching
  `Data.append(contentsOf: [UInt8])` per frame to `ByteBuffer.writeBuffer`
  was an early try (Run 4) — measured slightly *worse*, then kept anyway
  because subsequent reads showed the per-frame `Data` path was wasted
  allocation we did not need to pay for.

### Stage B — Zipper (`runZipStage`)

- **Single consumer** of `FileChannel.stream`. Files arrive in download
  completion order (not list order, but that does not matter — the central
  directory is built from the order we observe).
- Per file: emit local-file-header (LFH) + body bytes + data descriptor (DD)
  into the `ChunkProducer`. The bodies are NOT held in the zipper; they are
  forwarded as `ByteBuffer` slices, one zero-copy memcpy into the chunk
  accumulator.
- After all files, the zipper builds the central directory, ZIP64 EOCD, ZIP64
  EOCD locator, and EOCD, appending each into the chunk producer in order.
- Calls `byteBudget.release(file.releaseBytes)` after handing off each file's
  bytes to the chunk producer, so a downloader can resume.

The zipper is intentionally single-threaded and serial. Run-5 instrumentation
(see `RESULTS.md`) showed the zipper stage sums to ~4.4 s out of ~372 s — 0.1%
of runtime. Parallelising it is not where the time is.

### Stage C — Uploader (`runUploadStage`)

- Pulls `UploadChunk` items from `producer.stream` (an `AsyncStream` driven
  by `ChunkProducer`).
- Drives a `withThrowingTaskGroup` of up to `Tunables.maxConcurrentUploads`
  in-flight `S3.uploadPart` calls. Each completed part records its `eTag` +
  `partNumber`; the final list is sorted and sealed via
  `completeMultipartUpload`.
- After each upload completes, calls `producer.releaseSlot()` so the producer
  can build more chunks (see ChunkProducer backpressure).

### `ChunkProducer` (`ChunkProducer.swift`)

- Swift `actor`. Buckets writes from the zipper into fixed-size 10 MiB chunks
  and emits each sealed chunk on an `AsyncStream<UploadChunk>` for the uploader.
- Has two `append` overloads — `Data` (for the small ZIP headers) and
  `ByteBuffer` (for the big file bodies, zero-copy from the downloader's
  per-file accumulator) — plus an `appendCompound(lfh:body:dataDescriptor:)`
  convenience that the zipper uses to do LFH+body+DD as one operation.
- Backpressure: at most `bufferChunksCount` (= 4) chunks may be outstanding
  (built-but-not-yet-uploaded). The producer suspends in `waitForSlot()` when
  the in-flight count is at the ceiling. Combined with the 10 MiB chunk size
  this caps the producer→uploader path at 40 MiB.

### Backpressure summary

Three independent budgets keep the 512 MB Lambda within memory:

| Budget | Mechanism | Capacity |
|---|---|---|
| In-flight downloads | `ByteSemaphore` over file sizes | 20 MiB |
| Producer-buffered chunks | `ChunkProducer.maxInFlight` | 4 chunks × 10 MiB = 40 MiB |
| Concurrent uploads | TaskGroup size cap | 3 |

These three are *not* additive on the hot path — once a file is fully
downloaded and the body has been memcpy'd into the chunk producer, the byte
budget is released; the producer then owns the bytes. Peak observed memory
across runs: 452–511 MB (the swing is mostly Foundation / NIO buffer churn
and Lambda's own runtime overhead).

## Tunables (`Pipeline.swift`)

```swift
enum Tunables {
    static let maxDownloadsMemory: Int = 20 * 1024 * 1024   // 20 MiB
    static let maxConcurrentUploads: Int = 3
    static let chunkSize: Int = 10 * 1024 * 1024            // 10 MiB
    static let bufferChunksCount: Int = 4                    // ChunkProducer in-flight ceiling
}
```

The HTTP client connection pool is configured in `main.swift` (not in
`Tunables`) because it must be set on the AsyncHTTPClient before the AWSClient
is built:

```swift
httpConfig.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit = 32
httpConfig.timeout.read = .seconds(120)
httpConfig.timeout.connect = .seconds(10)
```

The pool ceiling was raised from the default 8 to 32 in Run 7 to avoid GETs
serialising behind the connection pool. That change moved the run from
~385 s → ~367 s; it is the only tunable change that has produced a measurable
improvement.

## ZIP format

STORED only (no DEFLATE), GP flag bit 3 set so CRC32 + sizes are emitted in a
data descriptor *after* each file body — required because the downloader does
not know the body size ahead of the LFH (it does, from `ListObjectsV2`, but
emitting in the descriptor keeps the Rust reference layout intact for
A/B comparison). ZIP64 is mandatory: the archive total exceeds 4 GiB.

| Record | Size | Notes |
|---|---|---|
| Local file header (LFH) | 30 + nameLen | Fields: sig, version 45, GP flag 0x0008, method STORED (0), DOS time/date (fixed 2010-01-01), CRC=0, compSize=0, uncSize=0, nameLen, extraLen=0, name |
| Data descriptor (ZIP64) | 24 | sig, CRC32, 8B compSize, 8B uncSize (== compSize because STORED) |
| Central directory record | 46 + nameLen + 28 | Last 28 bytes are the ZIP64 extended-info extra field (uncSize8, compSize8, lfhOffset8). The 32-bit fields in the CD record are set to `0xFFFFFFFF` to force readers to read from the ZIP64 extra. |
| ZIP64 EOCD record | 56 | sig, sizeOfRest=44, version, disk numbers, entry count (×2), CD size, CD offset |
| ZIP64 EOCD locator | 20 | sig, disk=0, ZIP64-EOCD offset, totalDisks=1 |
| EOCD | 22 | All counts/offsets are `0xFFFF` / `0xFFFFFFFF` (defer to ZIP64) |

Filenames are pure ASCII (SHA-256 hex, 64 chars), so no UTF-8 GP-flag tweaks
are required. There are no directory entries and no extra fields beyond the
mandatory ZIP64 one. All implementation lives in
`Sources/SwiftSebstoClassic/Zip/Headers.swift`.

## Optional profiling instrumentation — `STATS=1`

`Stats.swift` defines an `actor Stats` that records per-stage durations
(`downloadFile`, `zipperQueueWait`, `zipperAppend`, `uploadPart`,
`uploaderQueueWait`) and emits a `n / sum / p50 / p95 / p99 / max` line per
stage at the end of a run. It is gated on the `STATS` environment variable —
truthy values are `1`, `true`, `yes` (case-insensitive). When unset, both
`record` and `report` short-circuit to no-ops, leaving only the `monoNs()`
calls at the call sites (~30 ns each).

To enable for one run:

```bash
aws lambda update-function-configuration \
  --function-name demo-s3-archiving-swift-sebsto-classic \
  --environment 'Variables={STATS=1}'
```

Then trigger the Step Function and read the stats from CloudWatch Logs at
the end of the run. Switch back off afterwards (un-set or `STATS=0`) — the
samples accumulate in an unbounded array, so a long-lived warm Lambda will
slowly grow them.

The instrumentation is the tool that produced the Run 5 / Run 7 breakdowns
in `RESULTS.md` and is the way to investigate further regressions.

The clock used is `clock_gettime(CLOCK_MONOTONIC)` via a platform-conditional
libc import (`Darwin.C`, `Glibc`, `Musl`) — see `monoNs()` in `Stats.swift`.

## File map (sebsto-classic)

```
contenders/swift/sebsto-classic/
├── Package.swift                        SwiftPM manifest, Swift 6.0
├── Package.resolved
├── scripts/test-codebuild-locally.sh    Manual reproduce-the-CI-build helper
└── Sources/
    ├── CCRC32/
    │   ├── ccrc32.c                     ARMv8 __crc32 intrinsics + sw fallback
    │   └── include/ccrc32.h
    └── SwiftSebstoClassic/
        ├── main.swift                   Cold-start: HTTPClient, AWSClient, S3,
        │                                LambdaRuntime entry point.
        ├── Pipeline.swift               Tunables, FileInfo/JobInfo, listFiles,
        │                                multipart upload helpers, ByteSemaphore,
        │                                ArchivingError.
        ├── Archiver.swift               runArchiveJob + runPipeline; the three
        │                                stage functions (download/zip/upload)
        │                                and the per-file downloader.
        ├── ChunkProducer.swift          actor ChunkProducer + UploadChunk type;
        │                                10 MiB chunking + slot semaphore.
        ├── Stats.swift                  Optional profiling actor (STATS env var)
        │                                + monoNs() platform-conditional clock.
        └── Zip/
            ├── Headers.swift            LFH, DD, CD, ZIP64 EOCD encoders.
            └── CRC32.swift              Swift wrapper over CCRC32.
```

## Stack decisions

| Concern | Decision | Why |
|---|---|---|
| Lambda runtime | `swift-aws-lambda-runtime` v2.x | Closure-based `LambdaRuntime { (event, context) in … }`, Codable JSON in/out. |
| AWS SDK | **Soto 7.x** | AsyncHTTPClient (swift-nio) → smaller binary, lower cold start, native `AsyncSequence<ByteBuffer>` streaming. AWS SDK for Swift was tried as a sibling experiment; see *Sibling experiment* below. |
| ZIP writer | Hand-rolled (`Zip/Headers.swift`) | ZIPFoundation requires a URL or full-`Data` backing — incompatible with 15 GB-doesn't-fit-in-/tmp streaming. ~150 LOC of Swift gets us streaming + ZIP64. |
| CRC32 | Tiny C shim with ARMv8 `__crc32{b,h,w,d}` intrinsics; software fallback present but never hit on Graviton2. |
| Foundation | `FoundationEssentials` on Linux (`#if canImport(FoundationEssentials)`), `Foundation` on macOS for local builds. Smaller binary on Linux. |
| Architecture | arm64 (Graviton2) | Matches Rust reference; cheaper per GB-s. |
| Memory | 512 MB | Same as Rust reference. Peak observed ~470–510 MB. |
| Concurrency | Swift 6 strict concurrency, structured `withThrowingTaskGroup`, actors for shared state, `AsyncStream` for inter-stage queues. |

A redesign that swapped the actors for `NIOLockedValueBox` was scoped (the
old `DESIGN-LOCK-LIGHT.md`, now removed) and never built — Run 5
instrumentation showed actor cost was 4.4 s out of 372 s, so the redesign
would not have moved the ranking. See `RESULTS.md` for details.

## Build & deploy

The CI is `ci-config/buildspec.yml`. Relevant block (the `# SWIFT BUILD`
section):

```bash
SWIFT_CONTENDER_LAMBDA_PATH=./contenders/swift
for LAMBDA_DIR in "$SWIFT_CONTENDER_LAMBDA_PATH"/*/; do
  [ -d "$LAMBDA_DIR" ] || continue            # skips DESIGN.md, RESULTS.md
  LAMBDA=$(basename "$LAMBDA_DIR")
  cd $SWIFT_CONTENDER_LAMBDA_PATH/$LAMBDA
  swift build -c release --static-swift-stdlib -Xswiftc -Osize
  BIN=$(swift build -c release --static-swift-stdlib --show-bin-path)/bootstrap
  strip "$BIN" || true
  cp "$BIN" /tmp/bootstrap-$LAMBDA
  cd $CODEBUILD_SRC_DIR
  find $SWIFT_CONTENDER_LAMBDA_PATH/$LAMBDA -mindepth 1 -delete
  mv /tmp/bootstrap-$LAMBDA $SWIFT_CONTENDER_LAMBDA_PATH/$LAMBDA/bootstrap
done
```

Notes:

- The Swift toolchain (`swift-6.3.2-RELEASE` for `amazonlinux2023-aarch64`) is
  installed in the `install` phase, with the tarball cached under `.swift-cache/`
  and verified against the Swift signing GPG key.
- `--static-swift-stdlib` builds bundle the Swift runtime into the binary so
  the Lambda doesn't need a Swift layer.
- After build, the contender directory is **emptied** and replaced by a single
  `bootstrap` binary — that is what `aws cloudformation package` zips.
- SwiftPM build artefacts are persisted via the CodeBuild S3 cache
  (`contenders/swift/*/.build/**/*`) so warm rebuilds are fast.

## CFN registration (`templates/contenders.yml`)

Two `AWS::Serverless::Function` blocks plus matching `AWS::Logs::LogGroup`
entries are registered:

- `SwiftSebstoClassicFunction` — `CodeUri: ../contenders/swift/sebsto-classic`
- `SwiftSebstoClassicAWSSDKFunction` — sibling experiment, see below

Both use `Runtime: provided.al2023`, `MemorySize: 512`, `Timeout: 600`,
`Architectures: [arm64]`, `Handler: swift.handler` (ignored), and reference
`LambdaContenderRole`. Their ARNs are added to the `ContenderArns` output
between the `BEGIN/END CONTENDER ARN LIST` markers so the Step Function picks
them up.

## Verification

1. **CI build passes**: CodePipeline for `demo-s3-archiving-ci` shows the
   Swift build block produced a `bootstrap` for both contenders.
2. **Stack deployed**: `demo-s3-archiving-root` → `CREATE_COMPLETE`, with
   the Swift function ARNs in `ContenderArns`.
3. **Successful invocation**: trigger the Step Function with the published
   `ContenderArns`. The contender should appear under `success` (not `failure`)
   in the ranked output.
4. **Control-Lambda check**: the in-state-machine control Lambda re-hashes the
   ZIP entries and validates entry-name == SHA-256(content). A failure here
   appears as `invalid: content hash mismatch`.
5. **Memory check**: CloudWatch `Max Memory Used` should be < 512 MB.
   Anything ≥ 510 MB is on the edge of OOM (Run 6 at 511 MB with the
   `maxDownloadsMemory=40 MiB` bump stalled and timed out — see Run 6).

## Sibling experiment — `swift-sebsto-classic-awssdk`

A second branch, `contender/swift-sebsto-classic-awssdk`, ports this code to
the AWS SDK for Swift (aws-sdk-swift) instead of Soto. Same architecture,
same backpressure model, same ZIP encoder, same CRC shim — only the SDK
calls differ:

- Dependency: `aws-sdk-swift` (AWSS3) instead of `soto` (SotoS3)
- HTTP client: aws-crt-swift (CRT) instead of AsyncHTTPClient (NIO)
- Body iteration: `Smithy.ByteStream.stream(stream).readAsync(upToCount:)`
  instead of Soto's native `AsyncSequence<ByteBuffer>` body
- Pool config: `HttpClientConfiguration.maxConnectionsPerEndpoint = 32`

It registered as `SwiftSebstoClassicAWSSDKFunction` and was benchmarked
against Soto in a 3-way run (Rust + Soto + AWS SDK). Result: **timed out at
600 s** while reaching ~2200/3000 entries (projected ~13 minutes total).
Soto remains the winning Swift variant. See `RESULTS.md` Run 9 for the run
data and the `contender/swift-sebsto-classic-awssdk` branch for the source
diff.
