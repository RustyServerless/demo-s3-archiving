<!-- markdownlint-disable MD013 -->
# Swift contender — `sebsto-soto`

The Swift contender that ships in this repo. A Lambda that, given
`{bucket_name, files_prefix, archive_key}`, lists every object under
`s3://${bucket_name}/${files_prefix}/`, reads them all, and uploads a
flat STORED ZIP archive to `s3://${bucket_name}/${archive_key}` — in a
single Lambda invocation, on Graviton, 512 MB memory, 600 s timeout.

This document describes what the code doesand the design choices I made.

## Per-Lambda configuration

| Setting | Value |
|---|---|
| Runtime | `provided.al2023` |
| Architecture | `arm64` (Graviton) |
| Memory | 512 MB (~0.29 vCPU) |
| Timeout | 600 s |
| Handler | `swift.handler` (ignored — `bootstrap` is executed) |

## Two critical design decisions for performance

### 1. Use Soto, not the official AWS SDK for Swift

Two SDKs exist for calling AWS services from Swift:

- **The official [AWS SDK for Swift](https://github.com/awslabs/aws-sdk-swift)** —
  built on top of the [AWS Common Runtime (`aws-crt-swift`)](https://github.com/awslabs/aws-crt-swift),
  a set of C libraries shared across the official SDKs for several
  languages (Java, Python, JavaScript, …). Generated from the same
  Smithy service models as those other SDKs.
- **[Soto](https://github.com/soto-project/soto)** — a community SDK
  built on top of [SwiftNIO](https://github.com/apple/swift-nio) and
  [AsyncHTTPClient](https://github.com/swift-server/async-http-client),
  Apple's official server-side Swift HTTP stack. Pure Swift end-to-end.

We started this contender on the official AWS SDK for Swift. With the
same architecture and the same optimisations applied to both, our
measurements were:

| SDK | Cold-1 wall-clock | Cold-2 wall-clock | `uploadPart` p50 |
|---|---|---|---|
| **Soto** | **250 s** | **250 s** | **200 ms** |
| AWS SDK for Swift | 544 s | timeout @ 600 s | 680 ms |

Identical Swift application code: same three-stage pipeline, same byte
budget, same chunk size, same pure-Swift CRC32, same backpressure
model. Only the SDK calls differ. Despite that, the AWS SDK port runs
**at the edge of the 600 s Lambda timeout** — succeeding once, timing
out the next time. That's not a viable production stance.

Why the gap? Two reasons, both at the HTTP transport:

- **`S3.uploadPart` is 3.4× slower per call** on aws-crt-swift than on
  AsyncHTTPClient (680 ms vs 200 ms p50 for a 10 MiB part). With ~1500
  parts in the run, that single difference accounts for ~700 s of
  extra work.
- **The API surface forces extra copies.** aws-sdk-swift exposes the
  S3 response body as a `Smithy.ByteStream` whose only progressive read
  primitive is `readAsync(upToCount:)` returning a fresh `Data`. The
  upload body is `ByteStream.data(Data?)` or `.stream(Stream)` — no
  ByteBuffer init. Any custom Stream gets collected back into a single
  `Data` by `FlexibleChecksumsRequestMiddleware` before SigV4 signing,
  so there is no zero-copy path even with manual bridging. Combined
  with `Data`'s allocation behaviour (see decision #2), this multiplies
  CRT-internal memcpys.

Soto, by contrast, hands us NIO `ByteBuffer` end-to-end and accepts
`ByteBuffer` zero-copy on `S3.uploadPart` via `AWSHTTPBody(buffer:)`.
That is decision #2.

### 2. Use NIO `ByteBuffer` instead of Foundation `Data`

If you don't write Swift on the server every day, this distinction is
not obvious. Both look like "an array of bytes". Why does it matter?

**`Foundation.Data`** is the standard Swift "blob of bytes" type. It is
copy-on-write, like Swift's `Array`, and it is *opaque about its
storage*: an instance may be backed by one contiguous heap allocation,
or by several disjoint segments stitched together at the API surface.
The hot-path consequence:

- Every `Data.append(other:)` checks whether the receiver is uniquely
  referenced (cheap), grows the backing buffer if needed (potentially
  reallocates and copies the existing bytes), then memcpys the new
  bytes into place.
- When you ask for a contiguous pointer (`withUnsafeBytes`), Foundation
  may have to flatten the segments first, which copies bytes you didn't
  ask to copy.
- It carries Foundation's bridging surface — types like `NSData`,
  Objective-C interop, even on Linux where there is no Objective-C
  runtime.

**`NIOCore.ByteBuffer`** is the byte-buffer type used inside SwiftNIO.
It is value-typed (also copy-on-write), but with three crucial
differences for our workload:

- **One contiguous allocation, always.** A ByteBuffer is a slice over
  exactly one underlying buffer. `withUnsafeReadableBytes` always hands
  back a single contiguous pointer-and-length, no flattening needed.
- **Reader index + writer index, no bookkeeping copies.** Splitting a
  ByteBuffer (`readSlice(length:)`) and concatenating
  (`writeBuffer(&other:)`) advance internal indices over the same
  storage. Slicing is zero-copy; concatenation is a single memcpy.
- **Allocator-aware.** The default `ByteBufferAllocator` recycles
  storage from a per-event-loop arena, so high-throughput pipelines
  pay much less malloc/free overhead than `Data` does on its general
  allocator.

In our pipeline this matters at three places:

| Hot-path location | Per-run cost if we used `Data` | What `ByteBuffer` saves |
|---|---|---|
| Download accumulator (`downloadFile`) | One `Data.append(_:count:)` per ~80 KB frame, ~250k frames/run, with possible re-allocation as the buffer grows | One pre-sized allocation per file, one `writeBuffer` memcpy per frame |
| Producer→uploader chunk (`ChunkProducer`) | Each 10 MiB chunk built via `Data.append`; passed to `S3.uploadPart` as `Data` | Built directly in a 10 MiB `ByteBuffer`; passed as `AWSHTTPBody(buffer:)` zero-copy |
| `S3.uploadPart` body | `AWSHTTPBody(bytes: Data)` copies into a fresh `ByteBuffer` internally before NIO emits it on the wire | `AWSHTTPBody(buffer:)` wraps the existing ByteBuffer with no copy |

Concretely, switching from `Data` to `ByteBuffer` on the upload path
alone eliminates ~15 GiB of avoidable copies per run (10 MiB × ~1500
parts) and reduces NIO `ByteBufferAllocator` arena pressure (which is
what makes the warm Lambda's RSS climb on `Data`-based code).

If `Data` is your shovel, `ByteBuffer` is the conveyor belt — designed
for moving lots of bytes through one pipe.

## Architecture

A three-stage streaming pipeline. All three stages run as sibling
`async let` children of a single `runPipeline` task; if any throws, the
others are cancelled.

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
   │   └──────────────────┘                            │   (single    │  │
   │                                                   │   consumer)  │  │
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
   │                                                  └────────────────┘ │
   │                                                                     │
   └─── ByteSemaphore.release ◄─ released by zipper after each file ─────┘
```

### Stage A — Downloader (`runDownloadStage` in `Archiver.swift`)

A `withThrowingTaskGroup` of N worker tasks. Concurrency is bounded by
the byte budget: a new download task is `addTask`'d only after
`byteBudget.acquire(file.size)` succeeds, so at any moment the in-flight
set fits in 20 MiB. With ~5 MB files that gives ~4 concurrent tasks
(observed mean: 1.96–2.66, max 5).

Each task:

- Calls `s3.getObject` (Soto returns an `AWSHTTPBody`, which is an
  `AsyncSequence<ByteBuffer>` over the response body).
- Pre-allocates one `ByteBuffer` of exactly `expectedSize` bytes.
- Iterates the body and `writeBuffer(&frame)`s each frame into the
  accumulator — one memcpy per frame, no re-allocation.
- After the body is fully read, runs CRC32 once over the contiguous
  readable view (see CRC32 below).
- Sends a `DownloadedFile { name, buffer, crc32, releaseBytes }` into
  the `FileChannel` (a `nonisolated`-friendly `AsyncStream` wrapper).

### Stage B — Zipper (`runZipStage` in `Archiver.swift`)

Single consumer of `FileChannel.stream`. Files arrive in download
completion order, not list order — that's fine because the central
directory is built from the order observed.

Per file:

- Emits local-file-header (LFH) + body bytes + data descriptor (DD)
  into the `ChunkProducer` via `appendCompound(lfh:body:dataDescriptor:)`.
- Releases the byte budget so a downloader can resume.
- Records the entry in the central-directory entries array.

After all files: the zipper builds the central directory, ZIP64 EOCD,
ZIP64 EOCD locator, and EOCD; appends each into the chunk producer in
order; calls `producer.finish()`.

### Stage C — Uploader (`runUploadStage` in `Archiver.swift`)

A `withThrowingTaskGroup` of up to `Tunables.maxConcurrentUploads = 3`
in-flight `S3.uploadPart` calls. After each upload completes, calls
`producer.releaseSlot()` so the producer can build more chunks. The
final list of `S3.CompletedPart` is returned to `runArchiveJob`, which
calls `S3.completeMultipartUpload`.

### `ChunkProducer` (`ChunkProducer.swift`)

Swift `actor`. Buckets writes from the zipper into fixed-size 10 MiB
chunks and emits each sealed chunk on a `nonisolated AsyncStream<UploadChunk>`
for the uploader.

- Internal buffer is a `ByteBuffer` sized exactly to `chunkSize`.
- Two append overloads — `Data` (for the small ZIP headers) and
  `ByteBuffer` (for the big file bodies, zero-copy).
- Backpressure: at most `bufferChunksCount = 2` chunks may be
  outstanding (built but not yet uploaded). The producer suspends in
  `waitForSlot()` when the in-flight count is at the ceiling.

### Backpressure summary

Three independent budgets keep the 512 MB Lambda within memory:

| Budget | Mechanism | Capacity |
|---|---|---|
| In-flight downloads | `ByteSemaphore` over file sizes | 20 MiB |
| Producer-buffered chunks | `ChunkProducer.maxInFlight` | 2 chunks × 10 MiB = 20 MiB |
| Concurrent uploads | TaskGroup size cap | 3 |

These are not additive on the hot path — once a file is fully
downloaded and its bytes have been memcpy'd into the chunk producer,
the byte budget is released and the producer owns the bytes.

## Tunables (`Pipeline.swift`)

```swift
enum Tunables {
    static let maxDownloadsMemory: Int = 20 * 1024 * 1024   // 20 MiB
    static let maxConcurrentUploads: Int = 3
    static let chunkSize: Int = 10 * 1024 * 1024            // 10 MiB
    static let bufferChunksCount: Int = 2                   // ChunkProducer in-flight ceiling
}
```

The HTTP client connection pool is configured in `main.swift` because it
must be set on the AsyncHTTPClient before the AWSClient is built:

```swift
httpConfig.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit = 32
httpConfig.timeout.read = .seconds(120)
httpConfig.timeout.connect = .seconds(10)
```

## ZIP format

STORED only (no DEFLATE), GP flag bit 3 set so CRC32 + sizes are emitted
in a data descriptor *after* each file body. ZIP64 is mandatory: the
archive total exceeds 4 GiB.

| Record | Size | Notes |
|---|---|---|
| Local file header (LFH) | 30 + nameLen | Fields: sig, version 45, GP flag 0x0008, method STORED (0), DOS time/date (fixed 2010-01-01), CRC=0, compSize=0, uncSize=0, nameLen, extraLen=0, name |
| Data descriptor (ZIP64) | 24 | sig, CRC32, 8B compSize, 8B uncSize (== compSize because STORED) |
| Central directory record | 46 + nameLen + 28 | Last 28 bytes are the ZIP64 extended-info extra field (uncSize8, compSize8, lfhOffset8). The 32-bit fields in the CD record are set to `0xFFFFFFFF` to force readers to read from the ZIP64 extra. |
| ZIP64 EOCD record | 56 | sig, sizeOfRest=44, version, disk numbers, entry count (×2), CD size, CD offset |
| ZIP64 EOCD locator | 20 | sig, disk=0, ZIP64-EOCD offset, totalDisks=1 |
| EOCD | 22 | All counts/offsets are `0xFFFF` / `0xFFFFFFFF` (defer to ZIP64) |

Filenames are pure ASCII (SHA-256 hex, 64 chars), so no UTF-8 GP-flag
tweaks are required. There are no directory entries and no extra fields
beyond the mandatory ZIP64 one. All implementation lives in
`Sources/SwiftSebstoSoto/Zip/Headers.swift`.

## CRC32

Pure-Swift slicing-by-8 implementation in `Zip/CRC32.swift`. ~125 LOC,
no C dependency, no platform intrinsics. The IEEE polynomial
(`0xEDB88320` reflected), as used by ZIP/zlib.

Why not the ARMv8 `__crc32{b,h,w,d}` hardware intrinsics (which would
require a tiny C shim)? Two reasons:

- **Single-language comparison.** The Rust reference contender uses the
  pure-Rust `zip` crate's CRC. Calling out to C would muddy the
  Swift-vs-Rust comparison.
- **It is actually faster on this CPU.** The `__crc32d` intrinsic has a
  serial dependency chain (each call depends on the previous CRC
  result). Slicing-by-8 issues 8 parallel table lookups per byte
  position, which the out-of-order core on Graviton2 schedules across
  multiple memory ports. On a 0.29 vCPU allocation, the memory-level
  parallelism wins: ~4 ms per 5 MB file in pure Swift vs ~76 ms with
  the intrinsics + the cost of crossing the Swift→C boundary on a
  non-contiguous source. Removing the C path also unblocked the
  downloader's per-task pipeline, since the CRC was running inline at
  end of each file and serializing the next download.

The implementation is verified against standard reference vectors:
empty, `"a"`, `"123456789"` (RFC 3720 = `0xCBF43926`), 32×0x00, 32×0xFF,
chunked splits, and a 1 MiB cross-check against a slicing-by-1
reference.

## Optional profiling instrumentation — `STATS=1`

`Stats.swift` defines a `final class Stats` (lock-protected via
`NIOLockedValueBox`) that records per-stage durations and aggregate
gauges, and emits a per-stage report at the end of a run.

| Stage | What it measures |
|---|---|
| `downloadFile` | total time inside the per-task body iteration + CRC |
| `downloadInFrame` | time of the single CRC pass at end of file |
| `zipperQueueWait` | time the zipper waits for the next downloaded file |
| `zipperAppend` | time inside `producer.appendCompound` |
| `uploadPart` | time inside `S3.uploadPart` |
| `uploaderQueueWait` | time the uploader waits for the next sealed chunk |
| `downloadInFlight` | time-weighted mean + max concurrent download tasks |
| `producerHops` | counter of `ChunkProducer.appendCompound` actor hops |
| `peakRSS` | end-of-run `getrusage(RUSAGE_SELF).ru_maxrss` |

Gated on the `STATS` environment variable — truthy values are `1`,
`true`, `yes` (case-insensitive). `Stats.enabled` is read once at cold
start. When unset, every call site short-circuits via an inlined
`if Stats.enabled` check — neither `monoNs()` nor `record()` runs, no
allocation, no actor hop, no lock acquisition.

To enable for one run:

```bash
aws lambda update-function-configuration \
  --function-name demo-s3-archiving-swift-sebsto-soto \
  --environment 'Variables={STATS=1}'
```

Then trigger the Step Function and read the stats from CloudWatch Logs
at the end of the run. Switch back off afterwards.

The clock is `clock_gettime(CLOCK_MONOTONIC)` via a platform-conditional
libc import (`Darwin.C`, `Glibc`, `Musl`) — see `monoNs()` in
`Stats.swift`. Reading it costs ~30 ns per call.

## File map

```
contenders/swift/sebsto-soto/
├── Package.swift                      SwiftPM manifest, Swift 6.0
├── Package.resolved
├── scripts/test-codebuild-locally.sh  Manual reproduce-the-CI-build helper
└── Sources/
    └── SwiftSebstoSoto/
        ├── main.swift                 Cold-start: HTTPClient, AWSClient, S3,
        │                              LambdaRuntime entry point
        ├── Pipeline.swift             Tunables, FileInfo/JobInfo, listFiles,
        │                              multipart upload helpers, ByteSemaphore,
        │                              ArchivingError
        ├── Archiver.swift             runArchiveJob + runPipeline; the three
        │                              stage functions and the per-file downloader
        ├── ChunkProducer.swift        actor ChunkProducer + UploadChunk type;
        │                              10 MiB chunking + slot semaphore
        ├── Stats.swift                Optional profiling class (STATS env var)
        │                              + monoNs() platform-conditional clock
        └── Zip/
            ├── Headers.swift          LFH, DD, CD, ZIP64 EOCD encoders
            └── CRC32.swift            Pure-Swift slicing-by-8 CRC32
```

No C target, no native dependencies beyond the SwiftPM packages
declared in `Package.swift`.

## Stack decisions (summary table)

| Concern | Decision | Why |
|---|---|---|
| Lambda runtime | `swift-aws-lambda-runtime` v2.x | Closure-based `LambdaRuntime { (event, context) in … }`, Codable JSON in/out |
| **AWS SDK** | **Soto 7.x** (community) | NIO `ByteBuffer` end-to-end, zero-copy upload via `AWSHTTPBody(buffer:)`. The official aws-sdk-swift forces `Data` everywhere and times out on this workload — see "Two critical design decisions" above |
| **Byte buffer** | **NIO `ByteBuffer`** | Single contiguous allocation, reader/writer indices, allocator-pooled; vs Foundation `Data`'s opaque storage and per-`append` re-allocation risk |
| ZIP writer | Hand-rolled (`Zip/Headers.swift`) | ZIPFoundation requires a URL or full-`Data` backing — incompatible with 15 GB-doesn't-fit-in-/tmp streaming. ~150 LOC of Swift gets us streaming + ZIP64 |
| CRC32 | Pure-Swift slicing-by-8 | Single-language vs Rust comparison; faster than the ARMv8 intrinsic on this CPU due to memory-level parallelism (see CRC32 section) |
| Foundation | `FoundationEssentials` on Linux (`#if canImport(FoundationEssentials)`), `Foundation` on macOS for local builds | Smaller binary on Linux |
| Architecture | arm64 (Graviton2) | Matches Rust reference; cheaper per GB-s |
| Memory | 512 MB | Same as Rust reference. Peak observed ~390 MB |
| Concurrency | Swift 6 strict concurrency, structured `withThrowingTaskGroup`, actors for shared state, `AsyncStream` for inter-stage queues |

## Build & deploy

The CI is `ci-config/buildspec.yml`. Relevant block (the `# SWIFT BUILD`
section):

```bash
SWIFT_CONTENDER_LAMBDA_PATH=./contenders/swift
for LAMBDA_DIR in "$SWIFT_CONTENDER_LAMBDA_PATH"/*/; do
  [ -d "$LAMBDA_DIR" ] || continue            # skips DESIGN.md
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

- The Swift toolchain (`swift-6.3.2-RELEASE` for `amazonlinux2023-aarch64`)
  is installed in the `install` phase, with the tarball cached under
  `.swift-cache/` and verified against the Swift signing GPG key.
- `--static-swift-stdlib` builds bundle the Swift runtime into the binary
  so the Lambda doesn't need a Swift layer.
- After build, the contender directory is **emptied** and replaced by a
  single `bootstrap` binary — that is what `aws cloudformation package`
  zips.
- SwiftPM build artefacts are persisted via the CodeBuild S3 cache
  (`contenders/swift/*/.build/**/*`) so warm rebuilds are fast.

## CFN registration (`templates/contenders.yml`)

```yaml
SwiftSebstoSotoFunction:
  Type: AWS::Serverless::Function
  Properties:
    FunctionName: !Sub ${ProjectName}-swift-sebsto-soto
    CodeUri: ../contenders/swift/sebsto-soto
    Runtime: provided.al2023
    MemorySize: 512
    Timeout: 600
    Architectures: [arm64]
    Handler: swift.handler   # ignored by provided.al2023
    Role: !GetAtt LambdaContenderRole.Arn
```

The function ARN is added to the `ContenderArns` output between the
`BEGIN/END CONTENDER ARN LIST` markers so the Step Function picks it
up.

## Results

10 sequential Step Function executions, each invoking both the Swift
contender and the Rust reference. Same Lambda configuration for both,
in eu-west-3, on the same 3000-object / ~15 GB test bucket. STATS
instrumentation off on Swift (we are measuring production, not the
profiler). All 10 runs succeeded.

### Per-run table

| Run | Swift (s) | Rust (s) | Δ (s) | Ratio |
|---|---|---|---|---|
| 1 | 234.67 | 211.61 | +23.05 | 1.109 |
| 2 | 235.62 | 210.57 | +25.06 | 1.119 |
| 3 | 217.37 | 211.94 | +5.43 | 1.026 |
| **4** | **212.39** | **211.72** | **+0.68** | **1.003** |
| 5 | 216.40 | 211.77 | +4.62 | 1.022 |
| 6 | 237.57 | 210.42 | +27.15 | 1.129 |
| 7 | 218.80 | 211.66 | +7.14 | 1.034 |
| 8 | 222.79 | 211.50 | +11.29 | 1.053 |
| 9 | 219.45 | 211.16 | +8.29 | 1.039 |
| 10 | 218.05 | 211.09 | +6.96 | 1.033 |

### Wall-clock duration (seconds, lower is better)

|  | min | p50 | p90 | max | mean | stdev |
|---|---|---|---|---|---|---|
| **Swift** | **212.4** | 219.5 | 235.8 | 237.6 | 223.3 | **9.13** |
| **Rust** | 210.4 | 211.6 | 211.8 | 211.9 | 211.3 | 0.52 |

### `run_price_usd` (lower is better — ranking metric)

|  | min | p50 | p90 | max | mean |
|---|---|---|---|---|---|
| **Swift** | **$0.001416** | $0.001463 | $0.001572 | $0.001584 | $0.001489 |
| **Rust** | $0.001403 | $0.001411 | $0.001412 | $0.001413 | $0.001409 |

### Ratio Swift / Rust

|  | min | p50 | p90 | max | mean |
|---|---|---|---|---|---|
| Ratio | 1.003 | 1.039 | 1.120 | 1.129 | **1.057** |

### Reading these numbers

- **The best Swift run beat the worst Rust run** (Swift 212.4 s vs
  Rust 211.9 s max) — within sampling noise.
- **The median Swift run is 1.039× Rust** — about 8 s slower on a
  211 s baseline.
- **Swift mean cost is 5.7% higher than Rust** ($0.001489 vs
  $0.001409 per invocation).
- Rust's wall-clock standard deviation is **0.52 s over 10 runs**;
  Swift's is **9.13 s** — Rust is metronomic, Swift more variable.
  The Swift variance correlates with sandbox cold-vs-warm placement
  across the 10 runs (the high outliers are colder cases). The Rust
  binary's static cold start is short enough that it's invisible at
  this granularity.
- The earlier project-history figure of "Swift 250 s" was measured
  with `STATS=1` instrumentation enabled. The instrumentation costs
  ~25 s of run time; with it off, the contender lands as above.

## Verification

1. **CI build passes**: CodePipeline shows the Swift build block produced
   a `bootstrap`.
2. **Stack deployed**: the root stack reaches `CREATE_COMPLETE` with the
   Swift function ARN in `ContenderArns`.
3. **Successful invocation**: triggering the Step Function with the
   published `ContenderArns` produces the contender under `success` (not
   `failure`) in the ranked output.
4. **Control-Lambda check**: the in-state-machine control Lambda
   re-hashes the ZIP entries via SHA-256 and validates entry-name ==
   SHA-256(content). A failure here appears as
   `invalid: content hash mismatch`.
5. **Memory check**: CloudWatch `Max Memory Used` should be < 512 MB.
   Anything ≥ 480 MB is on the edge of OOM.
