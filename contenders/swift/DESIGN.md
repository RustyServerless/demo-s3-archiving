# Plan — Add a Swift contender to `demo-s3-archiving`

> ## Postscript: experiment outcome (2026-05, supersedes the recommendation below)
>
> The two-PR plan was executed. The predicted-layout `swift-sebsto` contender
> was deployed and benchmarked, and it **lost** to the simpler `swift-sebsto-classic`
> port:
>
> | Contender | Best run | vs Rust (~213 s) |
> |---|---|---|
> | `rust-jeremie-rodon` | 213 s | 1.00× |
> | `swift-sebsto-classic` (3-stage) | 372 s (run 1), 367 s (run 7) | ~1.74× |
> | `swift-sebsto` (predicted-layout) | 532 s (run 3) | ~2.50× |
>
> **Why predicted-layout did not win.** Removing the central serial zipper moved
> the bottleneck onto `PartActor`, which is hit by *every* NIO frame rather than
> once per file. That is roughly 250k actor hops × ~1 µs ≈ 250 ms of pure hop
> overhead, plus the matching continuation allocations. The classic pipeline's
> single zipper actor is hit only ~3000 times (once per file) and totals 4.4 s
> over the whole run, so the structural "win" of predicted-layout was a loss in
> practice.
>
> **Why we are not pursuing `sebsto v2` (the lock-light NIOLockedValueBox redesign
> documented in `DESIGN-LOCK-LIGHT.md`).** The run-5 instrumentation on classic
> revealed the actual bottleneck: per-task S3 GET throughput is ~12 MB/s vs
> Rust's ~35 MB/s. The downloader stage sums to 1256 s of work; the zipper
> stage sums to 4.4 s. The entire architecture-level optimisation thread above
> — predicted-layout, lock-light, Span, etc. — was attacking the wrong target.
> The gap is in the per-request SDK path (Soto + AsyncHTTPClient on Lambda),
> not in the Swift concurrency model.
>
> **Pivot.** Work has therefore moved to:
>
> 1. Stay with the simpler 3-stage classic architecture (`swift-sebsto-classic`)
>    rather than continue the predicted-layout track.
> 2. Test whether **AWS SDK for Swift** (aws-crt-swift) closes the per-request
>    throughput gap that Soto/AsyncHTTPClient leaves open, in
>    `swift-sebsto-classic-awssdk`.
>
> The `sebsto` directory and AWS stacks (`demo-s3-archiv-sebsto-*`) and the
> `contender/swift-sebsto` branch were deleted as part of this cleanup.
>
> See `contenders/swift/RESULTS.md` for the run-by-run history and profiling
> tables.
>
> ---
>
> The original two-contender plan is kept below for context. Treat the
> `swift-sebsto` (predicted-layout) sections as a record of what was tried, not
> as a current recommendation.

## Context

This forked repository is a Lambda-archiving benchmark: a Step Function invokes
each registered contender Lambda once with `{ bucket_name, files_prefix,
archive_key }`, expecting it to read ~3000 objects (~15 GB) from
`s3://${bucket_name}/${files_prefix}/` and upload a flat STORED ZIP to
`s3://${bucket_name}/${archive_key}` within 600 s. A control Lambda re-hashes
every entry's decompressed bytes against the entry name (entry name == basename
== SHA256 hex of content). The reference Rust contender lands at ~215 s with
~350 MB peak RSS in a 512 MB Lambda using a 3-stage `download → zip → upload`
pipeline (20 MB byte-budget for downloads, 10 MB ring slabs, 3 concurrent
multipart parts).

**Goal**: add a Swift implementation under `contenders/swift/sebsto/`,
optimised for `provided.al2023` arm64 Lambda, that beats or matches the Rust
reference on the project's ranking metric (`run_price_usd`).

We will ship **two PRs / two contenders**:

1. **`swift-sebsto-classic`** — direct port of the Rust three-stage pipeline.
   Establishes correctness against the control Lambda and a Swift baseline.
2. **`swift-sebsto`** — predicted-layout, parallel-writer variant (the actual
   performance bid). The `swift-sebsto-classic` contender remains in the repo
   for A/B comparison until we drop it.

This plan covers **both** contenders. The first one we land is the classic
port; the predicted-layout variant follows as a separate PR but its design is
documented here so the shared infrastructure (Package, CI, ZIP writer) is
factored correctly from day one.

## Stack decisions

| Concern | Decision | Why |
|---|---|---|
| AWS SDK | **Soto 7.x** | AsyncHTTPClient (swift-nio) → smaller binary, lower cold start, native `AsyncSequence<ByteBuffer>` streaming bodies. AWS SDK for Swift uses aws-crt-swift (developer preview) with bigger Linux footprint. |
| Lambda runtime | `swift-aws-lambda-runtime` v2.x | Closure-based `LambdaRuntime { (event, context) in … }`, Codable JSON in/out. |
| Build | `swift package archive` plugin | Produces a `bootstrap`-shaped artifact via `swift:amazonlinux2023` Docker image. We will adapt it for the project's CI bind-shape (`bootstrap` binary in the contender directory). |
| ZIP writer | **Hand-rolled** | ZIPFoundation requires a URL or full-`Data` backing — incompatible with both 15 GB-doesn't-fit-in-/tmp and predicted-layout's offset-indexed writes. ~300 LOC of Swift gets us streaming + ZIP64. |
| CRC32 | Hardware-accelerated via tiny C shim using ARMv8 `__crc32{b,h,w,d}` intrinsics. Fall back to zlib `crc32_z` if intrinsics unavailable. |
| Architecture | arm64 (Graviton2) | Matches Rust reference; cheaper per GB-s. |
| Memory | start at 512 MB | Predicted-layout design has ~150 MB headroom; classic ~250 MB. We may tune in CFN if the predicted variant is comfortable below 512. |
| Concurrency | Swift 6 strict concurrency, `withThrowingTaskGroup`, actors for shared state, `AsyncChannel`-style bounded queues for backpressure. |

## Architecture — `swift-sebsto-classic` (PR 1, baseline)

Direct Swift port of `contenders/rust/jeremie-rodon/src/main.rs` and friends:

- **Stage A — Downloader**: a `TaskGroup` of N=10 child tasks, each pulling
  `FileInfo` from a bounded async queue and pushing
  `(name, [ByteBuffer], permits)` into another bounded queue. A counting
  semaphore actor enforces 20 MB total in-flight download bytes.
- **Stage B — Zipper**: single actor receives in arrival order, writes ZIP
  local-file-header (GP flag bit 3 set), streams data through CRC32 +
  byte-emit, writes data descriptor. Output goes through a fixed-slab ring
  buffer (port of `slabs_ring.rs`) of 2×10 MB slabs.
- **Stage C — Uploader**: 3 child tasks pull sealed slabs and call
  `uploadPart`. Part numbers stamped at enqueue time. Final ZIP
  central-directory + ZIP64 EOCD emitted into the last slab(s).
- **CRC32**: per-entry, hardware-accelerated, computed inline as bytes flow
  through the zipper.

**Memory bound**: ~140–180 MB peak. **Expected runtime**: 210–220 s.

## Architecture — `swift-sebsto` (PR 2, performance bid)

The serial zipper is the only structural bottleneck in the Rust design.
Removing it requires that ZIP layout be planned up-front, which is possible
because every entry uses STORED, every size is known from `ListObjectsV2`, and
local-file-header size is `30 + nameLen` (no extra fields):

1. **List + plan**: paginated `listObjectsV2` returns name+size for all 3000
   objects. Compute the absolute archive offset of every entry's LFH and data
   region. Produce a `[Part]` plan where each `Part` is an 8 MB slot whose
   content is fully predicted (which entries' LFH + data + descriptor land in
   it, plus the trailing CD bytes if it is the last part).
2. **Concurrent multipart upload start** (overlap with listing).
3. **Downloaders**: `withThrowingTaskGroup` of N=12 tasks. Each task picks a
   file, streams its body via `getObject().body` (an `AsyncSequence<ByteBuffer>`),
   feeds bytes into a `PartActor` at the file's predicted absolute offset.
   CRC32 is computed inline in the downloader and written into the data
   descriptor at its predicted offset on completion.
4. **PartActor**: holds at most K=8 8 MB `ByteBuffer` parts open at once.
   Sealing is by byte-counter: when a part has received all the bytes it
   was predicted to contain, it is handed to the upload group. Out-of-order
   producer arrivals are fine — `Part.partNumber` is fixed at planning time.
5. **Uploaders**: 6 concurrent `uploadPart` calls. The `[CompletedPart]`
   collected and sorted by partNumber for `completeMultipartUpload`.
6. **Backpressure**: two semaphores — `downloadBytesInFlight ≤ 24 MB` and
   `openPartsCount ≤ 8`.
7. **Central directory**: pre-sized at planning time, materialised into the
   last part(s) by a final task that writes after every `Part` it touches has
   been sealed.

**Memory bound**: ~130–150 MB peak. **Expected runtime**: 205–212 s.

### ZIP layout details (both variants)

- Local file header: 30 bytes + nameLen, GP flag bit 3 (data descriptor) set.
- Data descriptor (24 bytes, ZIP64): signature, CRC32, compressed size (8B),
  uncompressed size (8B). Both sizes equal (STORED).
- Central directory record: 46 bytes + nameLen + 28-byte ZIP64 extra field
  (sizes + LFH offset).
- ZIP64 EOCD record (56 bytes) + ZIP64 EOCD locator (20 bytes) + EOCD (22 bytes).
- ZIP64 is mandatory because total > 4 GB (15 GB).
- Filenames are pure ASCII (SHA256 hex, 64 chars), so no UTF-8 GP flag tweaks
  required. No directory entries, no extra fields beyond ZIP64.

## Repository layout

```
contenders/
  swift/
    sebsto/                 # PR 2: predicted-layout, parallel-writer
      Package.swift
      Sources/SwiftSebsto/main.swift
      Sources/SwiftSebsto/Handler.swift
      Sources/SwiftSebsto/Plan.swift          # offset arithmetic, layout planner
      Sources/SwiftSebsto/PartActor.swift     # actor sealing predicted parts
      Sources/SwiftSebsto/Downloader.swift
      Sources/SwiftSebsto/Uploader.swift
      Sources/SwiftSebsto/Zip/Headers.swift   # LFH/CD/EOCD encoders (shared)
      Sources/SwiftSebsto/Zip/CRC32.swift     # ARMv8-accelerated CRC
      Sources/CCRC32/include/ccrc32.h         # hardware CRC32 C shim
      Sources/CCRC32/ccrc32.c
    sebsto-classic/         # PR 1: 3-stage port
      Package.swift
      Sources/SwiftSebstoClassic/...          # mirrors Rust crate structure
      (shares Zip/* code with sebsto via local Package dep or copy)
```

Each contender is a standalone SwiftPM package because (a) the CI replaces the
contender directory with a single `bootstrap`, so the source must be
self-contained per directory; (b) the project does not have a Swift workspace
analogue to the Rust workspace; (c) two independent packages let us version
and tune each contender separately.

## Build pipeline (CI)

Touch points in `ci-config/buildspec.yml`:

- Add a **SWIFT BUILD** block in the `build` phase, paralleling the Go
  example. Approach: run `swift package archive --base-docker-image
  swift:amazonlinux2023` *inside* the existing CodeBuild image. The plugin
  will pull the Swift Linux toolchain via Docker (CodeBuild's
  `amazonlinux-aarch64-standard:4.0` provides a docker daemon).
  - Iterate over `./contenders/swift/*` directories.
  - For each, `cd` into it, run the archive plugin, then extract the
    `bootstrap` binary from
    `.build/plugins/AWSLambdaPackager/outputs/AWSLambdaPackager/<Target>/<Target>.zip`.
  - Replace the directory contents with just `bootstrap` (matches the Rust /
    Go pattern that `aws cloudformation package` then zips verbatim).
- No changes to `pre_build` (no Swift dep caching for this first pass — the
  archive plugin's container has its own cache; revisit if cold builds become
  painful).
- Verify the CodeBuild image has a docker daemon by adding a
  `docker --version` echo to the install phase before the Swift build runs.

## CFN registration (`templates/contenders.yml`)

Two new resource pairs inserted between the BEGIN/END CONTENDERS markers,
plus two ARN entries between the BEGIN/END CONTENDER ARN LIST markers.
Logical IDs: `SwiftSebstoFunction` / `SwiftSebstoFunctionLogGroup`, and
`SwiftSebstoClassicFunction` / `SwiftSebstoClassicFunctionLogGroup`. Both
copy the `RustJeremieRodonFunction` block; only the FunctionName, CodeUri,
and Handler change. Reuse `LambdaContenderRole`. Set `Runtime: provided.al2023`,
`MemorySize: 512`, `Timeout: 600`, `Architectures: [arm64]`,
`Handler: swift.handler` (ignored by the runtime; matches project convention).

## Critical files

To **modify**:
- `templates/contenders.yml` — register both contenders + add two ARNs.
- `ci-config/buildspec.yml` — add a SWIFT BUILD block.

To **create**:
- `contenders/swift/sebsto/` — predicted-layout package (PR 2).
- `contenders/swift/sebsto-classic/` — 3-stage port (PR 1).

To **read for reference** (do not modify):
- `contenders/rust/jeremie-rodon/src/main.rs` — orchestration to port.
- `contenders/rust/jeremie-rodon/src/zipper.rs` — ZIP encoder semantics, GP
  flag bit 3, data descriptor layout.
- `contenders/rust/jeremie-rodon/src/slabs_ring.rs` — backpressure model the
  classic variant ports.
- `benching/control-lambda/src/main.rs` — verifier expectations.
- `templates/benching.asl.json` — InvokeContender event payload shape (no
  retries, errors classified as crash/timeout/invalid).

## Verification

1. **Local correctness — handler unit test**: synthesise a `bucket_name /
   files_prefix / archive_key` event from a small fake bucket (LocalStack or
   a real test bucket of ~10 objects), run `swift run` with the
   `LOCAL_LAMBDA_HOST` server, validate the produced ZIP locally with
   `unzip -l` and a SHA256 cross-check on each entry.
2. **CI build**: push the branch, observe the CodePipeline build emits a
   `bootstrap` binary in `contenders/swift/sebsto*/` and that
   `aws cloudformation package` zips it.
3. **End-to-end deploy**: the CI deploys `demo-s3-archiving-root`. Verify
   both Swift functions appear in `ContenderArns` output.
4. **Benchmark run**: trigger the Step Function with the published
   `ContenderArns`. Read the ranked output. Confirm:
   - Both Swift contenders appear in `success` (not `failure`).
   - `swift-sebsto` `run_price_usd` < `rust-jeremie-rodon`'s (the goal).
   - `swift-sebsto-classic` is within 10% of the Rust baseline (parity check).
5. **Memory headroom check**: read CloudWatch `Max Memory Used` for both
   functions. If `swift-sebsto` is under 350 MB on every run, file a follow-up
   to bump the function down to 384 MB and re-rank by `run_price_usd`.
6. **Failure-mode probes**: deliberately corrupt one entry name in a one-off
   manual test (point the function at a fixture bucket whose hash doesn't
   match content) — confirm the control Lambda's `invalid: content hash
   mismatch` reason surfaces correctly through Step Functions.

## Open questions to resolve while implementing

1. Does `swift package archive` work inside CodeBuild's
   `amazonlinux-aarch64-standard:4.0` image (needs Docker-in-Docker)? If not,
   fall back to manually invoking `swift build --static-swift-stdlib` inside a
   `swift:amazonlinux2023` container we run by hand.
2. Soto's `S3.UploadPartRequest` body — confirm it accepts an
   `AsyncSequence<ByteBuffer>` with explicit `length` and that no 3× SDK copy
   occurs (test by uploading one 10 MB part with peak-RSS observed via
   `getrusage`).
3. AsyncHTTPClient connection pool size for parallel `getObject` calls — does
   the default 8 connections suffice for N=12 download tasks, or must we raise
   the pool ceiling?
4. Does S3 negotiate HTTP/2 with AsyncHTTPClient? If yes, raise N concurrent
   requests cheaply; if no, stick with HTTP/1.1 connection pool sizing.
5. ARMv8 `__crc32` intrinsics availability inside the Lambda runtime kernel
   (should be fine on Graviton2; verify with a one-off `cat /proc/cpuinfo`).
