<!-- markdownlint-disable MD013 -->
# C contender — `sebsto`

A single-Lambda implementation of the
[demo-s3-archiving](../../README.md) benchmark contract, written in
plain C on top of the **AWS Common Runtime (CRT)** — the same set of
C libraries that the Rust SDK uses underneath via `aws-sdk-s3`.

Mirrors the Rust reference's three-stage `download → zip → upload`
pipeline in ~1 100 LOC of C. STORED-only ZIP with ZIP64 records,
hardware-accelerated CRC32 from `aws-checksums`, no SDK abstraction —
the contender talks directly to `aws-c-s3` for parallel ranged GETs and
multipart upload.

## Per-Lambda configuration

| Setting | Value |
|---|---|
| Runtime | `provided.al2023` |
| Architecture | `arm64` (Graviton) |
| Memory | 512 MB (~0.29 vCPU) |
| Timeout | 600 s |
| Handler | `c.handler` (ignored — `bootstrap` is executed) |

## Architecture

Same shape as the Rust reference, with one notable simplification: no
hand-written ring buffer between the zipper and the uploader. The CRT
exposes `aws_s3_meta_request_write()` (`send_using_async_writes=true`)
which buffers ZIP bytes internally and turns each contiguous part into
a parallel `UploadPart` HTTP call.

```
                        +--------------------+
ListObjectsV2 (CRT) --> | downloader (zipper |
                        |  thread orchestrates|
                        |  parallel GETs)    |
                        +---------+----------+
                                  | downloaded bytes
                                  v
                        +---------+----------+
                        | zip_writer (STORED |
                        |  + CRC32 via       |
                        |  aws_checksums)    |
                        +---------+----------+
                                  | accumulator (8 MiB)
                                  v
                +-----------------+------------------+
                | aws_s3_meta_request_write +        |
                |   PUT_OBJECT meta-request          |
                |   (parallel multipart parts)       |
                +-------------------------------------+
```

### Tunables

All in [`src/main.c`](sebsto/src/main.c) at the top of the file:

| Constant | Value | Notes |
|---|---|---|
| `MAX_CONCURRENT_DOWNLOADS` | 64 | Parallel `GetObject` meta-requests |
| `MAX_DOWNLOAD_INFLIGHT_BYTES` | 32 MiB | Memory cap on pending downloads |
| `UPLOAD_BATCH_BYTES` | 8 MiB | Accumulator before each CRT write |
| `CRT_PART_SIZE_BYTES` | 8 MiB | Multipart part size for the final PUT |
| `CRT_THROUGHPUT_GBPS` | 100.0 | CRT connection-pool sizing hint |

### Why STORED only

Source files are random bytes (each filename is the SHA-256 of the
content), so DEFLATE'ing them costs CPU without reducing size. The
control Lambda just rehashes each entry's decompressed bytes, so STORED
is contract-correct.

### CRC32 — `aws-checksums`, not zlib

The CRT ships [`aws-checksums`](https://github.com/awslabs/aws-checksums)
which uses ARMv8 hardware CRC32 instructions on Graviton (and PCLMULQDQ
on x86\_64). On the 0.29 vCPU Lambda the difference is measurable —
zlib's slicing-by-4 was visible on a flame graph; the CRT's intrinsic
implementation drops out of the top frames.

### Async writes — not a custom `aws_input_stream`

The first cut used a thread-safe ring buffer fronted by a custom
`aws_input_stream` whose `read()` callback blocked on the ring. This
deadlocked: CRT explicitly warns that blocking input streams can stall
the S3 client's I/O threads, and in practice it did — every invocation
hit the 600 s Lambda timeout.

The current version uses `aws_s3_meta_request_write()` with
`send_using_async_writes=true`. The zipper thread pushes 8 MiB
accumulated batches, each `write()` returns a future, we
`aws_future_void_wait()` on it before issuing the next. CRT does the
heavy lifting — buffering, signing, multipart bookkeeping, retries —
internally on its event-loop threads.

## Build

The contender's CodeBuild step clones the CRT repos at pinned tags and
CMake-builds them once into a CodeBuild-cached prefix:

| Library | Tag |
|---|---|
| `awslabs/aws-c-common` | v0.14.0 |
| `aws/s2n-tls` | v1.7.3 |
| `awslabs/aws-c-cal` | v0.9.14 |
| `awslabs/aws-c-io` | v0.26.3 |
| `awslabs/aws-checksums` | v0.2.10 |
| `awslabs/aws-c-compression` | v0.3.2 |
| `awslabs/aws-c-http` | v0.11.0 |
| `awslabs/aws-c-sdkutils` | v0.2.4 |
| `awslabs/aws-c-auth` | v0.10.3 |
| `awslabs/aws-c-s3` | v0.12.4 |

After the CRT prefix exists, `make` in the contender directory
produces a `bootstrap` ELF binary statically linked against every CRT
library and dynamically linked against `libcrypto`, `libz`,
`libpthread`, `libdl`, `libm`, and `librt` — all of which AL2023 Lambda
ships in its base layer.

The CRT prefix is ~25 MB and gets cached by CodeBuild keyed on the tag
list above. Cold-build is ~5 minutes; warm-build is ~1 minute.

A working `bootstrap` is ~5.4 MB (stripped).

## Results

Median of three Step Function executions on the same configuration
(512 MB / arm64 / 600 s, eu-west-3, 3 000 files / ~15 GB, 10 parallel
runs per execution):

|  | min | mean | max | stdev |
|---|---|---|---|---|
| **C** wall-clock (s) | 213.5 | **252** | 317 | 41 |
| **Swift** wall-clock (s) | 217 | 231 | 249 | 12 |
| **Rust** wall-clock (s) | 209 | **211** | 213 | 1 |

Best C run is **2 s slower than Rust's mean** (213.5 vs 211.3) — when
Lambda VM placement cooperates we are essentially tied with the Rust
SDK that wraps the same CRT we use directly. The C contender's **mean
is ~1.20× Rust** and the **stdev is 80× higher**, driven by a bimodal
distribution where ~half of the 10 invocations cluster around 215 s
(matching Rust) and the other half around 315 s.

This bimodal pattern is reproducible across runs and does NOT appear in
the Rust reference on the same Step Function executions, so it is
specific to the C code, not Lambda VM noise. The most likely cause is
that the zipper thread blocks the download dispatcher whenever it
waits on a CRT write future — a race the Rust reference avoids by
keeping its three pipeline stages on independent tokio tasks. Open to
suggestions.

## Files

- `contenders/c/README.md` — this file.
- `contenders/c/sebsto/Makefile` — single-target build, depends on
  `$CRT_PREFIX`.
- `contenders/c/sebsto/src/` — six C files (~1 100 LOC):
  - `main.c` — CRT bootstrap, runtime API loop, pipeline orchestration.
  - `runtime.c/h` — minimal Lambda Runtime API client (HTTP/1.1 over
    plain TCP, blocking).
  - `zip.c/h` — STORED-only ZIP encoder with ZIP64.
  - `json.c/h` — flat-object JSON value extractor (just enough to
    parse the Lambda event payload).
  - `util.c/h` — `xmalloc`/`buf_t` and small helpers.
- `templates/contenders.yml` — `CSebstoFunction` block + ARN in the
  `ContenderArns` output.
- `ci-config/buildspec.yml` — `# C BUILD` block: clones the pinned CRT
  tags into a cached install prefix, then `make` in each contender
  directory and replaces the source tree with the `bootstrap` binary.
