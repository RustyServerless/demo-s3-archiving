<!-- markdownlint-disable MD013 -->
# Figment Engine — S3 archiving contenders

Two single-Lambda contenders for the
[demo-s3-archiving](https://github.com/RustyServerless/demo-s3-archiving)
benchmark, built around one idea taken in two directions.

The benchmark contract: given `{bucket_name, files_prefix, archive_key}`,
archive every object under `s3://${bucket_name}/${files_prefix}/` into one
flat **STORED** ZIP64 at `s3://${bucket_name}/${archive_key}` — in a single
invocation, on an arm64 / `provided.al2023` Lambda with a 600 s timeout. The
benchmark bucket holds ~3 000 objects totalling ~14.65 GB. Contenders are
ranked on `run_price_usd` = `memory_mb × duration` (Lambda cost only).

## The two contenders

Both rest on the same insight — **don't move bytes that don't have to move** —
but optimise for different things.

| Contender | Optimises | Duration | Memory | Notes |
|---|---|---|---|---|
| [`figment-engine`](contenders/rust/figment-engine/README.md) | **price** | ~TODO_FE_DURATION | 640 MB | One MPU, alternating copy/stream + "steal". Throttle-safe. |
| [`figment-engine-chain`](contenders/rust/figment-engine-chain/README.md) | **speed** | ~TODO_CHAIN_DURATION | 1024 MB | Copy-only segment chain. Almost zero bytes cross the Lambda. |

<!--
  NUMBERS TO FILL once the sequential-harness benchmark run completes, so both
  contenders and the reference are measured under ONE harness and are directly
  comparable. Current placeholders:
    figment-engine        ~116 s solo (single-MPU, price-optimal)
    figment-engine-chain  ~15 s solo (copy-only, speed-optimal; uncontended)
    reference (jrodon-v2) ~106 s
  Replace TODO_* above with the harness-measured figures and state the harness
  conditions explicitly (sequential contenders, repeat-runs bounded).
-->

For comparison, the reference single-Lambda designs:

| Reference | Duration | Approach |
|---|---|---|
| jrodon (Gen1) | ~211 s | Streaming — every byte round-trips the ENI |
| jrodon-v2 (Gen2) | ~106 s | UploadPartCopy, single Lambda |

(Paul Santus's ~6 s and ~41 s results are **multi-Lambda** — a ~1 500-worker
fan-out and a Distributed-Map version respectively — so they are out of the
single-Lambda class these contenders compete in. They're a useful upper bound
on what the whole fleet can do, not a single-Lambda comparison.)

## The shared insight: the ENI is the bottleneck

A Lambda archiving S3 objects has one scarce resource — its **elastic network
interface (ENI)** bandwidth, which scales with configured memory. The
reference streams every object **through** the Lambda: `GET` it down the ENI,
`UploadPart` it back up. For ~14.65 GB that is ~29 GB across the ENI, and that
round-trip is essentially the whole runtime.

```
  Reference: every byte makes a round trip through the Lambda ENI

      S3 (files/)                 Lambda                  S3 (archive)
          │                         │                           │
          │ ─────── GET ──────────► │ ──────  UploadPart ─────► │
          │      (down ENI)         │       (up ENI)            │
          └── ~14.65 GB down ───────┴──── ~14.65 GB up ─────────┘
                         ≈ 29 GB total over the ENI
```

S3's **`UploadPartCopy`** moves an object (whole or ranged) into a multipart
upload **server-side** — the bytes go S3→S3 and never touch the Lambda ENI.
Both contenders are built on assembling the archive out of server-side copies
so the ENI carries as little as possible.

```
  The shared idea: copies stay server-side, off the ENI

      S3 (files/)                 Lambda                  S3 (archive)
          │                         │                           │
          │ ══════ UploadPartCopy (server-side, off-ENI) ═════► │
          └─────────────────────────────────────────────────────┘
```

`UploadPartCopy` doesn't get you a valid ZIP for free, though, because of one
hard S3 rule: **every multipart-upload part except the last must be ≥ 5 MiB.**
The benchmark data splits almost exactly across that floor:

| | count | bytes |
|---|---|---|
| **"bigs"** (≥ 5 MiB) | ~1 488 | ~8.44 GB |
| **"smalls"** (< 5 MiB) | ~1 512 | ~6.21 GB |

A big clears the floor and can be its own copy part. A small can't be a
standalone non-last part. **How each contender resolves the floor — and the
ZIP-header, CRC, and library problems that come with it — is what makes them
different.** The two READMEs tell those stories.

## Two answers to the same floor

**`figment-engine` — optimise for price.** Builds the whole archive as **one
multipart upload** of alternating *copy* parts (bigs, server-side) and *stream*
parts (batches of smalls, on-ENI, built up past the floor). A big's header
rides the tail of the preceding stream part so it sits adjacent to its copied
body. The **"steal"** trick streams just enough of a big's prefix to lift a
stream part over the floor, then copies the remainder — letting nearly every
big stay off the ENI. Writing to **one key** at a modest rate, it never trips
S3 throttling. Ships at 640 MB because, on the `memory_mb × duration` metric,
640's speed-up exactly cancels its memory premium versus 512. **The
price-optimal entry.**

**`figment-engine-chain` — optimise for speed.** Goes further: it copies the
*smalls* too, conceding only **one 5 MiB bootstrap read** for the very first
entry. Every other body — big and small — reaches the archive by
`UploadPartCopy`. It does this by building each big-plus-its-smalls as a short
**chain of per-segment MPUs** ("links") joined by copy-forward, then
copy-stitching the segments into the final archive. Because almost no bodies
cross the ENI, it escapes the bandwidth floor entirely and is bound only by S3
call latency — landing far faster than the single-MPU design. **The
speed-optimal entry, and (as far as we know) the first copy-only single-Lambda
archive in this challenge.**

A note worth making, because the two designs appear to disagree: the
`figment-engine` README argues that spreading writes across many temporary
objects "reliably trips S3 SlowDown". That's true of an *unbounded burst* of
writes. The chain shows the more precise statement — what trips SlowDown is
the *rate*, not the object count: a multi-object design whose call rate stays
under S3's per-bucket knee (which the chain's serial link structure enforces
naturally) runs ~22 k calls without a single 503. The two contenders aren't
"one safe, one not" — they're two points on a genuine price/speed trade, and
the chain partly rehabilitates the multi-object approach the single-MPU README
had set aside.

## Repository layout

```
demo-s3-archiving/
├── README.md                              ← this file (benchmark overview)
├── contenders/rust/
│   ├── figment-engine/
│   │   ├── README.md                      ← single-MPU, price-optimal
│   │   └── src/…
│   └── figment-engine-chain/
│       ├── README.md                      ← copy-only chain, speed-optimal
│       └── src/…
└── templates/                             ← benchmark infra (CFN + Step Function)
```

## Shared foundations

Both contenders share the same generic machinery (the chain depends on the
`figment-engine` crate by path and reuses it):

- **ZIP64 STORED encoding** (`zip_format`): hand-rolled local-header,
  central-directory, and end-of-central-directory encoders. No off-the-shelf
  ZIP writer can emit an archive as independently-produced MPU parts, and
  STORED ZIP64 is simple enough to encode directly.
- **CRC32 from metadata** (`crc`): a copied body never reaches the Lambda, so
  its CRC32 can't be computed locally. The benchmark objects carry a stored
  full-object CRC32 in their S3 metadata, so a single `HEAD` per object
  supplies every header's CRC without touching a body. (Had they not, the
  strategy still works — S3 would compute the checksums server-side — just not
  for free.)
- **Pure engine + thin executor**: all correctness-critical layout lives in a
  pure, side-effect-free planner that returns a plan; the AWS executor is
  plumbing with no layout decisions. This split is what makes the layout
  testable — plans are validated by building real archives in memory and
  parsing them with a standard ZIP reader, with zero AWS calls.
