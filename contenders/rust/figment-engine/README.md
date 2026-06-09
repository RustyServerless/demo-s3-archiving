<!-- markdownlint-disable MD013 -->
# Rust contender — `figment-engine`

A single-Lambda implementation of the
[demo-s3-archiving](../../README.md) benchmark contract: given a
`{bucket_name, files_prefix, archive_key}` input, archive every object
under `s3://${bucket_name}/${files_prefix}/` into one flat **STORED**
ZIP at `s3://${bucket_name}/${archive_key}` — in a single invocation, on
an arm64 / `provided.al2023` Lambda with a 600 s timeout.

The benchmark bucket holds ~3 000 objects totalling ~14.65 GB. Ranking
is on `run_price_usd` = `memory_mb × duration` (Lambda cost only). The
question this contender answers: **how little of the archive can you
actually push through the Lambda's network interface?**

This README explains the approach and, more importantly, *why* — the
chain of reasoning that took the design from "stream everything" (the
reference's approach, ~211 s) to "move almost everything server-side"
(~116 s, ~31 % cheaper on the scored metric).

## TL;DR

| | Duration | `run_price_usd` | vs reference |
|---|---|---|---|
| **figment-engine (640 MB)** | **~116 s** | **~$0.00097** | **−31 %** |
| reference (jeremie-rodon, 512 MB) | ~211 s | ~$0.00141 | — |

Same correctness contract, no throttling, no OOM, transient-error
resilient. The win is almost entirely from **not moving bytes that don't
have to move.**

## The core insight: the ENI is the bottleneck

A Lambda archiving S3 objects has exactly one scarce resource — its
**elastic network interface (ENI)** bandwidth, which scales with the
configured memory. My rule of thumb for performance optimization?
first try not to do the work at all.

The **original** reference contender streams every object **through** the Lambda:
`GET` it down the ENI, then `UploadPart` it back up the ENI. For ~14.65 GB
that is ~14.65 GB down **plus** ~14.65 GB up — ~29 GB across the ENI.
That is the whole runtime.

```
  Reference: every byte makes a round trip through the Lambda ENI

      S3 (files/)                 Lambda                  S3 (archive)
          │                         │                           │
          │ ─────── GET ──────────► │ ──────  UploadPart ─────► │
          │      (down ENI)         │       (up ENI)            │
          │                         │                           │
          └── ~14.65 GB down ───────┴──── ~14.65 GB up ─────────┘
                         ≈ 29 GB total over the ENI
```

The entire design is the answer to one question:
**how do we avoid reading the data in lambda at all?**

I know from working on these kinds of problems before the Multi-part upload (MPU) is
super powerful, and you can achieve amazing outcomes if you just arrange your
strategy correctly. By asking S3 to copy an object into a multipart upload **server-side**, with
`UploadPartCopy` — the bytes go S3→S3 and never touch the Lambda ENI at
all! If we could assemble the archive mostly out of server-side copies,
the ENI would carry almost no traffic and the run would be bounded either by
the S3 call latency (unlikely) or by streaming the remaining data.

```
  This idea: copies stay server-side; only the unavoidable
  remainder crosses the ENI

      S3 (files/)                 Lambda                  S3 (archive)
          │                         │                           │
          │ ══════ UploadPartCopy (server-side, off-ENI) ═════► │   ← big files
          │                         │                           │
          │ ─────────GET──────────► │ ───────UploadPart───────► │   ← small files
          │                         │                           │     (must stream)
          └─ only ~7.5 GB over the ENI instead of ~29 GB ───────┘
```

`UploadPartCopy` doesn't get us the archive for free, though. Several
challenges stand between the idea and a valid ZIP — the next section
states them, the one after answers each in turn.

## The challenges

You can't just copy each object into its own MPU part. S3 multipart
upload has a hard rule: **every part except the last must be ≥ 5 MiB.**
The benchmark data splits almost exactly in half against that floor:

| | count | bytes |
|---|---|---|
| **"bigs"** (≥ 5 MiB) | 1 488 | 8.44 GB |
| **"smalls"** (< 5 MiB) | 1 512 | 6.21 GB |

A big clears the floor and can be its own copy part. Everything else
fights it. Five challenges stand in the way, roughly in the order we
hit them:

1. **Smalls.** 1 512 sub-floor objects can't be standalone copy parts —
   each would be a non-last part below the 5 MiB minimum, which S3
   rejects.
2. **The floor.** So smalls have to be combined into parts that each
   clear 5 MiB — and we have to do it without a buffer anywhere near big
   enough to hold the archive.
3. **Headers.** A ZIP entry is `[local header][body]`. The header has to
   sit immediately before its body in the archive byte stream, but a
   server-side copy moves only the body — there is nowhere for a
   Lambda-authored header to go.
4. **The CRC precondition.** A ZIP local header must carry the entry's
   CRC32. For a copied big we never see the body, so we can't compute it
   locally.
5. **No ZIP library can do this.** Off-the-shelf ZIP writers assemble a
   file (or an in-memory blob) sequentially; none can emit a valid ZIP
   *as a set of independently-produced MPU parts that S3 concatenates by
   number*.

## The solutions

Each challenge maps to a piece of the design. The **alternating
copy/stream layout** folds smalls into legal parts and gives the copied
bigs' headers somewhere to live (1, 3); the **"steal"** clears the floor
while keeping almost every big body server-side (2); the **central
directory placement** mops up the sub-floor leftovers (2); a single
**HEAD per object** supplies the CRCs cheaply (4); and a **hand-rolled
STORED ZIP64 writer** emits the format as MPU parts (5).

### One MPU of alternating Copy / Stream parts — solves 1 & 3

We build the whole archive as **one multipart upload**. Part numbers are
fixed by a pure planner up front; S3 reassembles parts in number order at
`CompleteMultipartUpload`, so parts have **no execution order** — they
can be produced in any order and simply slot into place.

Parts alternate between two kinds:

- **Copy part** — a big's body, moved server-side via `UploadPartCopy`.
  Off-ENI.
- **Stream part** — a batch of small files, each `[header][body]`,
  built in the Lambda and uploaded. On-ENI. Sized to clear the 5 MiB
  floor by batching enough smalls together.

That batching is the answer to **challenge 1**: smalls never become
standalone parts; they ride together in stream parts that clear the
floor as a group.

The trick that ties the two kinds together — and answers
**challenge 3** — is that **a big's local header rides on the tail of the
preceding stream part.** A stream part ends by appending the *next* big's
header bytes; the following copy part appends that big's body. In
archive-byte order they are adjacent, so the big reads back as one clean
`[header][body]` — even though the header came from the Lambda and the
body came from a server-side copy.

```
  Archive byte layout (= MPU parts concatenated in part-number order)

  part 1 (Stream)          part 2 (Copy)   part 3 (Stream)        part 4 (Copy)
  ┌───────────────────────┬───────────────┬──────────────────────┬────────────┐
  │ [hS][S] [hS][S] [hB1] │   B1 body     │ [hS][S] [hS][S] [hB2]│   B2 body  │ …
  └───────────────────────┴───────────────┴──────────────────────┴────────────┘
        smalls batch  ▲          ▲                smalls batch ▲        ▲
        (≥ 5 MiB)     │          │                (≥ 5 MiB)    │        │
            big-1 header ────────┘                    big-2 header ─────┘
            rides the tail; its body is the next (copy) part

  hS = small's local header   S = small's body
  hB = big's local header (Lambda-written)   B body = big's body (server-side copy)
```

The first part bootstraps entry 0's header; the central directory is the
last part (see below). Every non-last part is either a big copy (≥ floor
by definition) or a stream batch built up past the floor.

### The "steal" — solves 2

The first working version of this design left **half the bigs streamed,
not copied** (`folded_bigs: 732`). Why? Each stream batch must reach
5 MiB *with smalls*, and there are only ~1 512 smalls (~6.2 GB) to act as
"chaperones". A batch needs ~2 smalls to clear the floor, so the small
budget can only chaperone ~756 bigs over the floor as copies. The other
~732 bigs had no smalls left to ride with, so they were **folded** —
streamed whole through the ENI. ENI load was ~10 GB and the run took
~192 s.

The fix: **let a big chaperone itself.** When a stream batch has a small
in it but is still short of the floor, stream just the first *K* bytes of
the next big to bridge the gap — then copy the **remainder** of that big
server-side with a ranged `UploadPartCopy`.

```
  "Steal": a big donates a small prefix to lift the stream part over the
  floor; the rest of the body is still copied server-side.

  stream part                          copy part (ranged: bytes K..end)
  ┌──────────────────────────────────┬───────────────────────────────┐
  │ [hS][S]  [hB]  B[0..K)           │            B[K..end)          │
  └──────────────────────────────────┴───────────────────────────────┘
              ▲      ▲   └─ first K bytes streamed (on-ENI)
              │      └────── big's header (rides the tail)
       one small (must cross ENI anyway)

  Reads back as one contiguous entry:  [hB][ B[0..K) B[K..end) ] = [hB][B body]
  because the two parts are adjacent in part-number order.
```

`K` is tiny — just enough to bridge ~1 small's shortfall to 5 MiB (≈ 1 MiB),
versus folding the whole ~5.7 MiB big. Two constraints bound it:

- **Bridge the floor:** `K ≥ 5 MiB − (bytes already in the part)`.
- **Leave a valid copy part:** the remainder must itself be ≥ 5 MiB, so
  `K ≤ big_size − 5 MiB`.

A big with both satisfiable gets copied; only bigs hugging the floor
(too small to donate and still leave 5 MiB) fold. With this, the planner
copies **1 460 of 1 488 bigs** and folds just **2**. ENI load drops from
~10 GB to ~7.5 GB, and the run drops from ~192 s to ~145 s (both at
512 MB, where the steal was tuned; the shipping 640 MB config lands
~116 s — see below).

Two rules keep the steal honest:

- **A small is used first whenever one is available** — a small has to
  cross the ENI regardless, so we never waste one by stealing big bytes
  in its place. Steal covers only the residual gap after the small.
- **Largest-first ordering.** Bigs are copied largest-first and smalls
  paired smallest-first, so the handful of forced folds always land on
  the *smallest* bigs — the cheapest possible bytes to stream.

### The central directory rides in the last part — finishes 2

The ZIP central directory is written last. It cannot be its own trailing
MPU part safely: the part *before* it would then be a non-last part and
would have to clear the 5 MiB floor — which a small leftover-smalls part
won't. So the planner always emits the directory as the **final segment
of the final stream part**. That part is genuinely the last part in the
MPU (floor-exempt), so any sub-floor leftover smalls ride alongside the
directory and never form an undersized standalone part.

### A single HEAD per object — solves 4 (if we're lucky)

A ZIP local header must carry the entry's CRC32, and for a copied big we
never see the body — so we can't compute it ourselves. At this point the
plan depends on a bet: **we were hoping the objects already had a stored
CRC32.** Nothing guarantees it; it's a property of how the objects were
uploaded, and none of the existing contenders had noticed it was there to
exploit.

The bet paid off. The benchmark objects each carry a stored full-object
CRC32 in their S3 metadata (and a stored size that completes the header),
so a single `HEAD` returns everything a local header needs without ever
touching the body. We `HEAD` all ~3 000 objects (64-wide); the phase
costs ~2 s and is not the bottleneck.

Had the bet failed — objects with no stored checksum — the strategy would
*still work*, but not for free: we'd have to ask S3 to compute each
checksum server-side, which means S3 reads every body to hash it, adding
a server-side read per object and pushing the price up. So the stored
CRC32 isn't what makes copying *possible* — it's what makes it *cheap*,
and spotting that it was already there is what turned a good idea into a
cheap one.

### A hand-rolled STORED ZIP64 writer — solves 5

No off-the-shelf ZIP writer can emit an archive as independently-produced
MPU parts, so we hand-roll the format. That's tractable only because
STORED (no compression) ZIP64 is simple: a handful of fixed-layout record
encoders — local header, central-directory entry, end-of-central-directory
records — rather than a compression engine. ZIP64 records are emitted
because the archive exceeds 4 GiB. CRC32 and sizes are known up front
(from metadata), so they go directly in each local header — no data
descriptors needed. Filenames are ASCII (SHA-256 hex), so no UTF-8 flag
handling. All encoding lives in `src/engine/zip_format.rs`.

## Architecture: a pure engine + a thin AWS executor

All correctness-critical layout logic lives in a **pure, side-effect-free
engine** (`src/engine/`) that takes file metadata and returns a plan. The
AWS executor (`src/aws/assemble.rs`) is plumbing — it has no layout
decisions, only S3 calls. This split is what made the design testable:
the planner's output is validated by building a real archive in memory
and parsing it with a standard ZIP reader, all without touching AWS.

```
   list files          ┌──────────────────────────────────────────┐
   (name, size) ──────►│  engine::plan  (pure, total, TDD'd)      │
                       │                                          │
                       │  • partition bigs / smalls               │
                       │  • sort (bigs desc, smalls asc)          │
                       │  • walk: batch smalls → steal → copy     │
                       │  • compute ZIP offsets                   │
                       │  • emit ordered part list + entry table  │
                       └───────────────────┬──────────────────────┘
                                           │  SinglePlan
                                           ▼
                       ┌──────────────────────────────────────────┐
                       │  aws::assemble  (executor, no logic)     │
                       │                                          │
                       │  HEAD all objects → CRC32                │
                       │  create one MPU                          │
                       │  two pools realise parts → complete      │
                       └──────────────────────────────────────────┘
```

### Two independent pools

Copy parts and stream parts have opposite cost profiles, so they get
separate concurrency pools that never share slots:

- **Copy pool** (`COPY_CONCURRENCY = 128`): `UploadPartCopy` is
  server-side and latency-bound — each task just awaits, using no ENI.
  Run it wide and cheap.
- **Stream pool** (`STREAM_CONCURRENCY = 32`): `GET` + `UploadPart` is
  ENI-bandwidth-bound. Sized to saturate the pipe — enough concurrent
  transfers that each request's latency is hidden behind the others'
  throughput.

```
                       ┌──────── copy pool (128 wide) ───────┐
   part list  ────────►│  UploadPartCopy … (off-ENI)         │──┐
   (split by kind)     └─────────────────────────────────────┘  │
                                                                ├─► one MPU
                       ┌──────── stream pool (32 wide) ──────┐  │   (parts by
                       │  GET + UploadPart … (saturates ENI) │──┘    number)
                       └─────────────────────────────────────┘
```

If they shared one pool, idle copy-waits would occupy slots and starve
the ENI. Separated, the stream pool keeps the ENI pegged while the copy
pool churns through 1 486 copies in the background.

### Why one MPU is also the throttle-safe choice

An earlier design assembled the archive from many temporary S3 objects
and per-chain MPUs, then stitched them. When it worked, it was **much
faster** — dozens of independent objects uploading in true parallel got
runs close to **~60 s**, roughly half the single-MPU wall-clock, because
spreading writes across many objects multiplies aggregate throughput.
That is the real ceiling of this class of approach, and it's worth
knowing it exists.

But the same fan-out issued ~19 k writes (PUTs + per-chain MPU
operations) in a burst against the bucket and reliably tripped S3
`SlowDown` (503). Once 503s appear, the backoff and re-work erase the
throughput advantage and then some: the multi-object runs were either
slower than the single-MPU design or failed outright under contention
(the shared-bucket benchmark harness made this worse, since other
contenders were hammering the same bucket).

The single-MPU design trades that ~60 s ceiling for reliability: it
writes to exactly **one key** at a modest steady rate (~20 ops/s in
isolation), well under S3's per-prefix limits, so it doesn't throttle.
The lesson generalises: **for S3 assembly workloads, concentrating writes
onto one MPU trades a little theoretical parallelism for a lot of
reliability, and the reliability usually wins on wall-clock once
throttling is priced in.** You can chase the ~60 s ceiling, but only if
you also spread writes across many *prefixes* to dodge the per-prefix
request ceiling — which adds its own complexity (key-naming schemes,
cleanup of temporaries) that the single-MPU approach avoids entirely. A
bounded retry (exponential backoff + jitter, 5 attempts) wraps every S3
call to absorb transient stream breaks and the rare throttle, so a
dropped body re-fetches one part rather than failing the archive.

## Tunables (`src/aws/assemble.rs`)

```rust
const STREAM_CONCURRENCY: usize = 32;   // ENI-saturating
const COPY_CONCURRENCY:   usize = 128;  // server-side, run wide & cheap
const CRC_CONCURRENCY:    usize = 64;   // HEAD fan-out
const MAX_ATTEMPTS:       u32   = 5;    // bounded retry on transient errors
```

```rust
// src/engine/plan.rs
const PART_FLOOR:         u64 = 5 * 1024 * 1024;  // S3 MPU non-last-part minimum
const VIABILITY_MIN_TOTAL: u64 = 4 * PART_FLOOR;  // below this, fall back to streaming
```

## Memory configuration

Ships at **640 MB**. The pipeline is bandwidth-bound, not CPU-bound, so
more memory mostly buys ENI bandwidth — but because the ranking metric is
`memory_mb × duration`, faster-at-higher-memory only wins if the speed-up
beats the memory multiplier.

| Memory | Duration | `run_price_usd` | Notes |
|---|---|---|---|
| 512 MB | ~145 s | ~$0.00097 | price-optimal, but slower |
| **640 MB** | **~116 s** | **~$0.00097** | **shipping config — same price, ~30 s faster** |
| 768 MB | ~110 s | ~$0.00111 | faster but ~14 % pricier |

512 MB and 640 MB come out at essentially the **same price** (~$0.00097):
640's 1.25× memory is cancelled by its ~0.8× duration. So we ship 640 MB
for the faster wall-clock at no extra cost. Above 768 MB the speed-up no
longer pays for the memory. Peak observed memory is ~460 MB, well clear
of the 640 MB ceiling.

### It's the design, not the memory

To confirm the speed-up is the *design* and not just a bigger memory
allowance, we ran our contender and the reference at the **same** 640 MB:

| 640 MB | Duration | `run_price_usd` |
|---|---|---|
| **figment-engine** | **~116 s** | **~$0.00097** |
| reference (jeremie-rodon) | ~210 s | ~$0.00176 |

At identical memory the reference doesn't get faster — it's ENI-bound on
~29 GB of round-trips, so it stays ~210 s and merely costs more. Ours is
~1.8× faster and ~45 % cheaper on the same hardware. The win is the
server-side-copy strategy, not the memory tier.

## Plan shape on the benchmark data

```
PHASE plan  entries=3000  parts=2975
            copy_parts=1486  stream_parts=1489
            stolen_bigs=1460  folded_bigs=2
            bigs=1488  smalls=1512
```

1 460 of 1 488 bigs copied via steal, 26 copied whole, only 2 folded —
i.e. nearly all 8.44 GB of big bodies stay off the ENI. The ~7.5 GB that
does cross is ~6.2 GB of smalls plus ~1 MiB-per-big steal prefixes.

## File map

```
contenders/rust/figment-engine/
├── Cargo.toml
└── src/
    ├── main.rs                 Lambda entry; list files → plan → assemble / fallback
    ├── engine/
    │   ├── mod.rs
    │   ├── plan.rs             Pure planner: single-MPU alternating copy/stream + steal
    │   ├── zip_format.rs       ZIP64 STORED header / central-directory / EOCD encoders
    │   └── crc.rs              Decode S3's base64 stored CRC32
    └── aws/
        └── assemble.rs         Executor: HEAD CRCs, one MPU, two pools, bounded retry
```

## Design decisions (summary)

| Concern | Decision | Why |
|---|---|---|
| Move strategy | Server-side `UploadPartCopy` for bodies | The ENI is the only scarce resource; copies keep bytes off it |
| Small files | Batched stream parts, ≥ 5 MiB | Sub-floor objects can't be standalone copy parts |
| Big headers | Ride the tail of the preceding stream part | A copy moves only the body; the header must be Lambda-written and adjacent |
| Floor-bridging | "Steal" a small prefix from the next big | Lets each small chaperone one big instead of needing two smalls; copies ~all bigs |
| Fold ordering | Bigs largest-first, smalls smallest-first | Forced folds land on the smallest bigs — cheapest bytes to stream |
| Directory | Last segment of the last stream part | Keeps the only floor-exempt part last; leftover smalls ride with it |
| Structure | One MPU, parts in any execution order | Throttle-safe (one key) and lets copy/stream pools race freely |
| Concurrency | Two independent pools (copy 128, stream 32) | Opposite cost profiles; sharing slots would starve the ENI |
| Resilience | Bounded retry + jitter on every S3 call | Absorbs transient stream breaks and occasional SlowDown |
| CRC32 | Read from S3 metadata via HEAD | Copied bodies never reach the Lambda to be hashed; metadata makes it cheap |
| Correctness | Pure engine, validated with a real ZIP reader | Layout logic is testable with zero AWS calls (TDD) |
| Memory | 640 MB / arm64 | Bandwidth-bound; 640 MB matches 512's price but runs ~30 s faster |

## Verification

- **Engine tests** (`cargo test -p figment-engine --features zip_validate`):
  build archives straight from plans and parse them with a standard ZIP
  reader, asserting every non-last part ≥ floor, the directory rides in
  the last part, the steal never wastes an available small, and extracted
  content hashes match entry names.
- **Successful invocation**: the contender appears under `success` in the
  ranked Step Function output.
- **Control-Lambda check**: re-hashes ZIP entries (SHA-256) and validates
  entry-name == SHA-256(content).
- **Memory check**: CloudWatch `Max Memory Used` < 640 MB (observed peak
  ~460 MB).
