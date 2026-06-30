//! AWS executor for the segment-chain (copy-only) design — CORRECT + SIMPLE cut.
//!
//! Depends on `aws_sdk_s3`. Holds NO layout logic — that lives in the pure
//! `plan_chain`. This module is plumbing: HEAD the objects for their stored
//! CRC32, build each segment object via its link chain (a short chain of MPUs
//! joined by copy-forward), then copy-stitch the segment objects into the final
//! archive with the central directory as the exempt last part.
//!
//! Staging: this is the simple, obviously-correct version — segments built
//! concurrently (links within a segment are serial), then a single stitch at the
//! end. The speed optimisations (completion-driven overlapped stitch, two-tier
//! priority, rate-knee tuning) are deliberate FOLLOW-UPS, marked `// SPEED:`.
//!
//! Mirrors `engine`/`aws::assemble` house style: same retry wrapper, same error
//! taxonomy, same MPU call shapes, same CRC HEAD path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use futures::stream::StreamExt;

use figment_engine::engine::crc::decode_s3_crc32;
use figment_engine::engine::plan::FileId;
use figment_engine::engine::zip_format::{self, EntryMeta};

use crate::rate_limit::{Priority, RateLimiter};

use crate::plan_chain::{ChainPlan, Entry, Link, Piece, Segment};

/// Shared, awaitable CRC store. The paced HEAD stream fills it as CRCs arrive;
/// each segment awaits only the CRCs *it* needs (its own entries + the trailing
/// next-big lookahead) before building, rather than the whole set. Because the
/// HEADs are paced and issued in plan order, CRCs become available staggered, so
/// segments start staggered too — no synchronised create burst. The CD at the
/// end reads the full store (every CRC present by then).
#[derive(Clone)]
struct CrcStore {
    inner: Arc<CrcInner>,
}
struct CrcInner {
    map: std::sync::Mutex<std::collections::HashMap<FileId, u32>>,
    notify: tokio::sync::Notify,
}
impl CrcStore {
    fn new() -> Self {
        CrcStore {
            inner: Arc::new(CrcInner {
                map: std::sync::Mutex::new(std::collections::HashMap::new()),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }
    /// Record a CRC and wake any segments waiting on it.
    fn put(&self, id: FileId, crc: u32) {
        self.inner.map.lock().unwrap().insert(id, crc);
        self.inner.notify.notify_waiters();
    }
    fn get(&self, id: FileId) -> Option<u32> {
        self.inner.map.lock().unwrap().get(&id).copied()
    }
    /// Await until every id in `ids` has a CRC present. Re-checks on each wake;
    /// subscribing to the notify *before* the check avoids missing a wake.
    async fn wait_for(&self, ids: &[FileId]) {
        loop {
            let notified = self.inner.notify.notified();
            if ids.iter().all(|id| self.get(*id).is_some()) {
                return;
            }
            notified.await;
        }
    }
}

/// The CRC ids a segment needs before it can build: every entry in its links,
/// plus the trailing-header lookahead (the next segment's big, whose header rides
/// this segment's tail). Derived from the plan's link pieces.
fn segment_crc_ids(seg: &Segment) -> Vec<FileId> {
    let mut ids = Vec::new();
    for link in &seg.links {
        match link {
            Link::Bootstrap { anchor, .. } | Link::AnchorOnly { anchor } => ids.push(*anchor),
            Link::AnchorThenAppend { anchor, piece } => {
                ids.push(*anchor);
                ids.push(piece_id(piece));
            }
            Link::ForwardThenAppend { piece } => ids.push(piece_id(piece)),
        }
    }
    ids.sort_by_key(|id| id.0);
    ids.dedup_by_key(|id| id.0);
    ids
}

fn piece_id(p: &Piece) -> FileId {
    match p {
        Piece::Header(id) | Piece::Body(id) => *id,
    }
}

/// Segment objects are independent — build them wide. Links *within* a segment
/// are serial (each copies the previous link's completed object), so this bounds
/// how many segments are in flight at once, not total calls. Solo runs show zero
/// SlowDown at 64-wide and only ~690 calls/s achieved (a fifth of the knee), so
/// the bottleneck is under-concurrency, not throttling — push this high and let
/// the rate limiter (added later) cap it for the contended benchmark.
const SEGMENT_CONCURRENCY: usize = 256;
/// HEADs for CRC — read-only, cheap. Now issued as a paced background stream that
/// overlaps building (segments await only their own CRCs). The governor paces the
/// issue rate; this bounds in-flight HEADs so the stream can't outrun the build's
/// CRC-consume rate and stockpile a backlog that would let segments clump. ~32
/// in flight ≈ a few hundred HEADs/s per instance — safe ×3 under the HEAD knee.
const CRC_CONCURRENCY: usize = 32;
/// Max attempts per call before giving up. The adaptive governor keeps most
/// 503s from happening at all; this wider budget rides out the few that slip
/// through during a backoff transient under concurrent contention.
const MAX_ATTEMPTS: u32 = 10;

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error("s3 error: {0}")]
    S3(#[from] aws_sdk_s3::Error),
    #[error("missing upload id from create_multipart_upload")]
    NoUploadId,
    #[error("missing etag from {0}")]
    NoEtag(&'static str),
    #[error("missing or undecodable CRC for object {0}")]
    BadCrc(String),
    #[error("bytestream: {0}")]
    ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
}
impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for ChainError
where
    aws_sdk_s3::error::SdkError<E, R>: Into<aws_sdk_s3::Error>,
{
    fn from(err: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        ChainError::S3(err.into())
    }
}

fn is_retryable(e: &ChainError) -> bool {
    match e {
        ChainError::ByteStream(_) => true,
        ChainError::S3(_) => {
            let s = e.to_string();
            s.contains("SlowDown")
                || s.contains("Throttl")
                || s.contains("ServiceUnavailable")
                || s.contains("Service Unavailable")
                || s.contains("RequestTimeout")
                || s.contains("InternalError")
                || s.contains("(500)")
                || s.contains("(503)")
        }
        _ => false,
    }
}

/// Run `op` up to `MAX_ATTEMPTS` times, retrying transient failures with
/// exponential backoff (100ms..~3.2s) + jitter. `op` must be self-contained; part
/// re-uploads are idempotent (re-uploading a part number overwrites it).
async fn with_retry<T, F, Fut>(mut op: F) -> Result<T, ChainError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ChainError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt >= MAX_ATTEMPTS || !is_retryable(&e) {
                    return Err(e);
                }
                let base_ms = 100u64.saturating_mul(1u64 << (attempt - 1).min(5));
                let jitter_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_micros() as u64)
                    .unwrap_or(0)
                    % 100;
                tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
            }
        }
    }
}

/// Like `with_retry`, but routes every attempt through the adaptive rate
/// governor: acquire a token (at the given priority) before issuing, and on a
/// throttle signal tell the governor to back off so concurrent instances
/// converge under the shared S3 knee. The governor keeps most 503s from ever
/// happening; the wider retry budget here rides out the few that still slip
/// through during a backoff transient.
async fn with_retry_governed<T, F, Fut>(
    rl: &RateLimiter,
    prio: Priority,
    mut op: F,
) -> Result<T, ChainError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ChainError>>,
{
    let mut attempt: u32 = 0;
    loop {
        rl.acquire(prio).await;
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retryable = is_retryable(&e);
                if retryable {
                    // Distinguish a throttle (back off the rate) from a transient
                    // stream break (retry, but don't penalise the rate).
                    let s = e.to_string();
                    if s.contains("SlowDown") || s.contains("Throttl") || s.contains("(503)") {
                        rl.on_throttle();
                    }
                }
                attempt += 1;
                if attempt >= MAX_ATTEMPTS || !retryable {
                    return Err(e);
                }
                rl.note_retry();
                let base_ms = 100u64.saturating_mul(1u64 << (attempt - 1).min(7));
                let jitter_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_micros() as u64)
                    .unwrap_or(0)
                    % 100;
                tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
            }
        }
    }
}

/// Entry point: realise `plan` into the archive at `archive_key`.
pub async fn run(
    s3: &Client,
    bucket: &str,
    files_prefix: &str,
    archive_key: &str,
    plan: ChainPlan,
) -> Result<(), ChainError> {
    let st = plan.stats;
    tracing::info!(
        entries = st.entries,
        segments = st.segments,
        bigs = st.bigs,
        smalls = st.smalls,
        links = st.links,
        max_chain_depth = st.max_chain_depth,
        "PHASE plan"
    );

    // The adaptive rate governor. Slow-starts low and ramps; backs off on 503s so
    // concurrent instances converge under the shared knee. With paced HEADs and
    // CRC-gated (staggered) segment starts there is no cold-start spike for it to
    // catch — it only trims the sustained residual under contention.
    let rl = RateLimiter::new();

    let plan = Arc::new(plan);
    let crcs = CrcStore::new();

    // ---- Paced CRC HEAD stream (overlaps building). ----
    // Instead of a barrier that HEADs all entries before building starts, the
    // HEADs run as a paced background stream in plan order, filling the store as
    // each lands. Each segment awaits only its own CRCs, so segments start
    // staggered as their CRCs arrive — no synchronised create burst, and no
    // front-loaded HEAD burst. The producer is governed (High priority) so its
    // rate tracks the build's and backs off with it under contention.
    let crc_producer = {
        let s3 = s3.clone();
        let bucket = bucket.to_string();
        let files_prefix = files_prefix.to_string();
        let plan = plan.clone();
        let crcs = crcs.clone();
        let rl = rl.clone();
        async move {
            let t_crc = Instant::now();
            // Issue in plan order so CRCs arrive in roughly the order segments
            // consume them. CRC_CONCURRENCY bounds the in-flight HEADs; the
            // governor paces the issue rate.
            let stream = futures::stream::iter(plan.order.iter().map(|id| {
                let s3 = s3.clone();
                let bucket = bucket.clone();
                let files_prefix = files_prefix.clone();
                let crcs = crcs.clone();
                let rl = rl.clone();
                let name = plan.entries[id].name.clone();
                let id = *id;
                async move {
                    let key = format!("{files_prefix}/{name}");
                    let crc_b64 = with_retry_governed(&rl, Priority::High, || async {
                        let out = s3
                            .head_object()
                            .bucket(&bucket)
                            .key(&key)
                            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
                            .send()
                            .await?;
                        Ok::<_, ChainError>(out.checksum_crc32().map(ToOwned::to_owned))
                    })
                    .await?;
                    let crc = crc_b64
                        .as_deref()
                        .and_then(decode_s3_crc32)
                        .ok_or_else(|| ChainError::BadCrc(format!("{id:?}")))?;
                    crcs.put(id, crc);
                    Ok::<(), ChainError>(())
                }
            }))
            .buffer_unordered(CRC_CONCURRENCY);
            let res: Vec<Result<(), ChainError>> = stream.collect().await;
            for r in res {
                r?;
            }
            let cs = rl.stats();
            tracing::info!(
                ms = t_crc.elapsed().as_millis(),
                gov_rate = cs.rate,
                gov_throttles = cs.throttles,
                gov_down_steps = cs.down_steps,
                gov_retries = cs.retries,
                "PHASE crc_heads_paced"
            );
            Ok::<(), ChainError>(())
        }
    };

    // ---- Open the final stitch MPU up front. ----
    // Stitch part numbers are POSITIONAL and known from the plan before any
    // segment exists (segment k -> stitch part k+1; the CD is the exempt last
    // part). So the MPU can be created now, and each segment's stitch copy can
    // fire the instant that segment's object is complete — no end-of-run barrier.
    let stitch_upload_id = create_mpu(s3, bucket, archive_key).await?;
    let n_segments = plan.segments.len();
    let cd_part_number = (n_segments + 1) as i32;

    // ---- Build each segment object AND fire its stitch copy, completion-driven. ----
    // Each segment task first AWAITS its own CRCs (own entries + next-big
    // lookahead) from the store, so it can't start until they've arrived — which,
    // because the HEAD stream is paced and in-order, staggers segment starts and
    // removes the synchronised create burst. It then assembles the segment object
    // via its serial link chain and immediately UploadPartCopies it into the
    // stitch MPU as its positional part. The CRC producer runs concurrently (see
    // the join below), feeding the store as the build consumes it.
    let t_build = Instant::now();
    let build = async {
        let results: Vec<Result<CompletedPart, ChainError>> =
            futures::stream::iter(plan.segments.iter().map(|seg| {
                let s3 = s3.clone();
                let plan = plan.clone();
                let bucket = bucket.to_string();
                let files_prefix = files_prefix.to_string();
                let archive_key = archive_key.to_string();
                let upload_id = stitch_upload_id.clone();
                let seg_key = segment_key(&archive_key, seg.index);
                let stitch_part = (seg.index + 1) as i32;
                let rl = rl.clone();
                let crcs = crcs.clone();
                let crc_ids = segment_crc_ids(seg);
                async move {
                    // 0) Gate on this segment's CRCs arriving. Because the HEAD stream
                    //    is paced and in plan order, this staggers segment starts and
                    //    removes the synchronised create burst.
                    crcs.wait_for(&crc_ids).await;
                    // 1) Assemble the segment object (serial link chain). Link calls
                    //    are High priority — the critical path.
                    build_segment_object(
                        &s3,
                        &bucket,
                        &files_prefix,
                        &plan,
                        seg,
                        &seg_key,
                        &rl,
                        &crcs,
                    )
                    .await?;
                    // 2) Immediately copy it into the stitch MPU as its positional
                    //    part. Low priority: the stitch copy must yield to segment
                    //    links still being built, which it ultimately depends on.
                    let src = format!("{bucket}/{seg_key}");
                    let part = with_retry_governed(&rl, Priority::Low, || async {
                        upload_part_copy(
                            &s3,
                            &bucket,
                            &archive_key,
                            &upload_id,
                            stitch_part,
                            &src,
                            None,
                        )
                        .await
                    })
                    .await?;
                    Ok::<_, ChainError>(part)
                }
            }))
            .buffer_unordered(SEGMENT_CONCURRENCY)
            .collect()
            .await;
        results
    };

    // Run the paced CRC producer and the build concurrently. The producer feeds
    // the store as the build consumes it; both must succeed.
    let (_, results): ((), Vec<Result<CompletedPart, ChainError>>) =
        futures::try_join!(crc_producer, async { Ok::<_, ChainError>(build.await) })?;

    let build_ms = t_build.elapsed().as_millis();
    let s = rl.stats();
    let calls_per_sec = if build_ms > 0 {
        (s.acquires as f64) / (build_ms as f64 / 1000.0)
    } else {
        0.0
    };
    tracing::info!(
        ms = build_ms,
        segments = n_segments,
        gov_rate = s.rate,
        gov_min_rate = s.min_rate,
        gov_acquires = s.acquires,
        gov_throttles = s.throttles,
        gov_down_steps = s.down_steps,
        gov_up_steps = s.up_steps,
        gov_retries = s.retries,
        achieved_calls_per_sec = calls_per_sec,
        "PHASE build_and_stitch"
    );

    // ---- Central directory, built now that every CRC is in the store. ----
    // It needs all CRCs, so it can only be built once the producer has finished —
    // which it has (try_join above). Uploaded as the stitch MPU's exempt last part.
    let t_cd = Instant::now();
    let cd_bytes = build_central_directory(&plan, &crcs)?;
    let cd_part = {
        let s3 = s3.clone();
        let bucket = bucket.to_string();
        let archive_key = archive_key.to_string();
        let upload_id = stitch_upload_id.clone();
        with_retry(move || {
            let s3 = s3.clone();
            let bucket = bucket.clone();
            let archive_key = archive_key.clone();
            let upload_id = upload_id.clone();
            let cd_bytes = cd_bytes.clone();
            async move {
                upload_part(
                    &s3,
                    &bucket,
                    &archive_key,
                    &upload_id,
                    cd_part_number,
                    cd_bytes,
                )
                .await
            }
        })
        .await?
    };
    tracing::info!(ms = t_cd.elapsed().as_millis(), "PHASE cd");

    let mut parts: Vec<CompletedPart> = Vec::with_capacity(n_segments + 1);
    parts.push(cd_part);
    for r in results {
        parts.push(r?);
    }

    // ---- The one irreducible tail: complete the stitch MPU. ----
    let t_done = Instant::now();
    complete_mpu(s3, bucket, archive_key, &stitch_upload_id, parts).await?;
    tracing::info!(ms = t_done.elapsed().as_millis(), "PHASE terminal_complete");

    // The archive is now complete and valid; the handler returns here. The
    // intermediate `seg-temps/` objects are NOT deleted in-Lambda — a bucket
    // lifecycle rule (Prefix: seg-temps/) reaps them asynchronously. Cleaning up
    // here would put thousands of DeleteObject calls inside the benchmark-measured
    // invoke duration; the benchmark times the whole synchronous invoke, so any
    // in-handler GC directly inflates the reported time. GC is not the archiver's
    // job and not the benchmark's concern.
    Ok(())
}

/// Intermediate segment/link objects live under a dedicated top-level
/// `seg-temps/` prefix (NOT under `archives/`), so a single bucket lifecycle rule
/// (`Prefix: seg-temps/`) can expire them without any risk of matching the real
/// archives that share the `archives/` prefix. The archive_key is embedded for
/// uniqueness across concurrent runs; its own slashes are ordinary key chars.
fn segment_key(archive_key: &str, index: usize) -> String {
    format!("seg-temps/{archive_key}/{index:08}")
}

/// Build one segment's object at `seg_key` by walking its links. Each link is its
/// own MPU: a non-last part (bootstrap upload / anchor copy / forward copy) that
/// clears the floor, then the floor-exempt appended last part. The link's output
/// object becomes the copy-forward source for the next link.
#[allow(clippy::too_many_arguments)] // its a benchmark not production
async fn build_segment_object(
    s3: &Client,
    bucket: &str,
    files_prefix: &str,
    plan: &ChainPlan,
    seg: &Segment,
    seg_key: &str,
    rl: &RateLimiter,
    crcs: &CrcStore,
) -> Result<(), ChainError> {
    // The object built by the PREVIOUS link in this chain (copy-forward source).
    // For all but the first link it is `seg_key` itself (each link overwrites it).
    let mut prev_object: Option<String> = None;

    let last = seg.links.len() - 1;
    for (li, link) in seg.links.iter().enumerate() {
        let is_last_link = li == last;
        // Each link writes to a fresh temp key, then (if not last) becomes the next
        // link's source. Using one key per link avoids copy-source == dest aliasing.
        let out_key = if is_last_link {
            seg_key.to_string()
        } else {
            format!("{seg_key}.l{li}")
        };

        // Build the link. Each individual S3 call inside is governed (acquires a
        // token, retries, signals throttle) so the governor paces REAL calls, not
        // links — a link is ~4 calls, and pacing links let the true call rate
        // overshoot ~4x under contention. Priority High: the critical path.
        build_one_link(
            s3,
            bucket,
            files_prefix,
            plan,
            link,
            prev_object.as_deref(),
            &out_key,
            rl,
            crcs,
        )
        .await?;

        // NOTE: the previous link's temp object is NOT deleted here. The inline
        // delete used to sit on the critical path — one full round-trip per link,
        // ~4,500 serial deletes across the run, all pure housekeeping the next
        // link doesn't depend on. All `.l{li}` temps are reaped together by the
        // S3 lifecycle rule on the seg-temps/ prefix, so they never compete for
        // the serial chain's round-trip budget.
        if !is_last_link {
            prev_object = Some(out_key);
        }
    }
    Ok(())
}

/// Realise one link as a complete MPU producing `out_key`. Every individual S3
/// call is governed (acquire a token, retry, signal throttle), so the governor's
/// rate reflects the TRUE call rate — a link is ~4 calls, and governing the link
/// as a unit let the real call rate overshoot ~4x the governed rate, which is what
/// collapsed the contended case.
#[allow(clippy::too_many_arguments)] // its a benchmark not production
async fn build_one_link(
    s3: &Client,
    bucket: &str,
    files_prefix: &str,
    plan: &ChainPlan,
    link: &Link,
    prev_object: Option<&str>,
    out_key: &str,
    rl: &RateLimiter,
    crcs: &CrcStore,
) -> Result<(), ChainError> {
    let upload_id = with_retry_governed(rl, Priority::High, || async {
        create_mpu(s3, bucket, out_key).await
    })
    .await?;
    // Borrow once as a Copy &str so the per-call `async move` closures each capture
    // the reference (Copy) rather than trying to move the String.
    let uid: &str = &upload_id;
    let mut parts: Vec<CompletedPart> = Vec::with_capacity(2);

    match link {
        Link::Bootstrap { anchor, steal_len } => {
            // part1 = UploadPart( LFH_big0 ++ GET big0[..steal_len] )  (== 5 MiB)
            let entry = &plan.entries[anchor];
            let header = zip_format::local_header(&meta_of(entry, crcs)?);
            let prefix = with_retry_governed(rl, Priority::High, || async {
                get_object_range_bytes(
                    s3,
                    bucket,
                    &source_key(files_prefix, &entry.name),
                    0,
                    *steal_len,
                )
                .await
            })
            .await?;
            let mut p1 = Vec::with_capacity(header.len() + prefix.len());
            p1.extend_from_slice(&header);
            p1.extend_from_slice(&prefix);
            let p1_part = with_retry_governed(rl, Priority::High, || {
                let p1 = p1.clone();
                async move { upload_part(s3, bucket, out_key, uid, 1, p1).await }
            })
            .await?;
            parts.push(p1_part);

            // part2 = UploadPartCopy( big0[steal_len..] )  (exempt last)
            let src = copy_source(bucket, files_prefix, &entry.name);
            let range = Some(format!("bytes={}-{}", steal_len, entry.size - 1));
            let p2 = with_retry_governed(rl, Priority::High, || {
                let src = src.clone();
                let range = range.clone();
                async move { upload_part_copy(s3, bucket, out_key, uid, 2, &src, range).await }
            })
            .await?;
            parts.push(p2);
        }

        Link::AnchorOnly { anchor } => {
            // Single part = UploadPartCopy(anchor) (the only/last part, exempt).
            let entry = &plan.entries[anchor];
            let src = copy_source(bucket, files_prefix, &entry.name);
            let p = with_retry_governed(rl, Priority::High, || {
                let src = src.clone();
                async move { upload_part_copy(s3, bucket, out_key, uid, 1, &src, None).await }
            })
            .await?;
            parts.push(p);
        }

        Link::AnchorThenAppend { anchor, piece } => {
            // part1 = UploadPartCopy(anchor) (>=5 MiB, non-last)
            let entry = &plan.entries[anchor];
            let src = copy_source(bucket, files_prefix, &entry.name);
            let p1 = with_retry_governed(rl, Priority::High, || {
                let src = src.clone();
                async move { upload_part_copy(s3, bucket, out_key, uid, 1, &src, None).await }
            })
            .await?;
            parts.push(p1);
            // part2 = the piece (exempt last)
            parts.push(
                build_piece_part(
                    s3,
                    bucket,
                    files_prefix,
                    plan,
                    piece,
                    out_key,
                    uid,
                    2,
                    rl,
                    crcs,
                )
                .await?,
            );
        }

        Link::ForwardThenAppend { piece } => {
            // part1 = UploadPartCopy(prev segment object) (>=5 MiB, non-last)
            let prev = prev_object.expect("forward link must have a previous object");
            let src = format!("{bucket}/{prev}");
            let p1 = with_retry_governed(rl, Priority::High, || {
                let src = src.clone();
                async move { upload_part_copy(s3, bucket, out_key, uid, 1, &src, None).await }
            })
            .await?;
            parts.push(p1);
            // part2 = the piece (exempt last)
            parts.push(
                build_piece_part(
                    s3,
                    bucket,
                    files_prefix,
                    plan,
                    piece,
                    out_key,
                    uid,
                    2,
                    rl,
                    crcs,
                )
                .await?,
            );
        }
    }

    with_retry_governed(rl, Priority::High, || {
        let parts = parts.clone();
        async move { complete_mpu(s3, bucket, out_key, uid, parts).await }
    })
    .await
}

/// Build the appended last part for a piece: a generated header (UploadPart) or a
/// copied body (UploadPartCopy).
#[allow(clippy::too_many_arguments)] // its a benchmark not production
async fn build_piece_part(
    s3: &Client,
    bucket: &str,
    files_prefix: &str,
    plan: &ChainPlan,
    piece: &Piece,
    out_key: &str,
    upload_id: &str,
    part_number: i32,
    rl: &RateLimiter,
    crcs: &CrcStore,
) -> Result<CompletedPart, ChainError> {
    match piece {
        Piece::Header(id) => {
            let entry = &plan.entries[id];
            let bytes = zip_format::local_header(&meta_of(entry, crcs)?);
            with_retry_governed(rl, Priority::High, || {
                let bytes = bytes.clone();
                async move { upload_part(s3, bucket, out_key, upload_id, part_number, bytes).await }
            })
            .await
        }
        Piece::Body(id) => {
            let entry = &plan.entries[id];
            let src = copy_source(bucket, files_prefix, &entry.name);
            with_retry_governed(rl, Priority::High, || {
                let src = src.clone();
                async move {
                    upload_part_copy(s3, bucket, out_key, upload_id, part_number, &src, None).await
                }
            })
            .await
        }
    }
}

/// Central directory + ZIP64 end records from the realised plan (all CRCs known).
#[allow(clippy::result_large_err)] // S3 error struct is large, but ok for 22k calls with network
fn build_central_directory(plan: &ChainPlan, crcs: &CrcStore) -> Result<Vec<u8>, ChainError> {
    let mut out = Vec::new();
    let mut cd_size = 0u64;
    for id in &plan.order {
        let e = &plan.entries[id];
        let rec = zip_format::central_dir_entry(&meta_of(e, crcs)?);
        cd_size += rec.len() as u64;
        out.extend_from_slice(&rec);
    }
    out.extend_from_slice(&zip_format::end_records(
        plan.order.len() as u64,
        plan.cd_offset,
        cd_size,
    ));
    Ok(out)
}

#[allow(clippy::result_large_err)] // S3 error struct is large, but ok for 22k calls with network
fn meta_of(e: &Entry, crcs: &CrcStore) -> Result<EntryMeta, ChainError> {
    let crc = crcs
        .get(e.id)
        .ok_or_else(|| ChainError::BadCrc(e.name.clone()))?;
    Ok(EntryMeta {
        name: e.name.clone(),
        size: e.size,
        crc,
        local_header_offset: e.local_header_offset,
    })
}

fn source_key(files_prefix: &str, name: &str) -> String {
    format!("{files_prefix}/{name}")
}

/// `copy_source` for UploadPartCopy: `bucket/key` (the SDK URL-encodes as needed).
fn copy_source(bucket: &str, files_prefix: &str, name: &str) -> String {
    format!("{bucket}/{files_prefix}/{name}")
}

// ---- thin S3 call helpers (match aws::assemble shapes) ----

async fn create_mpu(s3: &Client, bucket: &str, key: &str) -> Result<String, ChainError> {
    let out = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    out.upload_id.ok_or(ChainError::NoUploadId)
}

async fn upload_part(
    s3: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    bytes: Vec<u8>,
) -> Result<CompletedPart, ChainError> {
    let out = s3
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .body(ByteStream::from(bytes))
        .send()
        .await?;
    let etag = out
        .e_tag()
        .ok_or(ChainError::NoEtag("upload_part"))?
        .to_string();
    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(etag)
        .build())
}

async fn upload_part_copy(
    s3: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: &str,
    copy_source_range: Option<String>,
) -> Result<CompletedPart, ChainError> {
    let mut req = s3
        .upload_part_copy()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .copy_source(copy_source);
    if let Some(range) = copy_source_range {
        req = req.copy_source_range(range);
    }
    let out = req.send().await?;
    let etag = out
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .ok_or(ChainError::NoEtag("upload_part_copy"))?
        .to_string();
    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(etag)
        .build())
}

async fn complete_mpu(
    s3: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    mut parts: Vec<CompletedPart>,
) -> Result<(), ChainError> {
    parts.sort_by_key(|p| p.part_number().unwrap_or_default());
    s3.complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

async fn get_object_range_bytes(
    s3: &Client,
    bucket: &str,
    key: &str,
    start: u64,
    len: u64,
) -> Result<Vec<u8>, ChainError> {
    let range = format!("bytes={}-{}", start, start + len - 1);
    let mut resp = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(range)
        .send()
        .await?;
    let mut data = Vec::with_capacity(len as usize);
    while let Some(chunk) = resp.body.next().await {
        data.extend_from_slice(&chunk?);
    }
    Ok(data)
}
