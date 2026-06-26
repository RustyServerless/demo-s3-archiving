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

use crate::plan_chain::{ChainPlan, Entry, Link, Piece, Segment};

/// Segment objects are independent — build them wide. Links *within* a segment
/// are serial (each copies the previous link's completed object), so this bounds
/// how many segments are in flight at once, not total calls. Solo runs show zero
/// SlowDown at 64-wide and only ~690 calls/s achieved (a fifth of the knee), so
/// the bottleneck is under-concurrency, not throttling — push this high and let
/// the rate limiter (added later) cap it for the contended benchmark.
const SEGMENT_CONCURRENCY: usize = 256;
/// HEADs for CRC — read-only, cheap, run wide (separate ~5,500/s GET/HEAD budget).
const CRC_CONCURRENCY: usize = 256;
/// Stitch copy-parts — server-side copies into one MPU, latency-bound.
const STITCH_CONCURRENCY: usize = 128;
/// Max attempts per call before giving up (transient breaks + SlowDown/5xx).
const MAX_ATTEMPTS: u32 = 5;

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
					.unwrap_or(0) % 100;
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
	mut plan: ChainPlan,
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

	// ---- Phase 1: CRCs. Every body is copied, so every entry needs a HEAD. ----
	let t_crc = Instant::now();
	fill_crcs(s3, bucket, files_prefix, &mut plan).await?;
	tracing::info!(
		ms = t_crc.elapsed().as_millis(),
		entries = plan.order.len(),
		"PHASE crc_heads"
	);

	let plan = Arc::new(plan);

	// ---- Open the final stitch MPU up front. ----
	// Stitch part numbers are POSITIONAL and known from the plan before any
	// segment exists (segment k -> stitch part k+1; the CD is the exempt last
	// part). So the MPU can be created now, and each segment's stitch copy can
	// fire the instant that segment's object is complete — no end-of-run barrier.
	let stitch_upload_id = create_mpu(s3, bucket, archive_key).await?;
	let n_segments = plan.segments.len();

	// ---- Build the central directory eagerly. ----
	// Inputs are ready: offsets at plan time, CRCs after fill_crcs. In the
	// copy-only design there is no body IO to contend with, so nothing forces the
	// CD to wait for the stitch (that deferral was a streaming-design habit).
	// Build it now and upload it as the exempt last part immediately.
	let t_cd = Instant::now();
	let cd_bytes = build_central_directory(&plan)?;
	let cd_part_number = (n_segments + 1) as i32;
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
	tracing::info!(ms = t_cd.elapsed().as_millis(), "PHASE cd_eager");

	// ---- Build each segment object AND fire its stitch copy, completion-driven. ----
	// Each segment task: build the segment object via its link chain, then
	// immediately UploadPartCopy it as stitch part (index+1). The stitch copies
	// thus overlap segment-building entirely — each fires the moment its own
	// segment finishes, riding the same pool, rather than waiting for all
	// segments. (SPEED: a future two-tier rate limiter would prioritise segment
	// links over stitch copies under one global cap; here they share the pool.)
	let t_build = Instant::now();
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
			async move {
				// 1) Assemble the segment object (serial link chain).
				build_segment_object(&s3, &bucket, &files_prefix, &plan, seg, &seg_key).await?;
				// 2) Immediately copy it into the stitch MPU as its positional part.
				let src = format!("{bucket}/{seg_key}");
				let part = with_retry(|| async {
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

	let mut parts: Vec<CompletedPart> = Vec::with_capacity(n_segments + 1);
	parts.push(cd_part);
	for r in results {
		parts.push(r?);
	}
	tracing::info!(
		ms = t_build.elapsed().as_millis(),
		segments = n_segments,
		"PHASE build_and_stitch"
	);

	// ---- The one irreducible tail: complete the stitch MPU. ----
	let t_done = Instant::now();
	complete_mpu(s3, bucket, archive_key, &stitch_upload_id, parts).await?;
	tracing::info!(ms = t_done.elapsed().as_millis(), "PHASE terminal_complete");

	// ---- Best-effort cleanup of the intermediate segment objects. ----
	// SPEED: deferred to after the archive is complete so these DeleteObject
	// calls never compete for call budget with the work that must succeed.
	cleanup(s3, bucket, n_segments, archive_key).await;

	Ok(())
}

fn segment_key(archive_key: &str, index: usize) -> String {
	format!("{archive_key}.seg/{index:08}")
}

/// Build one segment's object at `seg_key` by walking its links. Each link is its
/// own MPU: a non-last part (bootstrap upload / anchor copy / forward copy) that
/// clears the floor, then the floor-exempt appended last part. The link's output
/// object becomes the copy-forward source for the next link.
async fn build_segment_object(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &ChainPlan,
	seg: &Segment,
	seg_key: &str,
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

		with_retry(|| async {
			build_one_link(
				s3,
				bucket,
				files_prefix,
				plan,
				link,
				prev_object.as_deref(),
				&out_key,
			)
			.await
		})
		.await?;

		// Clean up the previous link's temp object (best-effort).
		if let Some(prev) = prev_object.take() {
			let _ = s3.delete_object().bucket(bucket).key(&prev).send().await;
		}
		if !is_last_link {
			prev_object = Some(out_key);
		}
	}
	Ok(())
}

/// Realise one link as a complete MPU producing `out_key`.
async fn build_one_link(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &ChainPlan,
	link: &Link,
	prev_object: Option<&str>,
	out_key: &str,
) -> Result<(), ChainError> {
	let upload_id = create_mpu(s3, bucket, out_key).await?;
	let mut parts: Vec<CompletedPart> = Vec::with_capacity(2);

	match link {
		Link::Bootstrap { anchor, steal_len } => {
			// part1 = UploadPart( LFH_big0 ++ GET big0[..steal_len] )  (== 5 MiB)
			let entry = &plan.entries[anchor];
			let header = zip_format::local_header(&meta_of(entry)?);
			let prefix = get_object_range_bytes(
				s3,
				bucket,
				&source_key(files_prefix, &entry.name),
				0,
				*steal_len,
			)
			.await?;
			let mut p1 = Vec::with_capacity(header.len() + prefix.len());
			p1.extend_from_slice(&header);
			p1.extend_from_slice(&prefix);
			parts.push(upload_part(s3, bucket, out_key, &upload_id, 1, p1).await?);

			// part2 = UploadPartCopy( big0[steal_len..] )  (exempt last)
			let src = copy_source(bucket, files_prefix, &entry.name);
			let range = Some(format!("bytes={}-{}", steal_len, entry.size - 1));
			parts.push(upload_part_copy(s3, bucket, out_key, &upload_id, 2, &src, range).await?);
		}

		Link::AnchorOnly { anchor } => {
			// Single part = UploadPartCopy(anchor) (the only/last part, exempt).
			let entry = &plan.entries[anchor];
			let src = copy_source(bucket, files_prefix, &entry.name);
			parts.push(upload_part_copy(s3, bucket, out_key, &upload_id, 1, &src, None).await?);
		}

		Link::AnchorThenAppend { anchor, piece } => {
			// part1 = UploadPartCopy(anchor) (>=5 MiB, non-last)
			let entry = &plan.entries[anchor];
			let src = copy_source(bucket, files_prefix, &entry.name);
			parts.push(upload_part_copy(s3, bucket, out_key, &upload_id, 1, &src, None).await?);
			// part2 = the piece (exempt last)
			parts.push(
				build_piece_part(
					s3,
					bucket,
					files_prefix,
					plan,
					piece,
					out_key,
					&upload_id,
					2,
				)
				.await?,
			);
		}

		Link::ForwardThenAppend { piece } => {
			// part1 = UploadPartCopy(prev segment object) (>=5 MiB, non-last)
			let prev = prev_object.expect("forward link must have a previous object");
			let src = format!("{bucket}/{prev}");
			parts.push(upload_part_copy(s3, bucket, out_key, &upload_id, 1, &src, None).await?);
			// part2 = the piece (exempt last)
			parts.push(
				build_piece_part(
					s3,
					bucket,
					files_prefix,
					plan,
					piece,
					out_key,
					&upload_id,
					2,
				)
				.await?,
			);
		}
	}

	complete_mpu(s3, bucket, out_key, &upload_id, parts).await
}

/// Build the appended last part for a piece: a generated header (UploadPart) or a
/// copied body (UploadPartCopy).
async fn build_piece_part(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &ChainPlan,
	piece: &Piece,
	out_key: &str,
	upload_id: &str,
	part_number: i32,
) -> Result<CompletedPart, ChainError> {
	match piece {
		Piece::Header(id) => {
			let entry = &plan.entries[id];
			let bytes = zip_format::local_header(&meta_of(entry)?);
			upload_part(s3, bucket, out_key, upload_id, part_number, bytes).await
		}
		Piece::Body(id) => {
			let entry = &plan.entries[id];
			let src = copy_source(bucket, files_prefix, &entry.name);
			upload_part_copy(s3, bucket, out_key, upload_id, part_number, &src, None).await
		}
	}
}

/// Central directory + ZIP64 end records from the realised plan (all CRCs known).
fn build_central_directory(plan: &ChainPlan) -> Result<Vec<u8>, ChainError> {
	let mut out = Vec::new();
	let mut cd_size = 0u64;
	for id in &plan.order {
		let e = &plan.entries[id];
		let rec = zip_format::central_dir_entry(&meta_of(e)?);
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

fn meta_of(e: &Entry) -> Result<EntryMeta, ChainError> {
	let crc = e.crc.ok_or_else(|| ChainError::BadCrc(e.name.clone()))?;
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
	out.upload_id()
		.map(ToOwned::to_owned)
		.ok_or(ChainError::NoUploadId)
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

/// HEAD every entry for its stored CRC32 (all objects carry one).
async fn fill_crcs(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &mut ChainPlan,
) -> Result<(), ChainError> {
	let jobs: Vec<(FileId, String)> = plan
		.order
		.iter()
		.map(|id| {
			let name = plan.entries[id].name.clone();
			(*id, format!("{files_prefix}/{name}"))
		})
		.collect();

	let results: Vec<Result<(FileId, Option<String>), ChainError>> =
		futures::stream::iter(jobs.into_iter().map(|(id, key)| {
			let s3 = s3.clone();
			let bucket = bucket.to_string();
			async move {
				with_retry(|| async {
					let out = s3
						.head_object()
						.bucket(&bucket)
						.key(&key)
						.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
						.send()
						.await?;
					Ok::<_, ChainError>((id, out.checksum_crc32().map(ToOwned::to_owned)))
				})
				.await
			}
		}))
		.buffer_unordered(CRC_CONCURRENCY)
		.collect()
		.await;

	for res in results {
		let (id, crc_b64) = res?;
		let crc = crc_b64
			.as_deref()
			.and_then(decode_s3_crc32)
			.ok_or_else(|| ChainError::BadCrc(format!("{id:?}")))?;
		if let Some(e) = plan.entries.get_mut(&id) {
			e.crc = Some(crc);
		}
	}
	Ok(())
}

/// Best-effort delete of the intermediate segment objects after the stitch.
/// Deferred until the archive is complete so these DeleteObject calls never
/// compete for call budget with the assembly that must succeed.
async fn cleanup(s3: &Client, bucket: &str, n_segments: usize, archive_key: &str) {
	futures::stream::iter((0..n_segments).map(|i| {
		let s3 = s3.clone();
		let bucket = bucket.to_string();
		let key = segment_key(archive_key, i);
		async move {
			let _ = s3.delete_object().bucket(&bucket).key(&key).send().await;
		}
	}))
	.buffer_unordered(STITCH_CONCURRENCY)
	.collect::<Vec<()>>()
	.await;
}
