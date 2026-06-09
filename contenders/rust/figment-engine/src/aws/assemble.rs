//! AWS executor: takes a realised `SinglePlan` and produces the archive in S3.
//!
//! Depends on `aws_sdk_s3`. Holds NONE of the correctness-critical layout logic — that lives
//! in the pure `engine`. This module is plumbing for the SINGLE-MPU design: HEAD the objects
//! for their stored CRC32, open ONE archive multipart upload, then realise each planned part
//! directly into it — `Copy` parts via server-side UploadPartCopy (off-ENI), `Stream` parts by
//! GETting the batched smalls and uploading the assembled bytes — then append the central
//! directory as the final part and complete. No temp objects, no per-chain MPUs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::stream::StreamExt;

use crate::engine::crc::decode_s3_crc32;
use crate::engine::plan::{Entry, FileId, PartSpec, Segment, SinglePlan};
use crate::engine::zip_format::{self, EntryMeta};

/// Tunables. The two part kinds have opposite cost profiles, so they get independent pools:
/// streams are ENI-bandwidth-bound (size to saturate the pipe, covering request latency with
/// concurrent transfers); copies are server-side and latency-bound (run wide and cheap — no
/// 503s observed copying into a single MPU). They never share slots, so copy-waits can't starve
/// the ENI.
const STREAM_CONCURRENCY: usize = 32;
const COPY_CONCURRENCY: usize = 128;
const CRC_CONCURRENCY: usize = 64;
/// Max attempts per part/HEAD before giving up. Transient stream breaks and SlowDown/5xx are
/// retried with exponential backoff + jitter; a re-run is safe because re-uploading a part number
/// overwrites it and a HEAD is read-only.
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, thiserror::Error)]
pub enum AssembleError {
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
impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for AssembleError
where
	aws_sdk_s3::error::SdkError<E, R>: Into<aws_sdk_s3::Error>,
{
	fn from(err: aws_sdk_s3::error::SdkError<E, R>) -> Self {
		AssembleError::S3(err.into())
	}
}

/// Is this error worth retrying? Transient stream breaks (a body GET/upload that dropped
/// mid-transfer) and S3 throttling / 5xx are retryable; missing-etag, bad-CRC and no-upload-id are
/// deterministic logic errors that won't change on a re-run.
fn is_retryable(e: &AssembleError) -> bool {
	match e {
		AssembleError::ByteStream(_) => true,
		AssembleError::S3(_) => {
			let s = e.to_string();
			s.contains("SlowDown")
                || s.contains("Throttl") // Throttling / Throttled
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

/// Run `op` up to `MAX_ATTEMPTS` times, retrying only transient failures with exponential backoff
/// (100ms, 200ms, 400ms, … capped at ~3.2s) plus a little jitter to de-synchronise concurrent
/// retries within the invocation. `op` is re-invoked from scratch each attempt, so it must be
/// self-contained (rebuild the request, re-GET the bodies); part re-uploads are idempotent.
async fn with_retry<T, F, Fut>(mut op: F) -> Result<T, AssembleError>
where
	F: FnMut() -> Fut,
	Fut: std::future::Future<Output = Result<T, AssembleError>>,
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
				// Cheap jitter from the system clock's sub-ms component; no extra deps.
				let jitter_ms = (std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.map(|d| d.subsec_micros() as u64)
					.unwrap_or(0)) % 100;
				tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
			}
		}
	}
}

/// Entry point: realise `plan` into the archive at `archive_key`.
pub async fn assemble(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	archive_key: &str,
	mut plan: SinglePlan,
) -> Result<(), AssembleError> {
	// Log the planner's decisions: bigs copied (off-ENI) vs folded into stream parts (on-ENI,
	// because smalls ran out), and the resulting part shape.
	let st = plan.stats;
	tracing::info!(
		entries = st.entries,
		parts = st.parts,
		copy_parts = st.copy_parts,
		stream_parts = st.stream_parts,
		folded_bigs = st.folded_bigs,
		stolen_bigs = st.stolen_bigs,
		bigs = st.bigs,
		smalls = st.smalls,
		"PHASE plan"
	);

	// ---- Phase 1: fill CRCs (HEAD all entries; every object carries a stored CRC32). ----
	let t_crc = Instant::now();
	fill_crcs(s3, bucket, files_prefix, &mut plan).await?;
	tracing::info!(
		ms = t_crc.elapsed().as_millis(),
		entries = plan.order.len(),
		"PHASE crc_heads"
	);

	let plan = Arc::new(plan);

	// ---- Open the single archive MPU. ----
	let upload_id = create_mpu(s3, bucket, archive_key).await?;

	// ---- Realise every planned part into the one MPU via two INDEPENDENT pools. ----
	// Part numbers are fixed by the plan and resolved at CompleteMPU, so parts have no execution
	// order — copies and streams just race to finish. Splitting them means the stream pool keeps
	// the ENI saturated (concurrent transfers hide each GET's latency) while the copy pool churns
	// the bigs server-side in the background, neither starving the other.
	let mut copy_parts: Vec<(u32, FileId, u64)> = Vec::new();
	let mut stream_parts: Vec<(u32, Vec<Segment>)> = Vec::new();
	for part in &plan.parts {
		match part {
			PartSpec::Copy {
				part_number,
				id,
				copy_from,
			} => copy_parts.push((*part_number, *id, *copy_from)),
			PartSpec::Stream {
				part_number,
				segments,
			} => stream_parts.push((*part_number, segments.clone())),
		}
	}

	let t_parts = Instant::now();

	let copies =
		futures::stream::iter(copy_parts.into_iter().map(|(part_number, id, copy_from)| {
			let s3 = s3.clone();
			let plan = plan.clone();
			let bucket = bucket.to_string();
			let files_prefix = files_prefix.to_string();
			let archive_key = archive_key.to_string();
			let upload_id = upload_id.clone();
			async move {
				with_retry(|| async {
					let entry = &plan.entries[&id];
					let source = format!("{bucket}/{files_prefix}/{}", entry.name);
					let mut req = s3
						.upload_part_copy()
						.bucket(&bucket)
						.key(&archive_key)
						.upload_id(&upload_id)
						.part_number(part_number as i32)
						.copy_source(source);
					// When a prefix was streamed, copy only the remainder [copy_from, size-1].
					if copy_from > 0 {
						req = req.copy_source_range(format!(
							"bytes={}-{}",
							copy_from,
							entry.size - 1
						));
					}
					let out = req.send().await?;
					let etag = out
						.copy_part_result()
						.and_then(|r| r.e_tag())
						.ok_or(AssembleError::NoEtag("upload_part_copy"))?
						.to_string();
					Ok::<_, AssembleError>(
						CompletedPart::builder()
							.part_number(part_number as i32)
							.e_tag(etag)
							.build(),
					)
				})
				.await
			}
		}))
		.buffer_unordered(COPY_CONCURRENCY);

	let streams = futures::stream::iter(stream_parts.into_iter().map(|(part_number, segments)| {
		let s3 = s3.clone();
		let plan = plan.clone();
		let bucket = bucket.to_string();
		let files_prefix = files_prefix.to_string();
		let archive_key = archive_key.to_string();
		let upload_id = upload_id.clone();
		async move {
			with_retry(|| async {
				let bytes =
					build_stream_part_bytes(&s3, &bucket, &files_prefix, &plan, &segments).await?;
				let out = s3
					.upload_part()
					.bucket(&bucket)
					.key(&archive_key)
					.upload_id(&upload_id)
					.part_number(part_number as i32)
					.body(ByteStream::from(bytes))
					.send()
					.await?;
				let etag = out
					.e_tag()
					.ok_or(AssembleError::NoEtag("upload_part"))?
					.to_string();
				Ok::<_, AssembleError>(
					CompletedPart::builder()
						.part_number(part_number as i32)
						.e_tag(etag)
						.build(),
				)
			})
			.await
		}
	}))
	.buffer_unordered(STREAM_CONCURRENCY);

	// Drain both pools concurrently into one collection. (Box::pin: buffer_unordered adapters
	// aren't Unpin, which select requires.) The central directory is NOT a separate part — the
	// planner places it as the CentralDirectory segment in the final Stream part, so it is
	// realised by the stream pool like any other part (and rides in the genuine last MPU part).
	let mut merged = futures::stream::select(Box::pin(copies), Box::pin(streams));
	let mut completed: Vec<CompletedPart> = Vec::with_capacity(plan.parts.len());
	while let Some(res) = merged.next().await {
		completed.push(res?);
	}
	tracing::info!(
		ms = t_parts.elapsed().as_millis(),
		parts = plan.parts.len(),
		"PHASE parts"
	);

	// ---- Complete the MPU (parts must be ascending by number). ----
	completed.sort_by_key(|p| p.part_number().unwrap_or_default());
	let t_done = Instant::now();
	s3.complete_multipart_upload()
		.bucket(bucket)
		.key(archive_key)
		.upload_id(&upload_id)
		.multipart_upload(
			CompletedMultipartUpload::builder()
				.set_parts(Some(completed))
				.build(),
		)
		.send()
		.await?;
	tracing::info!(ms = t_done.elapsed().as_millis(), "PHASE terminal_complete");

	Ok(())
}

/// Assemble one Stream part's bytes: a streamed file contributes [local header][body] (GET the
/// body); a copied-file header contributes just [local header] (the trailing-header handoff for
/// the big copied in the following part); a streamed big-prefix contributes the big's first `len`
/// body bytes (ranged GET, no header — its header is the preceding CopiedFileHeader), which the
/// following ranged Copy continues from `len` to the end.
async fn build_stream_part_bytes(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &SinglePlan,
	segments: &[Segment],
) -> Result<Vec<u8>, AssembleError> {
	let mut buf: Vec<u8> = Vec::new();
	for seg in segments {
		match seg {
			Segment::StreamedFile { id } => {
				let entry = &plan.entries[id];
				let key = format!("{files_prefix}/{}", entry.name);
				let body = get_object_bytes(s3, bucket, &key, entry.size as usize).await?;
				let crc = entry
					.crc
					.ok_or_else(|| AssembleError::BadCrc(entry.name.clone()))?;
				let meta = entry_meta(entry, crc);
				buf.extend_from_slice(&zip_format::local_header(&meta));
				buf.extend_from_slice(&body);
			}
			Segment::CopiedFileHeader { id } => {
				let entry = &plan.entries[id];
				let crc = entry
					.crc
					.ok_or_else(|| AssembleError::BadCrc(entry.name.clone()))?;
				let meta = entry_meta(entry, crc);
				buf.extend_from_slice(&zip_format::local_header(&meta));
			}
			Segment::StreamedBigPrefix { id, len } => {
				let entry = &plan.entries[id];
				let key = format!("{files_prefix}/{}", entry.name);
				let prefix = get_object_range_bytes(s3, bucket, &key, 0, *len).await?;
				buf.extend_from_slice(&prefix);
			}
			Segment::CentralDirectory => {
				// Always the last segment of the last part: append the directory + end records,
				// so the directory rides inside the final MPU part (the genuine, floor-exempt last
				// part) rather than as a separate trailing part.
				buf.extend_from_slice(&build_central_directory(plan)?);
			}
		}
	}
	Ok(buf)
}

/// Central directory + ZIP64 end records, built from the realised plan (all CRCs known).
fn build_central_directory(plan: &SinglePlan) -> Result<Vec<u8>, AssembleError> {
	let mut cd_offset = 0u64;
	for id in &plan.order {
		let e = &plan.entries[id];
		cd_offset += zip_format::entry_total_len(&e.name, e.size);
	}
	let mut out = Vec::new();
	let mut cd_size = 0u64;
	for id in &plan.order {
		let e = &plan.entries[id];
		let crc = e.crc.ok_or_else(|| AssembleError::BadCrc(e.name.clone()))?;
		let meta = entry_meta(e, crc);
		let rec = zip_format::central_dir_entry(&meta);
		cd_size += rec.len() as u64;
		out.extend_from_slice(&rec);
	}
	out.extend_from_slice(&zip_format::end_records(
		plan.order.len() as u64,
		cd_offset,
		cd_size,
	));
	Ok(out)
}

fn entry_meta(e: &Entry, crc: u32) -> EntryMeta {
	EntryMeta {
		name: e.name.clone(),
		size: e.size,
		crc,
		local_header_offset: e.local_header_offset,
	}
}

/// HEAD every entry to fetch its stored full-object CRC32 (all objects carry one).
async fn fill_crcs(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &mut SinglePlan,
) -> Result<(), AssembleError> {
	let jobs: Vec<(FileId, String)> = plan
		.order
		.iter()
		.map(|id| {
			let name = plan
				.entries
				.get(id)
				.expect("ordered id in entries")
				.name
				.clone();
			(*id, format!("{files_prefix}/{}", name))
		})
		.collect();

	let results: Vec<Result<(FileId, Option<String>), AssembleError>> =
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
					let crc_b64 = out.checksum_crc32().map(ToOwned::to_owned);
					Ok::<_, AssembleError>((id, crc_b64))
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
			.ok_or_else(|| AssembleError::BadCrc(format!("{id:?}")))?;
		if let Some(e) = plan.entries.get_mut(&id) {
			e.crc = Some(crc);
		}
	}
	Ok(())
}

async fn create_mpu(s3: &Client, bucket: &str, key: &str) -> Result<String, AssembleError> {
	let out = s3
		.create_multipart_upload()
		.bucket(bucket)
		.key(key)
		.send()
		.await?;
	out.upload_id()
		.map(ToOwned::to_owned)
		.ok_or(AssembleError::NoUploadId)
}

async fn get_object_bytes(
	s3: &Client,
	bucket: &str,
	key: &str,
	expected: usize,
) -> Result<Vec<u8>, AssembleError> {
	let mut resp = s3.get_object().bucket(bucket).key(key).send().await?;
	let mut data = Vec::with_capacity(expected);
	while let Some(chunk) = resp.body.next().await {
		data.extend_from_slice(&chunk?);
	}
	Ok(data)
}

/// GET the first `len` bytes of an object (range `[start, start+len-1]`), for a stolen big-prefix.
async fn get_object_range_bytes(
	s3: &Client,
	bucket: &str,
	key: &str,
	start: u64,
	len: u64,
) -> Result<Vec<u8>, AssembleError> {
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
