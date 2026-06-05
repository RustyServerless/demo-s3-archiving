//! ============================================================================
//! STATUS — read before trusting this module.
//!
//!   [ ] SDK method/field names match your pinned aws-sdk-s3 version. Used here:
//!       create_multipart_upload().upload_id()
//!       upload_part().body(ByteStream).part_number().e_tag()
//!       upload_part_copy().copy_source("bucket/key").part_number()
//!           .copy_part_result().e_tag()
//!       complete_multipart_upload().multipart_upload(CompletedMultipartUpload)
//!       head_object().checksum_mode(ChecksumMode::Enabled).checksum_crc32()
//!       get_object().body.next()
//!   [ ] `futures` crate is available (FuturesUnordered, StreamExt). Add if needed.
//!
//! KNOWN FIRST-CUT SIMPLIFICATION (correct, not yet maximally fast):
//!   Within a single chain, parts run sequentially (copy then stream), not concurrently.
//!   Chains run concurrently (CONTROL_CONCURRENCY) and stream parts are globally bounded
//!   (STREAM_CONCURRENCY), so the ENI is still driven by many concurrent chains. The
//!   pure event-driven per-part two-pool refinement can come AFTER an end-to-end pass.
//!
//! TEMP OBJECTS: chain objects are written under archives/.tmp-chains/ (only place the
//! contender role allows PutObject). They are harmless to validation (control Lambda
//! reads one archive key) and cleaned by the benching Step Function between runs (it has
//! DeleteObject on archives/*). We never need DeleteObject ourselves.
//! ============================================================================

//! AWS executor: takes a realised `Plan` and produces the archive in S3.
//!
//! Depends on `aws_sdk_s3`. Holds NONE of the correctness-critical layout logic — that
//! lives in the pure `engine`. This module is plumbing: fetch CRCs, run the two-pool
//! flow (stream parts on-ENI, copy parts off-ENI), complete each chain MPU, feed
//! completed chains into the final-merge MPU as-they-finish, then one terminal complete.
//!
//! Concurrency model (deliberately simple, no shared mutable buffer, no lifetimes):
//! - each unit of work owns its inputs (a Vec<u8> for a stream part, a source key for a
//!   copy part) and returns an owned result;
//! - two bounded pools by part-kind (streaming = ENI-bound, control = off-ENI);
//! - bookkeeping is plain maps/counters updated in one place as futures complete.

use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::stream::StreamExt;
use tokio::sync::Semaphore;

use crate::engine::crc::decode_s3_crc32;
use crate::engine::plan::{Chain, Entry, FileId, PartSpec, Plan, Segment};
use crate::engine::zip_format::{self, EntryMeta};

/// Tunables. Stream concurrency saturates the ENI; control concurrency bounds off-ENI calls.
const STREAM_CONCURRENCY: usize = 48;
const CHAIN_CONCURRENCY: usize = 32;
const CRC_CONCURRENCY: usize = 128;

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

/// Entry point: realise `plan` into the archive at `archive_key`.
pub async fn assemble(
	s3: &Client,
	bucket: &str,
	archive_key: &str,
	files_prefix: &str,
	mut plan: Plan,
) -> Result<(), AssembleError> {
	// ---- Phase 1: fetch CRCs for copyable entries (HEAD + checksum-mode), await all ----
	// Streamable entries' CRCs are computed from their bodies during streaming.
	fill_crcs(s3, bucket, files_prefix, &mut plan).await?;
	let plan = Arc::new(plan);

	// ---- Open the final-merge (archive) MPU up front so copies can flow in as-you-go ----
	let archive_upload_id = create_mpu(s3, bucket, archive_key).await?;

	// ---- Build per-chain MPUs and run their parts; complete each; feed final merge ----
	// For clarity this first cut runs chains with bounded concurrency and, on each chain's
	// completion, issues the final-merge UploadPartCopy for that chain's object.
	//
	// Intermediate chain objects are written under a temp prefix so they can be copied
	// server-side into the archive and then abandoned (lifecycle / explicit delete).
	// Collect final-merge completed parts (chain copies + the directory part).
	let mut final_parts: Vec<CompletedPart> = Vec::new();

	let archive_basename_s = archive_basename(archive_key);

	// The FIRST chain (index 0) streams ~all the small-file bytes — the ~90s ENI-bound long
	// pole. Run it as its OWN task with its OWN stream budget so it streams flat-out and does
	// NOT consume a slot in the normal-chain pool. The normal chains (mostly off-ENI copies +
	// one tiny GET each) run concurrently in a bounded pool; total wall-clock becomes
	// max(first chain, normal chains) rather than them contending for the same slots.
	let first_stream_sem = Arc::new(Semaphore::new(STREAM_CONCURRENCY));
	let first_handle = {
		let s3 = s3.clone();
		let plan = plan.clone();
		let bucket = bucket.to_string();
		let files_prefix = files_prefix.to_string();
		let archive_key = archive_key.to_string();
		let archive_upload_id = archive_upload_id.clone();
		let chain = plan.chains[0].clone();
		let chain_key = format!("archives/.tmp-chains/{}-0", archive_basename_s);
		tokio::spawn(async move {
			process_chain(
				&s3,
				&bucket,
				&files_prefix,
				&chain_key,
				&archive_key,
				&archive_upload_id,
				&plan,
				&chain,
				&first_stream_sem,
			)
			.await
		})
	};

	// Normal chains (1..) in a bounded pool with their own stream budget.
	let normal_stream_sem = Arc::new(Semaphore::new(STREAM_CONCURRENCY));
	let chain_jobs = futures::stream::iter(plan.chains.iter().enumerate().skip(1).map(
		|(chain_idx, chain)| {
			let s3 = s3.clone();
			let plan = plan.clone();
			let bucket = bucket.to_string();
			let files_prefix = files_prefix.to_string();
			let archive_key = archive_key.to_string();
			let archive_upload_id = archive_upload_id.clone();
			let stream_sem = normal_stream_sem.clone();
			let chain_key = format!("archives/.tmp-chains/{}-{}", archive_basename_s, chain_idx);
			let chain = chain.clone();
			async move {
				process_chain(
					&s3,
					&bucket,
					&files_prefix,
					&chain_key,
					&archive_key,
					&archive_upload_id,
					&plan,
					&chain,
					&stream_sem,
				)
				.await
			}
		},
	))
	.buffer_unordered(CHAIN_CONCURRENCY);

	futures::pin_mut!(chain_jobs);
	while let Some(res) = chain_jobs.next().await {
		final_parts.push(res?);
	}

	// Join the first chain (its merge-copy part).
	let first_part = first_handle
		.await
		.map_err(|e| AssembleError::BadCrc(format!("first-chain task join: {e}")))??;
	final_parts.push(first_part);

	// ---- Directory part: last part of the archive MPU (exempt from 5 MiB) ----
	let dir_part_number = (plan.chains.len() as i32) + 1;
	let dir_bytes = build_central_directory(&plan)?;
	let out = s3
		.upload_part()
		.bucket(bucket)
		.key(archive_key)
		.upload_id(&archive_upload_id)
		.part_number(dir_part_number)
		.body(ByteStream::from(dir_bytes))
		.send()
		.await?;
	let dir_etag = out
		.e_tag()
		.ok_or(AssembleError::NoEtag("directory upload_part"))?
		.to_string();
	final_parts.push(
		CompletedPart::builder()
			.part_number(dir_part_number)
			.e_tag(dir_etag)
			.build(),
	);

	// ---- Terminal complete ----
	final_parts.sort_by_key(|p| p.part_number());
	let completed = CompletedMultipartUpload::builder()
		.set_parts(Some(final_parts))
		.build();
	s3.complete_multipart_upload()
		.bucket(bucket)
		.key(archive_key)
		.upload_id(&archive_upload_id)
		.multipart_upload(completed)
		.send()
		.await?;

	Ok(())
}

/// Build a chain object, then copy it into the archive MPU at its pre-assigned slot.
/// Returns the final-merge CompletedPart. Used for both the first chain (own task) and the
/// normal chains (bounded pool).
#[allow(clippy::too_many_arguments)]
async fn process_chain(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	chain_key: &str,
	archive_key: &str,
	archive_upload_id: &str,
	plan: &Plan,
	chain: &Chain,
	stream_sem: &Arc<Semaphore>,
) -> Result<CompletedPart, AssembleError> {
	let _etag =
		build_chain_object(s3, bucket, files_prefix, chain_key, plan, chain, stream_sem).await?;
	let part_number = chain.final_merge_part_number as i32;
	let copy_source = format!("{bucket}/{chain_key}");
	let out = s3
		.upload_part_copy()
		.bucket(bucket)
		.key(archive_key)
		.upload_id(archive_upload_id)
		.part_number(part_number)
		.copy_source(copy_source)
		.send()
		.await?;
	let etag = out
		.copy_part_result()
		.and_then(|r| r.e_tag())
		.ok_or(AssembleError::NoEtag("upload_part_copy"))?
		.to_string();
	Ok(CompletedPart::builder()
		.part_number(part_number)
		.e_tag(etag)
		.build())
}

/// Build one chain object (its own MPU) and return its ETag. Streamed parts GET their
/// small bodies (bounded by `stream_sem`), copy parts use UploadPartCopy.
async fn build_chain_object(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	chain_key: &str,
	plan: &Plan,
	chain: &Chain,
	stream_sem: &Arc<Semaphore>,
) -> Result<String, AssembleError> {
	let upload_id = create_mpu(s3, bucket, chain_key).await?;
	let mut parts: Vec<CompletedPart> = Vec::new();

	for part in &chain.parts {
		match part {
			PartSpec::Copy { part_number, id } => {
				let entry = &plan.entries[id];
				let copy_source = format!("{bucket}/{files_prefix}/{}", entry.name);
				let out = s3
					.upload_part_copy()
					.bucket(bucket)
					.key(chain_key)
					.upload_id(&upload_id)
					.part_number(*part_number as i32)
					.copy_source(copy_source)
					.send()
					.await?;
				let etag = out
					.copy_part_result()
					.and_then(|r| r.e_tag())
					.ok_or(AssembleError::NoEtag("chain upload_part_copy"))?
					.to_string();
				parts.push(
					CompletedPart::builder()
						.part_number(*part_number as i32)
						.e_tag(etag)
						.build(),
				);
			}
			PartSpec::Stream {
				part_number,
				segments,
			} => {
				let _permit = stream_sem.acquire().await.unwrap();
				let body =
					build_stream_part_bytes(s3, bucket, files_prefix, plan, segments).await?;
				let out = s3
					.upload_part()
					.bucket(bucket)
					.key(chain_key)
					.upload_id(&upload_id)
					.part_number(*part_number as i32)
					.body(ByteStream::from(body))
					.send()
					.await?;
				let etag = out
					.e_tag()
					.ok_or(AssembleError::NoEtag("chain stream upload_part"))?
					.to_string();
				parts.push(
					CompletedPart::builder()
						.part_number(*part_number as i32)
						.e_tag(etag)
						.build(),
				);
			}
		}
	}

	parts.sort_by_key(|p| p.part_number());
	let completed = CompletedMultipartUpload::builder()
		.set_parts(Some(parts))
		.build();
	let out = s3
		.complete_multipart_upload()
		.bucket(bucket)
		.key(chain_key)
		.upload_id(&upload_id)
		.multipart_upload(completed)
		.send()
		.await?;
	out.e_tag()
		.map(ToOwned::to_owned)
		.ok_or(AssembleError::NoEtag("chain complete"))
}

/// Materialise a stream part's bytes: for each segment, either GET a small file and emit
/// `[local header][body]` (CRC self-computed) or emit a standalone copied-file header.
async fn build_stream_part_bytes(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &Plan,
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
		}
	}
	Ok(buf)
}

/// Central directory + ZIP64 end records, built from the realised plan (all CRCs known).
fn build_central_directory(plan: &Plan) -> Result<Vec<u8>, AssembleError> {
	// cd offset = total bytes of the entry region (header+body for every entry, in order).
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

async fn fill_crcs(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	plan: &mut Plan,
) -> Result<(), AssembleError> {
	// HEAD every entry to fetch its stored full-object CRC32. All objects carry one, so
	// this populates both copied (header rides a stream part) and streamed entries up front.
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
				let out = s3
					.head_object()
					.bucket(&bucket)
					.key(&key)
					.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
					.send()
					.await?;
				let crc_b64 = out.checksum_crc32().map(ToOwned::to_owned);
				Ok::<_, AssembleError>((id, crc_b64))
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

// Helper to name temp chain objects under archives/.
fn archive_basename(archive_key: &str) -> String {
	match archive_key.rfind('/') {
		Some(i) => archive_key[i + 1..].to_string(),
		None => archive_key.to_string(),
	}
}
