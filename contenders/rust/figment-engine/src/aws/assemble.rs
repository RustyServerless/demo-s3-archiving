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
use std::time::Instant;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::stream::{StreamExt, TryStreamExt};
use tokio::sync::Semaphore;

use crate::engine::crc::decode_s3_crc32;
use crate::engine::header_blob::{HeaderBlob, HeaderRange, build_header_blob};
use crate::engine::plan::{Chain, ChainPlan, Entry, FileId, LayeredChain, PartSpec, Plan, Segment};
use crate::engine::zip_format::{self, EntryMeta};

/// Tunables. Stream concurrency saturates the ENI; layer concurrency bounds off-ENI copies.
const STREAM_CONCURRENCY: usize = 24;
const CRC_CONCURRENCY: usize = 64;
/// Concurrent parts within a single chain (matters for the first chain, ~1,200 parts).
const PART_CONCURRENCY: usize = 32;
/// Across-chain concurrency for each layer wave (server-side copies; bound for FD/throttle).
const LAYER_CONCURRENCY: usize = 64;

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
	let t_crc = Instant::now();
	fill_crcs(s3, bucket, files_prefix, &mut plan).await?;
	tracing::info!(
		ms = t_crc.elapsed().as_millis(),
		entries = plan.order.len(),
		"PHASE crc_heads"
	);
	let plan = Arc::new(plan);

	// ---- Phase 0: build the headers blob H and PutObject it (the ONLY ENI upload of bodies) ----
	// Layered normal chains copy each header as a byte-range of H (server-side), so no header
	// is streamed. H is ~hundreds of KB for thousands of entries.
	let t_h = Instant::now();
	let metas = plan.order.iter().map(|id| {
		let e = &plan.entries[id];
		let crc = e.crc.unwrap_or(0);
		(
			*id,
			EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			},
		)
	});
	let (h_bytes, h_blob) = build_header_blob(metas);
	let h_key = format!("archives/.tmp-chains/{}-H", archive_basename(archive_key));
	s3.put_object()
		.bucket(bucket)
		.key(&h_key)
		.body(ByteStream::from(h_bytes))
		.send()
		.await?;
	let h_blob = Arc::new(h_blob);
	tracing::info!(
		ms = t_h.elapsed().as_millis(),
		len = h_blob.bytes_len,
		"PHASE headers_blob"
	);

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
	let t_chains = Instant::now();
	let first_stream_sem = Arc::new(Semaphore::new(STREAM_CONCURRENCY));
	let first_handle = {
		let s3 = s3.clone();
		let plan = plan.clone();
		let bucket = bucket.to_string();
		let files_prefix = files_prefix.to_string();
		let archive_key = archive_key.to_string();
		let archive_upload_id = archive_upload_id.clone();
		let chain = match &plan.chains[0] {
			ChainPlan::FirstStream(c) => c.clone(),
			ChainPlan::Layered(_) => {
				return Err(AssembleError::BadCrc(
					"first chain must be FirstStream".into(),
				));
			}
		};
		let chain_key = format!("archives/.tmp-chains/{}-0", archive_basename_s);
		tokio::spawn(async move {
			let t = Instant::now();
			let r = process_chain(
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
			.await;
			tracing::info!(
				ms = t.elapsed().as_millis(),
				parts = chain.parts.len(),
				"PHASE first_chain"
			);
			r
		})
	};

	// Normal chains (1..): layered all-copy recipe, no ENI bytes. Run BY LAYER across all
	// chains, not per-chain: layer i of every chain is independent of other chains, so each
	// layer is one wide parallel wave. Within a chain layers are ordered (l2 copies l1, l3
	// copies l2), so we run wave 1 (all l1s) to completion, then wave 2 (all l2s), then wave 3.
	let layered: Vec<(u32, Vec<LayerSpec>)> = plan
		.chains
		.iter()
		.enumerate()
		.skip(1)
		.filter_map(|(i, c)| match c {
			ChainPlan::Layered(l) => {
				let chain_key = format!("archives/.tmp-chains/{}-{}", archive_basename_s, i);
				let specs =
					build_layer_specs(bucket, files_prefix, &chain_key, &h_key, &h_blob, &plan, l);
				Some((l.final_merge_part_number, specs))
			}
			ChainPlan::FirstStream(_) => None,
		})
		.collect();

	let max_layers = layered.iter().map(|(_, s)| s.len()).max().unwrap_or(0);
	for layer_idx in 0..max_layers {
		// Collect this layer's (key, parts) from every chain that has a layer at this depth.
		let wave: Vec<(String, Vec<CopyPart>)> = layered
			.iter()
			.filter_map(|(_, specs)| {
				specs
					.get(layer_idx)
					.map(|ls| (ls.key.clone(), ls.parts.iter().map(|p| p.clone()).collect()))
			})
			.collect();
		let n = wave.len();
		let jobs = futures::stream::iter(wave.into_iter().map(|(key, parts)| {
			let s3 = s3.clone();
			let bucket = bucket.to_string();
			async move { run_copy_mpu(&s3, &bucket, &key, &parts).await }
		}))
		.buffer_unordered(LAYER_CONCURRENCY);
		futures::pin_mut!(jobs);
		let mut done = 0usize;
		while let Some(res) = jobs.next().await {
			res?;
			done += 1;
		}
		tracing::info!(
			ms = t_chains.elapsed().as_millis(),
			layer = layer_idx + 1,
			ops = n,
			done,
			"PHASE layer_wave"
		);
	}

	// Final wave: merge each chain's final object into the archive at its slot (parallel).
	let merges: Vec<(u32, String)> = layered
		.iter()
		.map(|(pn, specs)| (*pn, specs.last().expect("chain has >=1 layer").key.clone()))
		.collect();
	let merge_jobs = futures::stream::iter(merges.into_iter().map(|(part_number, final_key)| {
		let s3 = s3.clone();
		let bucket = bucket.to_string();
		let archive_key = archive_key.to_string();
		let archive_upload_id = archive_upload_id.clone();
		async move {
			let out = s3
				.upload_part_copy()
				.bucket(&bucket)
				.key(&archive_key)
				.upload_id(&archive_upload_id)
				.part_number(part_number as i32)
				.copy_source(format!("{bucket}/{final_key}"))
				.send()
				.await?;
			let etag = out
				.copy_part_result()
				.and_then(|r| r.e_tag())
				.ok_or(AssembleError::NoEtag("layered merge upload_part_copy"))?
				.to_string();
			Ok::<_, AssembleError>(
				CompletedPart::builder()
					.part_number(part_number as i32)
					.e_tag(etag)
					.build(),
			)
		}
	}))
	.buffer_unordered(LAYER_CONCURRENCY);
	futures::pin_mut!(merge_jobs);
	while let Some(res) = merge_jobs.next().await {
		final_parts.push(res?);
	}
	tracing::info!(
		ms = t_chains.elapsed().as_millis(),
		n = plan.chains.len() - 1,
		"PHASE normal_pool_drained"
	);

	// Join the first chain (its merge-copy part).
	let first_part = first_handle
		.await
		.map_err(|e| AssembleError::BadCrc(format!("first-chain task join: {e}")))??;
	final_parts.push(first_part);
	tracing::info!(
		ms = t_chains.elapsed().as_millis(),
		"PHASE all_chains_joined"
	);

	// ---- Directory part: last part of the archive MPU (exempt from 5 MiB) ----
	let t_dir = Instant::now();
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
	tracing::info!(ms = t_dir.elapsed().as_millis(), "PHASE directory_part");

	// ---- Terminal complete ----
	let t_complete = Instant::now();
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
	tracing::info!(
		ms = t_complete.elapsed().as_millis(),
		"PHASE terminal_complete"
	);

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

/// One layer of a chain's build: an output object key and the copy-parts that compose it.
/// Later layers reference earlier layers' keys, so layers must run in order WITHIN a chain —
/// but layer i of different chains are independent and run together in a wave.
struct LayerSpec {
	key: String,
	parts: Vec<CopyPart>,
}

/// Build the ordered layer specs for one layered chain (pure: no awaits, no I/O). The final
/// chain object is the last layer's key. Variable length: 1 layer (unpaired, last) up to 3.
fn build_layer_specs(
	bucket: &str,
	files_prefix: &str,
	chain_key: &str,
	h_key: &str,
	h_blob: &HeaderBlob,
	plan: &Plan,
	chain: &LayeredChain,
) -> Vec<LayerSpec> {
	let big = &plan.entries[&chain.big];
	let big_src = format!("{bucket}/{files_prefix}/{}", big.name);
	let mut layers: Vec<LayerSpec> = Vec::new();

	// L1: [B] (+ header(small) if paired)
	let l1_key = format!("{chain_key}.l1");
	let mut l1_parts = vec![CopyPart::whole(big_src)];
	if let Some(sid) = chain.small {
		l1_parts.push(CopyPart::range(
			h_source(bucket, h_key),
			h_blob.ranges[&sid],
		));
	}
	layers.push(LayerSpec {
		key: l1_key.clone(),
		parts: l1_parts,
	});
	let mut prev_key = l1_key;

	// L2: [prev][small body]  (only if paired)
	if let Some(sid) = chain.small {
		let small = &plan.entries[&sid];
		let small_src = format!("{bucket}/{files_prefix}/{}", small.name);
		let l2_key = format!("{chain_key}.l2");
		layers.push(LayerSpec {
			key: l2_key.clone(),
			parts: vec![
				CopyPart::whole(format!("{bucket}/{prev_key}")),
				CopyPart::whole(small_src),
			],
		});
		prev_key = l2_key;
	}

	// L3: [prev][header(next_big)]  (only if there's a trailing header)
	if let Some(nb) = chain.next_big_header {
		let l3_key = format!("{chain_key}.l3");
		layers.push(LayerSpec {
			key: l3_key.clone(),
			parts: vec![
				CopyPart::whole(format!("{bucket}/{prev_key}")),
				CopyPart::range(h_source(bucket, h_key), h_blob.ranges[&nb]),
			],
		});
	}

	layers
}

/// A copy-source for one MPU part: a whole object, or a byte range of one.
#[derive(Clone)]
struct CopyPart {
	source: String,
	range: Option<(u64, u64)>, // (first_byte, last_byte) inclusive, for x-amz-copy-source-range
}
impl CopyPart {
	fn whole(source: String) -> Self {
		CopyPart {
			source,
			range: None,
		}
	}
	fn range(source: String, r: HeaderRange) -> Self {
		// S3 copy-source-range is inclusive: bytes=first-last.
		CopyPart {
			source,
			range: Some((r.offset, r.end() - 1)),
		}
	}
}

fn h_source(bucket: &str, h_key: &str) -> String {
	format!("{bucket}/{h_key}")
}

/// Create an MPU, upload each CopyPart via UploadPartCopy (part numbers 1..=n, last is exempt
/// from the 5 MiB floor), and complete it. All server-side; no ENI bytes.
async fn run_copy_mpu(
	s3: &Client,
	bucket: &str,
	key: &str,
	parts: &[CopyPart],
) -> Result<(), AssembleError> {
	let upload_id = create_mpu(s3, bucket, key).await?;
	let mut completed: Vec<CompletedPart> = Vec::with_capacity(parts.len());
	for (i, p) in parts.iter().enumerate() {
		let pn = (i + 1) as i32;
		let mut req = s3
			.upload_part_copy()
			.bucket(bucket)
			.key(key)
			.upload_id(&upload_id)
			.part_number(pn)
			.copy_source(&p.source);
		if let Some((first, last)) = p.range {
			req = req.copy_source_range(format!("bytes={first}-{last}"));
		}
		let out = req.send().await?;
		let etag = out
			.copy_part_result()
			.and_then(|r| r.e_tag())
			.ok_or(AssembleError::NoEtag("layer upload_part_copy"))?
			.to_string();
		completed.push(CompletedPart::builder().part_number(pn).e_tag(etag).build());
	}
	let mpu = CompletedMultipartUpload::builder()
		.set_parts(Some(completed))
		.build();
	s3.complete_multipart_upload()
		.bucket(bucket)
		.key(key)
		.upload_id(&upload_id)
		.multipart_upload(mpu)
		.send()
		.await?;
	Ok(())
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

	// Parts within a chain are independent (pre-assigned part numbers, ETags collected at
	// the end, S3 assembles by number at Complete). Run them concurrently: this is what
	// lets the FIRST chain — which has ~1,200 stream parts for ~6 GB — stream wide instead
	// of one-part-at-a-time. Normal chains have a single stream part so this is a no-op for
	// them. Memory is bounded by `stream_sem` (acquired inside each stream part before the
	// GET/alloc), so at most STREAM_CONCURRENCY part buffers are resident at once.
	//
	// The per-part work is a named async fn (not an inline closure) so its lifetimes are
	// well-formed under the surrounding tokio::spawn — a closure returning an async block
	// trips higher-ranked-lifetime inference here.
	// well-formed under the surrounding tokio::spawn. Build the futures with a plain loop
	// (no closure capturing `part` across a borrow boundary) so the spawn's 'static bound
	// doesn't trip higher-ranked-lifetime inference on a map closure.
	let upload_id = upload_id.as_str();
	let mut part_futures = Vec::with_capacity(chain.parts.len());
	for part in &chain.parts {
		part_futures.push(build_one_part(
			s3,
			bucket,
			files_prefix,
			chain_key,
			upload_id,
			plan,
			stream_sem,
			part,
		));
	}

	let mut parts: Vec<CompletedPart> = futures::stream::iter(part_futures)
		.buffer_unordered(PART_CONCURRENCY)
		.try_collect()
		.await?;

	parts.sort_by_key(|p| p.part_number());
	let completed = CompletedMultipartUpload::builder()
		.set_parts(Some(parts))
		.build();
	let out = s3
		.complete_multipart_upload()
		.bucket(bucket)
		.key(chain_key)
		.upload_id(upload_id)
		.multipart_upload(completed)
		.send()
		.await?;
	out.e_tag()
		.map(ToOwned::to_owned)
		.ok_or(AssembleError::NoEtag("chain complete"))
}

/// Process one part of a chain MPU: a Copy part issues UploadPartCopy from the source file;
/// a Stream part acquires the stream permit (memory backstop), materialises its bytes, and
/// UploadPart's them. Named fn (not a closure) for well-formed lifetimes under buffer_unordered.
#[allow(clippy::too_many_arguments)]
async fn build_one_part(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
	chain_key: &str,
	upload_id: &str,
	plan: &Plan,
	stream_sem: &Arc<Semaphore>,
	part: &PartSpec,
) -> Result<CompletedPart, AssembleError> {
	match part {
		PartSpec::Copy { part_number, id } => {
			let entry = &plan.entries[id];
			let copy_source = format!("{bucket}/{files_prefix}/{}", entry.name);
			let out = s3
				.upload_part_copy()
				.bucket(bucket)
				.key(chain_key)
				.upload_id(upload_id)
				.part_number(*part_number as i32)
				.copy_source(copy_source)
				.send()
				.await?;
			let etag = out
				.copy_part_result()
				.and_then(|r| r.e_tag())
				.ok_or(AssembleError::NoEtag("chain upload_part_copy"))?
				.to_string();
			Ok(CompletedPart::builder()
				.part_number(*part_number as i32)
				.e_tag(etag)
				.build())
		}
		PartSpec::Stream {
			part_number,
			segments,
		} => {
			let _permit = stream_sem.acquire().await.unwrap();
			let body = build_stream_part_bytes(s3, bucket, files_prefix, plan, segments).await?;
			let out = s3
				.upload_part()
				.bucket(bucket)
				.key(chain_key)
				.upload_id(upload_id)
				.part_number(*part_number as i32)
				.body(ByteStream::from(body))
				.send()
				.await?;
			let etag = out
				.e_tag()
				.ok_or(AssembleError::NoEtag("chain stream upload_part"))?
				.to_string();
			Ok(CompletedPart::builder()
				.part_number(*part_number as i32)
				.e_tag(etag)
				.build())
		}
	}
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
