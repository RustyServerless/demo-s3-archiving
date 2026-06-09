//! Minimal streaming fallback for degenerate inputs (tiny / all-small / empty), where the
//! copy-part fast path is not viable. Deliberately simple: GET each file, write
//! `[local header][body]` into a buffer, flush parts at the 5 MiB boundary (or a single
//! PutObject if the whole archive is tiny), then append the central directory + EOCD.
//!
//! No slab ring, no zero-copy: the fallback case is small by definition, so plain owned
//! buffers are fine. Shares the pure `engine::zip_format` byte layout with the fast path.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

use crate::engine::plan::{FileId, SourceFile};
use crate::engine::zip_format::{self, EntryMeta};

const PART_FLOOR: usize = 5 * 1024 * 1024;

pub use crate::aws::assemble::AssembleError;

/// Stream every object under `files_prefix` into the archive at `archive_key`.
///
/// Re-lists the prefix itself (the fast path's plan isn't reused here — the fallback owns
/// its own simple traversal). Computes each entry's CRC from the bytes it reads.
pub async fn run(
	s3: &Client,
	bucket: &str,
	archive_key: &str,
	files_prefix: &str,
) -> Result<(), AssembleError> {
	// List source files.
	let files = list(s3, bucket, files_prefix).await?;

	// Build the whole archive in memory. Fallback inputs are small by definition.
	let mut archive: Vec<u8> = Vec::new();
	let mut metas: Vec<EntryMeta> = Vec::new();
	let mut offset: u64 = 0;
	for f in &files {
		let key = format!("{files_prefix}/{}", f.name);
		let body = get_bytes(s3, bucket, &key, f.size as usize).await?;
		let crc = {
			let mut h = crc32fast::Hasher::new();
			h.update(&body);
			h.finalize()
		};
		let meta = EntryMeta {
			name: f.name.clone(),
			size: f.size,
			crc,
			local_header_offset: offset,
		};
		let header = zip_format::local_header(&meta);
		offset += header.len() as u64 + body.len() as u64;
		archive.extend_from_slice(&header);
		archive.extend_from_slice(&body);
		metas.push(meta);
	}

	// Central directory + ZIP64 end records.
	let cd_offset = archive.len() as u64;
	let mut cd_size = 0u64;
	for m in &metas {
		let rec = zip_format::central_dir_entry(m);
		cd_size += rec.len() as u64;
		archive.extend_from_slice(&rec);
	}
	archive.extend_from_slice(&zip_format::end_records(
		metas.len() as u64,
		cd_offset,
		cd_size,
	));

	// Emit: single PutObject if small, else a simple multipart upload.
	if archive.len() < PART_FLOOR {
		s3.put_object()
			.bucket(bucket)
			.key(archive_key)
			.body(ByteStream::from(archive))
			.content_type("application/zip")
			.send()
			.await?;
		return Ok(());
	}

	// Multipart: 5 MiB parts, last part takes the remainder (exempt).
	let upload_id = s3
		.create_multipart_upload()
		.bucket(bucket)
		.key(archive_key)
		.content_type("application/zip")
		.send()
		.await?
		.upload_id()
		.map(ToOwned::to_owned)
		.ok_or(AssembleError::NoUploadId)?;

	let mut parts: Vec<CompletedPart> = Vec::new();
	let mut part_number = 1i32;
	let mut pos = 0usize;
	while pos < archive.len() {
		let end = (pos + PART_FLOOR).min(archive.len());
		let chunk = archive[pos..end].to_vec();
		let out = s3
			.upload_part()
			.bucket(bucket)
			.key(archive_key)
			.upload_id(&upload_id)
			.part_number(part_number)
			.body(ByteStream::from(chunk))
			.send()
			.await?;
		let etag = out
			.e_tag()
			.ok_or(AssembleError::NoEtag("fallback upload_part"))?
			.to_string();
		parts.push(
			CompletedPart::builder()
				.part_number(part_number)
				.e_tag(etag)
				.build(),
		);
		part_number += 1;
		pos = end;
	}

	let completed = CompletedMultipartUpload::builder()
		.set_parts(Some(parts))
		.build();
	s3.complete_multipart_upload()
		.bucket(bucket)
		.key(archive_key)
		.upload_id(&upload_id)
		.multipart_upload(completed)
		.send()
		.await?;
	Ok(())
}

async fn list(
	s3: &Client,
	bucket: &str,
	files_prefix: &str,
) -> Result<Vec<SourceFile>, AssembleError> {
	let s3_prefix = format!("{files_prefix}/");
	let mut paginator = s3
		.list_objects_v2()
		.bucket(bucket)
		.prefix(&s3_prefix)
		.into_paginator()
		.send();
	let mut out = Vec::new();
	let mut id = 0u32;
	while let Some(page) = paginator.next().await {
		let page = page?;
		for obj in page.contents() {
			let Some(key) = obj.key() else { continue };
			let Some(size) = obj.size() else { continue };
			if key == s3_prefix {
				continue;
			}
			if let Some(name) = key.strip_prefix(&s3_prefix) {
				if name.is_empty() {
					continue;
				}
				out.push(SourceFile {
					id: FileId(id),
					key: key.to_string(),
					name: name.to_string(),
					size: size as u64,
				});
				id += 1;
			}
		}
	}
	Ok(out)
}

async fn get_bytes(
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
