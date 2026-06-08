//! Pure planning. NO `aws_sdk_s3` IMPORTS — pure engine, testable with no AWS.
//!
//! `plan(files)` turns the S3 listing into a fully-ordered, fully-numbered `Plan` (or
//! routes to the streaming fallback). All ordering, chain construction, part numbering
//! and offset computation happen here, deterministically, from sizes alone. CRC *values*
//! are filled in later (phase 1 for copyables, stream-time for streamables); they do not
//! affect order, numbering, or offsets.

use std::collections::HashMap;

use crate::engine::zip_format;

/// S3 multipart upload minimum non-last part size (5 MiB).
pub const PART_FLOOR: u64 = 5 * 1024 * 1024;

/// Total archive must comfortably exceed the floor to use the copy-part fast path.
pub const VIABILITY_MIN_TOTAL: u64 = 4 * PART_FLOOR;

/// Stable identity for a file, independent of its position in any collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u32);

/// A source object from the S3 listing — the planner's only input.
#[derive(Debug, Clone)]
pub struct SourceFile {
	pub id: FileId,
	pub key: String,
	pub name: String, // ZIP entry name (key minus prefix)
	pub size: u64,
}

/// One archive entry, in canonical order. Offsets are computed by the planner; `crc` is
/// filled later (None until then).
#[derive(Debug, Clone)]
pub struct Entry {
	pub id: FileId,
	pub name: String,
	pub size: u64,
	pub local_header_offset: u64,
	pub crc: Option<u32>,
	pub streamed: bool, // true: header written inline by a Stream part; false: COPIED body
}

/// A piece of a Stream part.
#[derive(Debug, Clone)]
pub enum Segment {
	/// A streamed file: GET it, emit its (inline) header + body. CRC self-computed from body.
	StreamedFile { id: FileId },
	/// A standalone local header for a COPIED file (the trailing-header handoff). No body here.
	CopiedFileHeader { id: FileId },
}

/// One MPU part within a chain.
#[derive(Debug, Clone)]
pub enum PartSpec {
	/// Big body moved server-side. Off-ENI.
	Copy { part_number: u32, id: FileId },
	/// Lambda-materialised bytes. On-ENI (GETs) + upload.
	Stream {
		part_number: u32,
		segments: Vec<Segment>,
	},
}

/// The single-MPU plan: ONE archive multipart upload whose parts alternate Copy(big) and
/// Stream(batch-of-smalls), in `parts` order = MPU part numbers 1..=N. No temp objects, no
/// per-chain MPUs — the entire archive is built directly. Bigs are server-side copies (off-ENI);
/// smalls are streamed but BATCHED so each non-last stream part clears the 5 MiB floor. Each
/// big's local header rides the tail of the preceding stream part (trailing-header trick); the
/// first part is a stream that bootstraps entry-0's header. The final stream part + directory are
/// floor-exempt.
#[derive(Debug, Clone)]
pub struct SinglePlan {
	pub order: Vec<FileId>,
	pub entries: HashMap<FileId, Entry>,
	pub parts: Vec<PartSpec>,
	pub copyable: Vec<FileId>, // bigs needing a phase-1 CRC HEAD
	pub stats: PlanStats,
}

/// Planner decision summary, for logging on the assembler side (the engine stays tracing-free).
#[derive(Debug, Clone, Copy)]
pub struct PlanStats {
	pub entries: usize,
	pub parts: usize,
	pub copy_parts: usize,   // bigs moved server-side (off-ENI)
	pub stream_parts: usize, // batched stream parts
	pub folded_bigs: usize,  // bigs forced to STREAM because smalls ran out (on-ENI)
	pub bigs: usize,
	pub smalls: usize,
}

/// Build the single-MPU plan, or route to Fallback if the fast path isn't viable.
pub fn plan_single_mpu(files: Vec<SourceFile>) -> SingleRouting {
	let (mut bigs, mut smalls): (Vec<SourceFile>, Vec<SourceFile>) =
		files.into_iter().partition(|f| f.size >= PART_FLOOR);
	let total: u64 = bigs.iter().chain(smalls.iter()).map(|f| f.size).sum();
	if bigs.is_empty() || total < VIABILITY_MIN_TOTAL {
		return SingleRouting::Fallback;
	}
	// Bigs LARGEST-FIRST: the small-byte budget can chaperone only ~N bigs over the floor as
	// copies; the rest are forced to STREAM. By copying the biggest bigs first, the forced folds
	// land on the SMALLEST bigs — minimising the big-bytes that cross the ENI. Ties by id for
	// determinism. Smalls in opposite order (smallest first) so we pair smallest with biggest.
	bigs.sort_by(|a, b| b.size.cmp(&a.size).then(a.id.0.cmp(&b.id.0))); // descending
	smalls.sort_by(|a, b| a.size.cmp(&b.size).then(a.id.0.cmp(&b.id.0))); // ascending

	let mut size_of: HashMap<FileId, (String, u64)> = HashMap::new();
	for f in bigs.iter().chain(smalls.iter()) {
		size_of.insert(f.id, (f.name.clone(), f.size));
	}

	// ----- Build the canonical entry ORDER and the alternating PART list together. -----
	// Layout (entry order): [hBig0 Big0] [hS S]xK [hBig1 Big1] [hS S]xK ... then leftover smalls.
	// Part list (MPU order):
	//   part1 = Stream( hBig0 + first batch of smalls' [hS][S] )   -- bootstraps Big0's header
	//   part2 = Copy(Big0)
	//   part3 = Stream( next batch [hS][S] + hBig1 )
	//   part4 = Copy(Big1)
	//   ...
	//   final = Stream( leftover smalls [hS][S] )  -- exempt last part; directory appended after
	// Smalls are distributed across the stream parts; each non-last stream part must reach
	// PART_FLOOR (batch ~2+ smalls). The stream part BEFORE Copy(Big_k) carries hBig_k on its tail.
	let mut order: Vec<FileId> = Vec::new();
	let mut parts: Vec<PartSpec> = Vec::new();
	let mut part_number: u32 = 1;
	let mut small_iter = smalls.iter().peekable();

	// A running stream batch (segments) we flush into a Stream part when it clears the floor or
	// when we need to hand off a big header.
	let mut cur_segs: Vec<Segment> = Vec::new();
	let mut cur_bytes: u64 = 0;
	// Bigs that ended up STREAMED (folded into a stream part because smalls ran out before the
	// batch reached the floor) rather than copied. These do NOT need a phase-1 CRC HEAD.
	let mut streamed_bigs: std::collections::HashSet<FileId> = std::collections::HashSet::new();

	for big in bigs.iter() {
		// Fill the current stream batch with smalls until it clears the floor (or smalls run out).
		while cur_bytes < PART_FLOOR {
			if let Some(s) = small_iter.peek() {
				let sid = s.id;
				cur_segs.push(Segment::StreamedFile { id: sid });
				order.push(sid);
				let (ref name, sz) = size_of[&sid];
				cur_bytes += zip_format::local_header_len(name) + sz;
				small_iter.next();
			} else {
				break;
			}
		}

		if cur_bytes >= PART_FLOOR {
			// Normal case: the batch is a valid non-last part. Ride this big's header on its tail
			// (trailing handoff), flush the stream part, then COPY the big body server-side.
			cur_segs.push(Segment::CopiedFileHeader { id: big.id });
			order.push(big.id);
			parts.push(PartSpec::Stream {
				part_number,
				segments: std::mem::take(&mut cur_segs),
			});
			part_number += 1;
			cur_bytes = 0;

			parts.push(PartSpec::Copy {
				part_number,
				id: big.id,
			});
			part_number += 1;
		} else {
			// Smalls ran out before the batch reached the floor: emitting it as a non-last part
			// would violate the 5 MiB floor. Instead FOLD this big into the batch by STREAMING it
			// ([hBig][big-body] inline) — the big alone exceeds the floor, so the part is valid.
			// This costs ENI for the folded big, but only happens once smalls are exhausted
			// (the tail), so nearly all bigs are still copied off-ENI.
			cur_segs.push(Segment::StreamedFile { id: big.id });
			order.push(big.id);
			streamed_bigs.insert(big.id);
			parts.push(PartSpec::Stream {
				part_number,
				segments: std::mem::take(&mut cur_segs),
			});
			part_number += 1;
			cur_bytes = 0;
		}
	}

	// Any leftover smalls go into a final stream part (exempt last part; directory appended after).
	for s in small_iter {
		cur_segs.push(Segment::StreamedFile { id: s.id });
		order.push(s.id);
	}
	if !cur_segs.is_empty() {
		parts.push(PartSpec::Stream {
			part_number,
			segments: std::mem::take(&mut cur_segs),
		});
	}

	// ----- Offsets + entries from the final order. -----
	let offsets = compute_offsets(&order, &size_of);
	let mut entries: HashMap<FileId, Entry> = HashMap::new();
	let big_ids: std::collections::HashSet<FileId> = bigs.iter().map(|f| f.id).collect();
	for (i, id) in order.iter().enumerate() {
		let (name, size) = size_of[id].clone();
		// Copied iff it's a big that was NOT folded into a stream part.
		let copied = big_ids.contains(id) && !streamed_bigs.contains(id);
		entries.insert(
			*id,
			Entry {
				id: *id,
				name,
				size,
				local_header_offset: offsets[i],
				crc: None,
				streamed: !copied,
			},
		);
	}
	// Only copied bigs need a phase-1 CRC HEAD; folded (streamed) bigs self-compute at stream time.
	let copyable: Vec<FileId> = bigs
		.iter()
		.map(|f| f.id)
		.filter(|id| !streamed_bigs.contains(id))
		.collect();

	let copy_parts = parts
		.iter()
		.filter(|p| matches!(p, PartSpec::Copy { .. }))
		.count();
	let stats = PlanStats {
		entries: order.len(),
		parts: parts.len(),
		copy_parts,
		stream_parts: parts.len() - copy_parts,
		folded_bigs: streamed_bigs.len(),
		bigs: bigs.len(),
		smalls: smalls.len(),
	};

	SingleRouting::SingleMpu(SinglePlan {
		order,
		entries,
		parts,
		copyable,
		stats,
	})
}

#[derive(Debug, Clone)]
pub enum SingleRouting {
	SingleMpu(SinglePlan),
	Fallback,
}

/// Compute local-header offsets across `order` via a single fold (the only place offset
/// arithmetic lives).
fn compute_offsets(order: &[FileId], size_of: &HashMap<FileId, (String, u64)>) -> Vec<u64> {
	order
		.iter()
		.scan(0u64, |acc, id| {
			let here = *acc;
			let (name, size) = &size_of[id];
			*acc += zip_format::entry_total_len(name, *size);
			Some(here)
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	// ===================================================================================
	// Single-MPU alternating copy/stream-batch layout: build the archive straight from the
	// part list and validate with the real zip reader. One MPU, no chains, no H blob.
	// Run with: cargo test -p figment-engine --features zip_validate single_mpu_layout
	// ===================================================================================
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_layout_matches_directory_offsets() {
		use crate::engine::zip_format::{self, EntryMeta};
		use sha2::{Digest, Sha256};
		use std::collections::HashSet;
		use std::io::{Cursor, Read};

		fn sha256_hex(b: &[u8]) -> String {
			let mut h = Sha256::new();
			h.update(b);
			let d = h.finalize();
			let mut s = String::with_capacity(64);
			for x in d {
				use core::fmt::Write;
				let _ = write!(s, "{:02x}", x);
			}
			s
		}
		fn crc32(b: &[u8]) -> u32 {
			let mut h = crc32fast::Hasher::new();
			h.update(b);
			h.finalize()
		}

		// Mix of bigs (>=floor) and smalls; smalls sized so 2 per batch clears the floor.
		let big = PART_FLOOR as usize + 77;
		let small = (PART_FLOOR as usize / 2) + 1000; // two smalls > floor
		let raw: Vec<Vec<u8>> = vec![
			vec![1u8; big],
			vec![2u8; small],
			vec![3u8; small],
			vec![4u8; big],
			vec![5u8; small],
			vec![6u8; small],
			vec![7u8; big],
			vec![8u8; small],
			vec![9u8; small],
			vec![10u8; small], // odd leftover small -> final stream part
		];
		let names: Vec<String> = raw.iter().map(|c| sha256_hex(c)).collect();
		let files: Vec<SourceFile> = (0..raw.len())
			.map(|i| SourceFile {
				id: FileId(i as u32),
				key: format!("files/{}", names[i]),
				name: names[i].clone(),
				size: raw[i].len() as u64,
			})
			.collect();

		let mut plan = match plan_single_mpu(files) {
			SingleRouting::SingleMpu(p) => p,
			SingleRouting::Fallback => panic!("expected single-MPU fast path"),
		};

		// Fill bigs' CRCs (phase-1 stand-in).
		let ids: Vec<FileId> = plan.copyable.clone();
		for id in ids {
			if let Some(e) = plan.entries.get_mut(&id) {
				e.crc = Some(crc32(&raw[id.0 as usize]));
			}
		}

		let header_bytes = |id: FileId| -> Vec<u8> {
			let e = &plan.entries[&id];
			let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
			zip_format::local_header(&EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			})
		};

		// ---- Build the archive straight from the part list (MPU part order). ----
		// Copy(id) => [body]; Stream(segments) => StreamedFile => [header][body],
		// CopiedFileHeader => [header] (the trailing big-header handoff).
		let mut archive: Vec<u8> = Vec::new();
		// Verify every non-last part clears the floor (last part exempt).
		let nparts = plan.parts.len();
		for (pi, part) in plan.parts.iter().enumerate() {
			let before = archive.len();
			match part {
				PartSpec::Copy { id, .. } => {
					archive.extend_from_slice(&raw[id.0 as usize]);
				}
				PartSpec::Stream { segments, .. } => {
					for seg in segments {
						match seg {
							Segment::StreamedFile { id } => {
								archive.extend_from_slice(&header_bytes(*id));
								archive.extend_from_slice(&raw[id.0 as usize]);
							}
							Segment::CopiedFileHeader { id } => {
								archive.extend_from_slice(&header_bytes(*id));
							}
						}
					}
				}
			}
			let part_len = (archive.len() - before) as u64;
			if pi + 1 < nparts {
				assert!(
					part_len >= PART_FLOOR,
					"non-last part {} is {} bytes, below the 5 MiB floor",
					pi + 1,
					part_len
				);
			}
		}

		// ---- Append central directory + end records. ----
		let mut cd_offset = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			cd_offset += zip_format::entry_total_len(&e.name, e.size);
		}
		assert_eq!(
			archive.len() as u64,
			cd_offset,
			"single-MPU entry region ({}) != directory cd_offset ({}); part layout disagrees \
             with plan.order offsets",
			archive.len(),
			cd_offset
		);
		let mut cd_size = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
			let rec = zip_format::central_dir_entry(&EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			});
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		archive.extend_from_slice(&zip_format::end_records(
			plan.order.len() as u64,
			cd_offset,
			cd_size,
		));

		// ---- Validate with the standard zip reader. ----
		let mut expected: HashSet<String> = plan
			.order
			.iter()
			.map(|id| plan.entries[id].name.clone())
			.collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("single-MPU archive must parse with the standard zip reader");
		assert_eq!(za.len(), plan.order.len());
		let arch_names: Vec<String> = za.file_names().map(ToOwned::to_owned).collect();
		for n in &arch_names {
			assert!(!n.contains('/'), "flat layout required");
			assert!(expected.remove(n), "unknown/duplicate {n}");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
		for n in &arch_names {
			let mut entry = za.by_name(n).unwrap();
			let mut buf = Vec::new();
			entry
				.read_to_end(&mut buf)
				.expect("extract (CRC verified by reader)");
			assert_eq!(&sha256_hex(&buf), n, "content hash == name");
		}
	}

	// Smalls run out while bigs remain: the remaining bigs must be FOLDED into stream parts
	// (streamed, not copied) so no undersized non-last part is emitted. Many bigs, few smalls.
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_handles_smalls_exhausted_early() {
		use crate::engine::zip_format::{self, EntryMeta};
		use sha2::{Digest, Sha256};
		use std::collections::HashSet;
		use std::io::{Cursor, Read};

		fn sha256_hex(b: &[u8]) -> String {
			let mut h = Sha256::new();
			h.update(b);
			let d = h.finalize();
			let mut s = String::with_capacity(64);
			for x in d {
				use core::fmt::Write;
				let _ = write!(s, "{:02x}", x);
			}
			s
		}
		fn crc32(b: &[u8]) -> u32 {
			let mut h = crc32fast::Hasher::new();
			h.update(b);
			h.finalize()
		}

		let big = PART_FLOOR as usize + 77;
		let small = (PART_FLOOR as usize / 2) + 1000;
		// 6 bigs, only 2 smalls — smalls exhaust after the first big's batch; bigs 1..5 must fold.
		let raw: Vec<Vec<u8>> = vec![
			vec![1u8; big],
			vec![2u8; small],
			vec![3u8; small],
			vec![4u8; big],
			vec![5u8; big],
			vec![6u8; big],
			vec![7u8; big],
			vec![8u8; big],
		];
		let names: Vec<String> = raw.iter().map(|c| sha256_hex(c)).collect();
		let files: Vec<SourceFile> = (0..raw.len())
			.map(|i| SourceFile {
				id: FileId(i as u32),
				key: format!("files/{}", names[i]),
				name: names[i].clone(),
				size: raw[i].len() as u64,
			})
			.collect();

		let mut plan = match plan_single_mpu(files) {
			SingleRouting::SingleMpu(p) => p,
			SingleRouting::Fallback => panic!("expected single-MPU fast path"),
		};
		let ids: Vec<FileId> = plan.copyable.clone();
		for id in ids {
			if let Some(e) = plan.entries.get_mut(&id) {
				e.crc = Some(crc32(&raw[id.0 as usize]));
			}
		}

		let header_bytes = |id: FileId| -> Vec<u8> {
			let e = &plan.entries[&id];
			let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
			zip_format::local_header(&EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			})
		};

		let mut archive: Vec<u8> = Vec::new();
		let nparts = plan.parts.len();
		for (pi, part) in plan.parts.iter().enumerate() {
			let before = archive.len();
			match part {
				PartSpec::Copy { id, .. } => archive.extend_from_slice(&raw[id.0 as usize]),
				PartSpec::Stream { segments, .. } => {
					for seg in segments {
						match seg {
							Segment::StreamedFile { id } => {
								archive.extend_from_slice(&header_bytes(*id));
								archive.extend_from_slice(&raw[id.0 as usize]);
							}
							Segment::CopiedFileHeader { id } => {
								archive.extend_from_slice(&header_bytes(*id));
							}
						}
					}
				}
			}
			let part_len = (archive.len() - before) as u64;
			if pi + 1 < nparts {
				assert!(
					part_len >= PART_FLOOR,
					"non-last part {} is {} bytes, below floor (smalls-exhausted path)",
					pi + 1,
					part_len
				);
			}
		}

		let mut cd_offset = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			cd_offset += zip_format::entry_total_len(&e.name, e.size);
		}
		assert_eq!(archive.len() as u64, cd_offset, "entry region != cd_offset");
		let mut cd_size = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
			let rec = zip_format::central_dir_entry(&EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			});
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		archive.extend_from_slice(&zip_format::end_records(
			plan.order.len() as u64,
			cd_offset,
			cd_size,
		));

		let mut expected: HashSet<String> = plan
			.order
			.iter()
			.map(|id| plan.entries[id].name.clone())
			.collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("smalls-exhausted archive must parse");
		assert_eq!(za.len(), plan.order.len());
		for n in za.file_names().map(ToOwned::to_owned).collect::<Vec<_>>() {
			assert!(expected.remove(&n), "unknown/duplicate {n}");
			let mut entry = za.by_name(&n).unwrap();
			let mut buf = Vec::new();
			entry.read_to_end(&mut buf).expect("extract");
			assert_eq!(&sha256_hex(&buf), &n, "content hash == name");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
	}
}
