//! Segment-chain planner — copy-only archive layout (the "Gen3" speed design).
//!
//! NO `aws_sdk_s3` IMPORTS — pure engine, testable with no AWS. Mirrors the
//! discipline of `engine::plan`: all ordering, segment grouping, link/part
//! structure and offset arithmetic happen here, deterministically, from sizes
//! alone. CRC *values* are filled later (every body is copied, so every entry
//! needs a phase-1 HEAD); they do not affect order, structure or offsets, and the
//! materialised header / central-directory bytes are produced at assemble time.
//!
//! ZIP byte-layout is the shared `engine::zip_format` (no second implementation).
//!
//! Design: copy-only-plan.md + segment-chain-wiring.md. Every image body reaches
//! the archive by server-side UploadPartCopy, bar a single 5 MiB bootstrap read
//! for the very first entry (a STORE ZIP has no preamble, so `LFH_big0` at
//! offset 0 has no prior link to ride on). Segments are built as per-segment MPU
//! objects chained by copy-forward, then copy-stitched into the final archive.
//!
//! Cross-boundary trailing headers: an interior segment's leading big header rides
//! the PREVIOUS segment's tail. So segment k's object physically ends with segment
//! k+1's big header, and segment k+1's object starts with that big's BODY. When
//! stitched, `…[LFH_big_{k+1}]` ++ `[big_{k+1}]…` is contiguous and valid.

use std::collections::HashMap;

use figment_engine::engine::plan::{FileId, SourceFile, PART_FLOOR};
use figment_engine::engine::zip_format;

/// One archive entry in canonical order. `crc` is filled after planning (phase-1
/// HEAD); `local_header_offset` is the absolute offset of this entry's local
/// header in the FINAL stitched archive.
#[derive(Debug, Clone)]
pub struct Entry {
	pub id: FileId,
	pub name: String,
	pub size: u64,
	pub local_header_offset: u64,
	pub crc: Option<u32>,
}

/// A piece appended to a segment object as the floor-exempt LAST part of one link.
/// Carries a `FileId` reference, not bytes: header bytes are materialised at
/// assemble time (they need the CRC) exactly like `engine::plan` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece {
	/// A generated local file header — a small's, or the NEXT segment's big's
	/// (the cross-boundary trailing header).
	Header(FileId),
	/// A copied body (UploadPartCopy from the source object).
	Body(FileId),
}

/// One MPU within a segment's chain. Every link = create + (one non-last part
/// that clears the 5 MiB floor) + (one floor-exempt last part) + complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
	/// First link of the FIRST segment only. Produces `[LFH_big0][big0]`.
	///   part1 = UploadPart(LFH_big0 ++ big0[..steal_len])   (read; == 5 MiB, non-last)
	///   part2 = UploadPartCopy(big0[steal_len..])           (exempt last)
	/// The one and only body read in the whole design.
	Bootstrap { anchor: FileId, steal_len: u64 },
	/// First link of a NON-first segment with nothing to append (last segment,
	/// solo big). Single part = UploadPartCopy(anchor), exempt (only part).
	AnchorOnly { anchor: FileId },
	/// First link of a NON-first segment. The big's header already rode the
	/// previous segment's tail, so the object starts with the big BODY.
	///   part1 = UploadPartCopy(anchor)   (>=5 MiB, non-last)
	///   part2 = append `piece`           (exempt last)
	AnchorThenAppend { anchor: FileId, piece: Piece },
	/// Subsequent links: copy the growing object forward, append one piece.
	///   part1 = UploadPartCopy(prev segment object)   (>=5 MiB, non-last)
	///   part2 = append `piece`                        (exempt last)
	ForwardThenAppend { piece: Piece },
}

/// One segment: a floor-anchoring big plus its smalls, realised as a link chain.
#[derive(Debug, Clone)]
pub struct Segment {
	/// The big that anchors this segment's floor.
	pub anchor: FileId,
	/// Position in the final stitch.
	pub index: usize,
	/// Ordered links building this segment's object.
	pub links: Vec<Link>,
	/// Final byte length of this segment's object (a non-last stitch part — must
	/// be >= 5 MiB; always true since it contains a big).
	pub object_len: u64,
}

/// The assembled plan.
#[derive(Debug, Clone)]
pub struct ChainPlan {
	/// Entries in final archive order.
	pub order: Vec<FileId>,
	pub entries: HashMap<FileId, Entry>,
	/// Segments in final concatenation order.
	pub segments: Vec<Segment>,
	/// Offset where the central directory begins (== total body region).
	pub cd_offset: u64,
	/// Central-directory byte length (computable without CRC).
	pub cd_size: u64,
	/// Every entry needs a phase-1 CRC HEAD (all bodies are copied).
	pub crc_heads: Vec<FileId>,
	pub stats: ChainStats,
}

#[derive(Debug, Clone, Copy)]
pub struct ChainStats {
	pub entries: usize,
	pub segments: usize,
	pub bigs: usize,
	pub smalls: usize,
	/// Total links across all segments (~ MPU count; ~2n - segments).
	pub links: usize,
	/// Deepest single segment chain (the per-segment serial critical path).
	pub max_chain_depth: usize,
}

impl ChainPlan {
	pub fn segment_count(&self) -> usize {
		self.segments.len()
	}
	/// Total final-archive size: body region + central directory + trailer.
	pub fn archive_len(&self) -> u64 {
		self.cd_offset + self.cd_size + 56 + 20 + 22
	}
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlanError {
	/// No big to anchor a segment floor. Impossible on the benchmark data set
	/// (~1,488 bigs); we surface it rather than panic.
	#[error("no big (>=5 MiB) object to anchor segments; {smalls} smalls cannot be placed")]
	NoBigs { smalls: usize },
}

/// Build the segment-chain plan from the listed objects.
pub fn plan_segment_chain(files: Vec<SourceFile>) -> Result<ChainPlan, PlanError> {
	let (mut bigs, mut smalls): (Vec<SourceFile>, Vec<SourceFile>) =
		files.into_iter().partition(|f| f.size >= PART_FLOOR);

	if bigs.is_empty() {
		return Err(PlanError::NoBigs {
			smalls: smalls.len(),
		});
	}

	// Bigs largest-first (ties by id) — deterministic. Smalls by id for a stable
	// round-robin spread.
	bigs.sort_by(|a, b| b.size.cmp(&a.size).then(a.id.0.cmp(&b.id.0)));
	smalls.sort_by(|a, b| a.id.0.cmp(&b.id.0));

	let mut size_of: HashMap<FileId, (String, u64)> = HashMap::new();
	for f in bigs.iter().chain(smalls.iter()) {
		size_of.insert(f.id, (f.name.clone(), f.size));
	}

	// One bucket per big; spread smalls thinly (round-robin) to keep chains
	// shallow. With the ~1:1 benchmark ratio nearly every bucket is [big, small].
	let mut buckets: Vec<Vec<FileId>> = bigs.iter().map(|b| vec![b.id]).collect();
	let n_buckets = buckets.len();
	for (i, s) in smalls.iter().enumerate() {
		buckets[i % n_buckets].push(s.id);
	}

	let n_segments = buckets.len();
	let hdr = |id: &FileId| -> u64 { zip_format::local_header_len(&size_of[id].0) };
	let body = |id: &FileId| -> u64 { size_of[id].1 };
	let plen = |p: &Piece| -> u64 {
		match p {
			Piece::Header(id) => hdr(id),
			Piece::Body(id) => body(id),
		}
	};

	let mut order: Vec<FileId> = Vec::new();
	let mut segments: Vec<Segment> = Vec::with_capacity(n_segments);
	let mut offset: u64 = 0;
	let mut total_links = 0usize;
	let mut max_depth = 0usize;

	for (k, bucket) in buckets.iter().enumerate() {
		let anchor = bucket[0];
		let seg_smalls = &bucket[1..];
		let next_big: Option<FileId> = if k + 1 < n_segments {
			Some(buckets[k + 1][0])
		} else {
			None
		};

		// Logical entry order within the segment: big, then its smalls. (The next
		// big is an entry of the NEXT segment, so it is not pushed here even
		// though its header rides this object's tail.)
		order.push(anchor);
		offset += hdr(&anchor) + body(&anchor);
		for s in seg_smalls {
			order.push(*s);
			offset += hdr(s) + body(s);
		}

		// Pieces appended after the anchor body is established.
		let mut pieces: Vec<Piece> = Vec::with_capacity(seg_smalls.len() * 2 + 1);
		for s in seg_smalls {
			pieces.push(Piece::Header(*s));
			pieces.push(Piece::Body(*s));
		}
		if let Some(nb) = next_big {
			pieces.push(Piece::Header(nb)); // cross-boundary trailing header
		}

		// Links + object length.
		let mut links: Vec<Link> = Vec::new();
		let mut object_len: u64 = if k == 0 {
			hdr(&anchor) + body(&anchor) // bootstrap includes big0's own header
		} else {
			body(&anchor) // interior/last: big's header is in the previous object
		};

		if k == 0 {
			let steal_len = PART_FLOOR.saturating_sub(hdr(&anchor)); // part1 == 5 MiB
			links.push(Link::Bootstrap { anchor, steal_len });
			for p in pieces {
				object_len += plen(&p);
				links.push(Link::ForwardThenAppend { piece: p });
			}
		} else if pieces.is_empty() {
			links.push(Link::AnchorOnly { anchor }); // last segment, solo big
		} else {
			let mut it = pieces.into_iter();
			let first = it.next().unwrap();
			object_len += plen(&first);
			links.push(Link::AnchorThenAppend {
				anchor,
				piece: first,
			});
			for p in it {
				object_len += plen(&p);
				links.push(Link::ForwardThenAppend { piece: p });
			}
		}

		debug_assert!(
			object_len >= PART_FLOOR,
			"segment {k} object {object_len} < 5 MiB floor (must contain a big)"
		);

		total_links += links.len();
		max_depth = max_depth.max(links.len());
		segments.push(Segment {
			anchor,
			index: k,
			links,
			object_len,
		});
	}

	let cd_offset = offset;

	// Offsets -> entries (crc filled later). CD size is computable now (name +
	// offset, not crc); CD bytes are built at assemble time.
	let offsets = compute_offsets(&order, &size_of);
	let mut entries: HashMap<FileId, Entry> = HashMap::new();
	let mut cd_size: u64 = 0;
	for (i, id) in order.iter().enumerate() {
		let (name, size) = size_of[id].clone();
		cd_size += zip_format::central_dir_entry_len(&name, offsets[i]);
		entries.insert(
			*id,
			Entry {
				id: *id,
				name,
				size,
				local_header_offset: offsets[i],
				crc: None,
			},
		);
	}

	// Cross-check: segment objects tile the body region exactly. Each interior
	// object carries the next big's header, which pairs with that big's body in
	// the following object, so the sum telescopes back to cd_offset.
	debug_assert_eq!(
		segments.iter().map(|s| s.object_len).sum::<u64>(),
		cd_offset,
		"segment objects must tile the body region exactly"
	);

	let stats = ChainStats {
		entries: order.len(),
		segments: n_segments,
		bigs: bigs.len(),
		smalls: smalls.len(),
		links: total_links,
		max_chain_depth: max_depth,
	};

	let crc_heads: Vec<FileId> = order.clone();

	Ok(ChainPlan {
		order,
		entries,
		segments,
		cd_offset,
		cd_size,
		crc_heads,
		stats,
	})
}

/// The single place offset arithmetic lives: local-header offset of each entry in
/// archive order, accumulating `local_header_len + body` per entry. Link and part
/// boundaries are irrelevant — the archive is the logical `[lfh][body]…` stream.
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
	#[cfg(feature = "zip_validate")]
	use figment_engine::engine::zip_format::EntryMeta;

	fn sf(id: u32, name: &str, size: u64) -> SourceFile {
		SourceFile {
			id: FileId(id),
			key: format!("files/{name}"),
			name: name.to_string(),
			size,
		}
	}

	// ---------- pure structural tests (no zip dependency) ----------

	#[test]
	fn rejects_zero_bigs() {
		let files = vec![sf(0, "s1", 1024), sf(1, "s2", 2048)];
		assert!(matches!(
			plan_segment_chain(files),
			Err(PlanError::NoBigs { smalls: 2 })
		));
	}

	#[test]
	fn offsets_contiguous_and_cd_follows() {
		let files = vec![
			sf(0, "big1", PART_FLOOR + 1000),
			sf(1, "big2", PART_FLOOR + 2000),
			sf(2, "s1", 1000),
			sf(3, "s2", 2000),
			sf(4, "s3", 3000),
		];
		let plan = plan_segment_chain(files).unwrap();
		let mut expect = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			assert_eq!(e.local_header_offset, expect, "offset mismatch {}", e.name);
			expect += zip_format::entry_total_len(&e.name, e.size);
		}
		assert_eq!(plan.cd_offset, expect, "cd_offset must follow last body");
	}

	#[test]
	fn segment_objects_tile_body_region() {
		let files = vec![
			sf(0, "big1", PART_FLOOR + 10),
			sf(1, "big2", PART_FLOOR + 20),
			sf(2, "big3", PART_FLOOR + 30),
			sf(3, "a", 100),
			sf(4, "b", 200),
			sf(5, "c", 300),
			sf(6, "d", 400),
		];
		let plan = plan_segment_chain(files).unwrap();
		let sum: u64 = plan.segments.iter().map(|s| s.object_len).sum();
		assert_eq!(sum, plan.cd_offset, "objects must tile the body region");
		for s in &plan.segments {
			assert!(s.object_len >= PART_FLOOR, "segment under 5 MiB floor");
		}
	}

	#[test]
	fn one_segment_per_big_smalls_spread_thin() {
		let files = vec![
			sf(0, "big1", PART_FLOOR),
			sf(1, "big2", PART_FLOOR),
			sf(2, "s1", 10),
			sf(3, "s2", 20),
			sf(4, "s3", 30),
		];
		let plan = plan_segment_chain(files).unwrap();
		assert_eq!(plan.segment_count(), 2);
		let mut small_counts: Vec<usize> = plan
			.segments
			.iter()
			.map(|s| {
				s.links
					.iter()
					.filter(|l| {
						matches!(
							l,
							Link::ForwardThenAppend {
								piece: Piece::Body(_)
							}
						) || matches!(
							l,
							Link::AnchorThenAppend {
								piece: Piece::Body(_),
								..
							}
						)
					})
					.count()
			})
			.collect();
		small_counts.sort();
		assert_eq!(small_counts, vec![1, 2]);
	}

	#[test]
	fn first_segment_bootstraps_only_once() {
		let files = vec![
			sf(0, "big1", PART_FLOOR + 1),
			sf(1, "big2", PART_FLOOR + 2),
			sf(2, "big3", PART_FLOOR + 3),
			sf(3, "s", 50),
		];
		let plan = plan_segment_chain(files).unwrap();
		let boots: usize = plan
			.segments
			.iter()
			.flat_map(|s| &s.links)
			.filter(|l| matches!(l, Link::Bootstrap { .. }))
			.count();
		assert_eq!(boots, 1, "exactly one bootstrap (entry 0)");
		assert!(matches!(plan.segments[0].links[0], Link::Bootstrap { .. }));
	}

	#[test]
	fn last_segment_has_no_trailing_header() {
		let files = vec![
			sf(0, "big1", PART_FLOOR + 1),
			sf(1, "big2", PART_FLOOR + 2),
			sf(2, "s1", 10),
			sf(3, "s2", 20),
		];
		let plan = plan_segment_chain(files).unwrap();
		let anchors: std::collections::HashSet<FileId> =
			plan.segments.iter().map(|s| s.anchor).collect();
		let last = plan.segments.last().unwrap();
		for l in &last.links {
			if let Link::ForwardThenAppend {
				piece: Piece::Header(id),
			}
			| Link::AnchorThenAppend {
				piece: Piece::Header(id),
				..
			} = l
			{
				assert!(
					!anchors.contains(id),
					"last segment carries a trailing big header"
				);
			}
		}
	}

	#[test]
	fn solo_big_single_entry_archive() {
		let files = vec![sf(0, "only", PART_FLOOR + 123)];
		let plan = plan_segment_chain(files).unwrap();
		assert_eq!(plan.segment_count(), 1);
		assert_eq!(plan.segments[0].links.len(), 1);
		assert!(matches!(plan.segments[0].links[0], Link::Bootstrap { .. }));
		assert_eq!(plan.order, vec![FileId(0)]);
	}

	// ---------- full executor simulation + real zip round-trip ----------
	// Run with: cargo test -p figment-engine-chain --features zip_validate

	#[cfg(feature = "zip_validate")]
	fn crc32(data: &[u8]) -> u32 {
		let mut crc: u32 = 0xFFFF_FFFF;
		for &b in data {
			crc ^= b as u32;
			for _ in 0..8 {
				let mask = (crc & 1).wrapping_neg();
				crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
			}
		}
		!crc
	}

	#[cfg(feature = "zip_validate")]
	fn sha256_hex(b: &[u8]) -> String {
		use core::fmt::Write;
		use sha2::{Digest, Sha256};
		let d = Sha256::new().chain_update(b).finalize();
		let mut s = String::with_capacity(64);
		for x in d {
			let _ = write!(s, "{:02x}", x);
		}
		s
	}

	#[cfg(feature = "zip_validate")]
	fn files_from_raw(raw: &[Vec<u8>]) -> Vec<SourceFile> {
		raw.iter()
			.enumerate()
			.map(|(i, b)| {
				let name = sha256_hex(b);
				SourceFile {
					id: FileId(i as u32),
					key: format!("files/{name}"),
					name,
					size: b.len() as u64,
				}
			})
			.collect()
	}

	#[cfg(feature = "zip_validate")]
	fn fill_crcs(plan: &mut ChainPlan, raw: &[Vec<u8>]) {
		for id in plan.order.clone() {
			let crc = crc32(&raw[id.0 as usize]);
			plan.entries.get_mut(&id).unwrap().crc = Some(crc);
		}
	}

	/// Simulate the chain executor in memory: build each segment object link by
	/// link (asserting every non-last part clears the floor), stitch the objects,
	/// append the central directory as the exempt last part, verify planner
	/// offsets against where headers landed, and round-trip the real zip reader.
	#[cfg(feature = "zip_validate")]
	fn assemble_and_validate(plan: &ChainPlan, raw: &[Vec<u8>]) {
		use std::io::{Cursor, Read};

		let meta = |id: FileId| -> EntryMeta {
			let e = &plan.entries[&id];
			EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc: e.crc.expect("crc filled before assemble"),
				local_header_offset: e.local_header_offset,
			}
		};
		let append_piece = |obj: &mut Vec<u8>, piece: &Piece| match piece {
			Piece::Header(id) => obj.extend_from_slice(&zip_format::local_header(&meta(*id))),
			Piece::Body(id) => obj.extend_from_slice(&raw[id.0 as usize]),
		};

		let mut objects: Vec<Vec<u8>> = Vec::with_capacity(plan.segments.len());
		for seg in &plan.segments {
			let mut obj: Vec<u8> = Vec::new();
			for link in &seg.links {
				match link {
					Link::Bootstrap { anchor, steal_len } => {
						let h = zip_format::local_header(&meta(*anchor));
						let part1 = h.len() as u64 + *steal_len;
						assert!(part1 >= PART_FLOOR, "bootstrap part1 {part1} < floor");
						assert!(
							*steal_len <= raw[anchor.0 as usize].len() as u64,
							"steal_len exceeds big0 body"
						);
						obj.extend_from_slice(&h);
						obj.extend_from_slice(&raw[anchor.0 as usize]);
					}
					Link::AnchorOnly { anchor } => {
						let b = &raw[anchor.0 as usize];
						assert!(b.len() as u64 >= PART_FLOOR, "AnchorOnly part < floor");
						obj.extend_from_slice(b);
					}
					Link::AnchorThenAppend { anchor, piece } => {
						let b = &raw[anchor.0 as usize];
						assert!(
							b.len() as u64 >= PART_FLOOR,
							"AnchorThenAppend copy(big) < floor"
						);
						obj.extend_from_slice(b);
						append_piece(&mut obj, piece);
					}
					Link::ForwardThenAppend { piece } => {
						assert!(
							obj.len() as u64 >= PART_FLOOR,
							"ForwardThenAppend copy-forward {} < floor",
							obj.len()
						);
						append_piece(&mut obj, piece);
					}
				}
			}
			assert_eq!(
				obj.len() as u64,
				seg.object_len,
				"segment {} len mismatch",
				seg.index
			);
			objects.push(obj);
		}

		let mut archive: Vec<u8> = Vec::new();
		for (i, obj) in objects.iter().enumerate() {
			assert!(
				obj.len() as u64 >= PART_FLOOR,
				"stitch part {i} below floor"
			);
			archive.extend_from_slice(obj);
		}
		assert_eq!(
			archive.len() as u64,
			plan.cd_offset,
			"body region != cd_offset"
		);

		let mut cd_size = 0u64;
		for id in &plan.order {
			let rec = zip_format::central_dir_entry(&meta(*id));
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		assert_eq!(cd_size, plan.cd_size, "cd_size mismatch vs planner");
		archive.extend_from_slice(&zip_format::end_records(
			plan.order.len() as u64,
			plan.cd_offset,
			cd_size,
		));
		assert_eq!(
			archive.len() as u64,
			plan.archive_len(),
			"archive_len mismatch"
		);

		for id in &plan.order {
			let off = plan.entries[id].local_header_offset as usize;
			assert_eq!(
				&archive[off..off + 4],
				&[0x50, 0x4b, 0x03, 0x04],
				"no LFH signature at planned offset for {}",
				plan.entries[id].name
			);
		}

		let mut expected: std::collections::HashSet<String> = plan
			.order
			.iter()
			.map(|id| plan.entries[id].name.clone())
			.collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("segment-chain archive must parse with the standard zip reader");
		assert_eq!(za.len(), plan.order.len());
		for n in za.file_names().map(ToOwned::to_owned).collect::<Vec<_>>() {
			assert!(!n.contains('/'), "flat layout required");
			assert!(expected.remove(&n), "unknown/duplicate entry {n}");
			let mut e = za.by_name(&n).unwrap();
			let mut buf = Vec::new();
			e.read_to_end(&mut buf)
				.expect("extract (CRC verified by reader)");
			assert_eq!(sha256_hex(&buf), n, "content hash must equal entry name");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
	}

	#[cfg(feature = "zip_validate")]
	fn run_case(raw: Vec<Vec<u8>>) {
		let mut plan = plan_segment_chain(files_from_raw(&raw)).unwrap();
		fill_crcs(&mut plan, &raw);
		assemble_and_validate(&plan, &raw);
	}

	#[cfg(feature = "zip_validate")]
	#[test]
	fn validate_one_to_one_bigs_and_smalls() {
		let big = PART_FLOOR as usize + 4096;
		let small = 4096usize;
		run_case(vec![
			vec![1u8; big],
			vec![2u8; small],
			vec![3u8; big],
			vec![4u8; small],
			vec![5u8; big],
			vec![6u8; small],
		]);
	}

	#[cfg(feature = "zip_validate")]
	#[test]
	fn validate_spare_smalls_fold_in() {
		let big = PART_FLOOR as usize + 10;
		let s = 1234usize;
		run_case(vec![
			vec![1u8; big],
			vec![2u8; big],
			vec![10u8; s],
			vec![11u8; s],
			vec![12u8; s],
			vec![13u8; s],
			vec![14u8; s],
		]);
	}

	#[cfg(feature = "zip_validate")]
	#[test]
	fn validate_lone_bigs_no_smalls() {
		run_case(vec![
			vec![1u8; PART_FLOOR as usize + 1],
			vec![2u8; PART_FLOOR as usize + 2],
			vec![3u8; PART_FLOOR as usize + 3],
		]);
	}

	#[cfg(feature = "zip_validate")]
	#[test]
	fn validate_single_big_only() {
		run_case(vec![vec![7u8; PART_FLOOR as usize + 999]]);
	}

	#[cfg(feature = "zip_validate")]
	#[test]
	fn validate_big_exactly_at_floor() {
		run_case(vec![
			vec![1u8; PART_FLOOR as usize],
			vec![2u8; 2048],
			vec![3u8; PART_FLOOR as usize + 5],
			vec![4u8; 2048],
		]);
	}
}
