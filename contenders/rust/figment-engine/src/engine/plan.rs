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
	#[allow(dead_code)] // used in tests
	pub key: String,
	pub name: String, // ZIP entry name (key minus prefix)
	pub size: u64,
}

/// One archive entry, in canonical order. Offsets are computed by the planner; `crc` is
/// filled later (None until then).
#[derive(Debug, Clone)]
pub struct Entry {
	#[allow(dead_code)] // used in tests
	pub id: FileId,
	pub name: String,
	pub size: u64,
	pub local_header_offset: u64,
	pub crc: Option<u32>,
	#[allow(dead_code)] // used in tests
	pub streamed: bool, // true: header written inline by a Stream part; false: COPIED body
}

/// A piece of a Stream part.
#[derive(Debug, Clone)]
pub enum Segment {
	/// A streamed file: GET it, emit its (inline) header + body. CRC self-computed from body.
	StreamedFile { id: FileId },
	/// A standalone local header for a COPIED file (the trailing-header handoff). No body here.
	CopiedFileHeader { id: FileId },
	/// The first `len` body bytes of a big, streamed (ranged GET) immediately after that big's
	/// `CopiedFileHeader`, to lift this stream part over the 5 MiB floor. The remainder of the
	/// big's body (`[len..]`) is moved server-side by a following ranged `Copy`. Physically
	/// contiguous in part-number order, so the big reads back as one `[header][body]`.
	StreamedBigPrefix { id: FileId, len: u64 },
	/// The central directory + ZIP64 end records. Always the LAST segment of the LAST part, so the
	/// directory rides inside the final MPU part rather than being a separate trailing part. This
	/// makes that final part the genuine last part (floor-exempt), so leftover sub-floor smalls
	/// placed alongside it need not independently clear the 5 MiB floor. Expanded by the assembler
	/// (it needs every entry's offset + CRC), so it carries no bytes in the plan itself.
	CentralDirectory,
}

/// One MPU part within a chain.
#[derive(Debug, Clone)]
pub enum PartSpec {
	/// Big body moved server-side. Off-ENI. `copy_from` is the byte offset into the source object
	/// where the copy starts (0 = whole body; >0 = the `[copy_from..]` remainder after a
	/// `StreamedBigPrefix` streamed the first `copy_from` bytes). Copies the source to its end.
	Copy {
		part_number: u32,
		id: FileId,
		copy_from: u64,
	},
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
	#[allow(dead_code)] // used in tests
	pub copyable: Vec<FileId>, // bigs needing a phase-1 CRC HEAD
	pub stats: PlanStats,
}

/// Planner decision summary, for logging on the assembler side (the engine stays tracing-free).
#[derive(Debug, Clone, Copy)]
pub struct PlanStats {
	pub entries: usize,
	pub parts: usize,
	pub copy_parts: usize, // bigs moved server-side (off-ENI), incl. ranged remainders
	pub stream_parts: usize, // batched stream parts
	pub folded_bigs: usize, // bigs forced to STREAM whole (on-ENI) — smalls gone & too small to donate
	pub stolen_bigs: usize, // bigs copied via ranged remainder after donating a floor-bridging prefix
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
	// Bigs LARGEST-FIRST: forced folds/floor-huggers land on the SMALLEST bigs (cheapest to
	// stream). Smalls SMALLEST-FIRST: paired against the biggest bigs, which can donate the most
	// toward the floor — so a single small + a small steal bridges a part, stretching the small
	// budget across as many bigs as possible. Ties by id for determinism.
	bigs.sort_by(|a, b| b.size.cmp(&a.size).then(a.id.0.cmp(&b.id.0)));
	smalls.sort_by(|a, b| a.size.cmp(&b.size).then(a.id.0.cmp(&b.id.0)));

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

	// A running stream batch (segments) we flush into a Stream part when it clears the floor.
	let mut cur_segs: Vec<Segment> = Vec::new();
	let mut cur_bytes: u64 = 0;
	// Bigs STREAMED whole (folded) because smalls ran out AND the big was too small to self-donate
	// a floor-bridging prefix. These do NOT need a phase-1 CRC HEAD beyond what every entry gets.
	let mut streamed_bigs: std::collections::HashSet<FileId> = std::collections::HashSet::new();
	let mut stolen_count: usize = 0;

	for big in bigs.iter() {
		let header_big = zip_format::local_header_len(&size_of[&big.id].0);
		let max_steal = big.size.saturating_sub(PART_FLOOR); // keep ranged remainder >= floor

		// Pull smalls one at a time, but only until the remaining gap to the floor is small enough
		// to bridge with a steal from THIS big. This spends the minimum smalls per chaperone part,
		// stretching the small budget across as many bigs as possible. A floor-hugging big
		// (max_steal ~0) can't bridge, so we keep pulling smalls until the batch clears outright.
		// Smalls must cross the ENI regardless, so we consume at least one (when available) before
		// resorting to a steal — otherwise a huge big would self-steal a full prefix while smalls
		// sit unused and get dumped (wastefully) into the final part.
		let mut pulled_small = false;
		loop {
			let with_header = cur_bytes + header_big;
			if with_header >= PART_FLOOR {
				break; // header alone clears the floor (K = 0, full copy)
			}
			let gap = PART_FLOOR - with_header;
			let smalls_left = small_iter.peek().is_some();
			if gap <= max_steal && (pulled_small || !smalls_left) {
				break; // bridgeable by a steal: use an available small first, else steal bare
			}
			if smalls_left {
				let sid = small_iter.next().unwrap().id;
				cur_segs.push(Segment::StreamedFile { id: sid });
				order.push(sid);
				let (ref name, sz) = size_of[&sid];
				cur_bytes += zip_format::local_header_len(name) + sz;
				pulled_small = true;
			} else {
				break; // smalls exhausted; decide steal-vs-fold below
			}
		}

		let with_header = cur_bytes + header_big;
		// A small that must cross the ENI anyway is never wasted: when one is available we consume
		// it into this part first and steal only the RESIDUAL gap. Once smalls are exhausted we
		// steal bare to keep a donatable big off the ENI; a big too small to bridge then folds.
		let smalls_left = small_iter.peek().is_some();
		let k = if pulled_small || !smalls_left {
			PART_FLOOR.saturating_sub(with_header).min(max_steal)
		} else {
			0
		};

		if with_header + k >= PART_FLOOR {
			// The stream part (batch + big header + optional stolen prefix) clears the floor.
			// Ride the big's header on the tail; if k>0, also stream its first k body bytes; then
			// COPY the remainder server-side (ranged when k>0, full when k==0).
			cur_segs.push(Segment::CopiedFileHeader { id: big.id });
			if k > 0 {
				cur_segs.push(Segment::StreamedBigPrefix { id: big.id, len: k });
				stolen_count += 1;
			}
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
				copy_from: k,
			});
			part_number += 1;
		} else {
			// Smalls exhausted and the big can't donate enough to clear the floor (floor-hugger).
			// FOLD it: stream [hBig][big-body] inline — the big alone exceeds the floor, so valid.
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

	// The FINAL part: any leftover smalls, then the central directory as the last segment. Because
	// the directory rides here, THIS is the last MPU part (floor-exempt) — so sub-floor leftover
	// smalls need not clear the floor. (cur_segs is empty here: the loop flushes its batch into a
	// Stream part on every big, so nothing is left mid-batch.)
	for s in small_iter {
		cur_segs.push(Segment::StreamedFile { id: s.id });
		order.push(s.id);
	}
	cur_segs.push(Segment::CentralDirectory);
	parts.push(PartSpec::Stream {
		part_number,
		segments: std::mem::take(&mut cur_segs),
	});

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
		stolen_bigs: stolen_count,
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
	// Single-MPU alternating copy / stream-batch / steal layout. Each test builds the archive
	// straight from the part list (exactly as the assembler would) via `assemble_and_validate`,
	// then validates with the real zip reader. One MPU, no chains, no H blob.
	// Run with: cargo test -p figment-engine --features zip_validate single_mpu
	// ===================================================================================

	#[cfg(feature = "zip_validate")]
	fn crc32(b: &[u8]) -> u32 {
		let mut h = crc32fast::Hasher::new();
		h.update(b);
		h.finalize()
	}

	#[cfg(feature = "zip_validate")]
	fn sha256_hex(b: &[u8]) -> String {
		use sha2::{Digest, Sha256};
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

	// Build SourceFiles from raw blobs. Each file's NAME is the sha256 of its content, so the zip
	// reader's extracted-content hash must equal the entry name — a self-checking fixture.
	#[cfg(feature = "zip_validate")]
	fn files_from_raw(raw: &[Vec<u8>]) -> Vec<SourceFile> {
		(0..raw.len())
			.map(|i| {
				let name = sha256_hex(&raw[i]);
				SourceFile {
					id: FileId(i as u32),
					key: format!("files/{name}"),
					name,
					size: raw[i].len() as u64,
				}
			})
			.collect()
	}

	#[cfg(feature = "zip_validate")]
	fn plan_or_panic(raw: &[Vec<u8>]) -> SinglePlan {
		match plan_single_mpu(files_from_raw(raw)) {
			SingleRouting::SingleMpu(p) => p,
			SingleRouting::Fallback => panic!("expected single-MPU fast path"),
		}
	}

	// Phase-1 stand-in: fill CRCs for the copyable (server-side-copied) bigs from raw content.
	// Streamed entries (smalls, folded bigs) self-compute their CRC in the harness below.
	#[cfg(feature = "zip_validate")]
	fn fill_crcs_from_raw(plan: &mut SinglePlan, raw: &[Vec<u8>]) {
		for id in plan.copyable.clone() {
			if let Some(e) = plan.entries.get_mut(&id) {
				e.crc = Some(crc32(&raw[id.0 as usize]));
			}
		}
	}

	// Realise the plan's parts into archive bytes exactly as the assembler does, expanding the
	// CentralDirectory segment inline. Asserts: every non-last part clears the 5 MiB floor; the
	// directory is the LAST segment of the LAST part; the entry region ends exactly at cd_offset;
	// and the result parses + extracts cleanly under the standard zip reader (CRCs verified).
	#[cfg(feature = "zip_validate")]
	fn assemble_and_validate(plan: &SinglePlan, raw: &[Vec<u8>]) {
		use crate::engine::zip_format::{self, EntryMeta};
		use std::collections::HashSet;
		use std::io::{Cursor, Read};

		let meta = |id: FileId| -> EntryMeta {
			let e = &plan.entries[&id];
			let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
			EntryMeta {
				name: e.name.clone(),
				size: e.size,
				crc,
				local_header_offset: e.local_header_offset,
			}
		};

		let mut cd_offset = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			cd_offset += zip_format::entry_total_len(&e.name, e.size);
		}
		let build_directory = || -> Vec<u8> {
			let mut out = Vec::new();
			let mut cd_size = 0u64;
			for id in &plan.order {
				let rec = zip_format::central_dir_entry(&meta(*id));
				cd_size += rec.len() as u64;
				out.extend_from_slice(&rec);
			}
			out.extend_from_slice(&zip_format::end_records(
				plan.order.len() as u64,
				cd_offset,
				cd_size,
			));
			out
		};

		let mut archive: Vec<u8> = Vec::new();
		let mut saw_directory = false;
		let nparts = plan.parts.len();
		for (pi, part) in plan.parts.iter().enumerate() {
			let before = archive.len();
			match part {
				PartSpec::Copy { id, copy_from, .. } => {
					archive.extend_from_slice(&raw[id.0 as usize][*copy_from as usize..]);
				}
				PartSpec::Stream { segments, .. } => {
					for seg in segments {
						match seg {
							Segment::StreamedFile { id } => {
								archive.extend_from_slice(&zip_format::local_header(&meta(*id)));
								archive.extend_from_slice(&raw[id.0 as usize]);
							}
							Segment::CopiedFileHeader { id } => {
								archive.extend_from_slice(&zip_format::local_header(&meta(*id)));
							}
							Segment::StreamedBigPrefix { id, len } => {
								archive.extend_from_slice(&raw[id.0 as usize][..*len as usize]);
							}
							Segment::CentralDirectory => {
								assert!(
									pi + 1 == nparts,
									"CentralDirectory must be in the last part, found in part {}",
									pi + 1
								);
								assert_eq!(
									archive.len() as u64,
									cd_offset,
									"entry region ({}) != cd_offset ({}) at directory point",
									archive.len(),
									cd_offset
								);
								archive.extend_from_slice(&build_directory());
								saw_directory = true;
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
		assert!(saw_directory, "plan emitted no CentralDirectory segment");

		let mut expected: HashSet<String> = plan
			.order
			.iter()
			.map(|id| plan.entries[id].name.clone())
			.collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("single-MPU archive must parse with the standard zip reader");
		assert_eq!(za.len(), plan.order.len());
		for n in za.file_names().map(ToOwned::to_owned).collect::<Vec<_>>() {
			assert!(!n.contains('/'), "flat layout required");
			assert!(expected.remove(&n), "unknown/duplicate {n}");
			let mut entry = za.by_name(&n).unwrap();
			let mut buf = Vec::new();
			entry
				.read_to_end(&mut buf)
				.expect("extract (CRC verified by reader)");
			assert_eq!(&sha256_hex(&buf), &n, "content hash == name");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
	}

	// Is `id` a small (sub-floor source file)? Used by the steal-invariant scan.
	#[cfg(feature = "zip_validate")]
	fn is_small(plan: &SinglePlan, id: FileId) -> bool {
		plan.entries[&id].size < PART_FLOOR
	}

	// ---- Basic layout: smalls batch 2-per-part to clear the floor (no steal needed). One odd
	// leftover small must end up in the final part WITH the directory. ----
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_layout_matches_directory_offsets() {
		let big = PART_FLOOR as usize + 77;
		let small = (PART_FLOOR as usize / 2) + 1000; // two smalls > floor; one alone < floor
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
			vec![10u8; small], // odd leftover small -> rides with the directory in the final part
		];
		let mut plan = plan_or_panic(&raw);
		// These bigs are floor-huggers (max_steal = 77 B), so they cannot self-donate; the batch
		// clears the floor with two smalls and the big is copied whole. No steal expected.
		assert_eq!(
			plan.stats.stolen_bigs, 0,
			"floor-hugger bigs should not steal"
		);
		assert_eq!(
			plan.stats.folded_bigs, 0,
			"two smalls clear the floor; nothing folds"
		);
		fill_crcs_from_raw(&mut plan, &raw);
		assemble_and_validate(&plan, &raw);
	}

	// ---- Smalls run out while bigs remain: with no small to ride, the remaining bigs FOLD
	// (streamed whole) rather than steal — they must never produce an undersized non-last part. ----
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_handles_smalls_exhausted_early() {
		// Floor-hugger bigs (cannot donate) + a single small. The first big folds carrying the
		// small; the rest fold bare. All folds are >= floor (big body alone exceeds the floor).
		let big = PART_FLOOR as usize + 100;
		let small = PART_FLOOR as usize / 2;
		let raw: Vec<Vec<u8>> = vec![
			vec![1u8; big],
			vec![2u8; big],
			vec![3u8; big],
			vec![4u8; big],
			vec![5u8; small],
		];
		let mut plan = plan_or_panic(&raw);
		assert!(
			plan.stats.folded_bigs > 0,
			"expected folds when smalls are exhausted"
		);
		fill_crcs_from_raw(&mut plan, &raw);
		assemble_and_validate(&plan, &raw);
	}

	// ---- Steal: a small alone is below the floor, but each big can donate a prefix to bridge it,
	// so every big is COPIED (ranged remainder), none folded. ----
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_steals_big_prefix_to_clear_floor() {
		let big = (PART_FLOOR + PART_FLOOR / 2) as usize; // max_steal = 0.5 floor
		let small = (PART_FLOOR * 3 / 5) as usize; // 0.6 floor: one small alone < floor
		let raw: Vec<Vec<u8>> = vec![
			vec![1u8; big],
			vec![2u8; small],
			vec![3u8; big],
			vec![4u8; small],
			vec![5u8; big],
			vec![6u8; small],
			vec![7u8; big],
			vec![8u8; small],
		];
		let mut plan = plan_or_panic(&raw);
		assert_eq!(
			plan.stats.folded_bigs, 0,
			"no big should fold when all can donate a prefix"
		);
		assert!(
			plan.stats.stolen_bigs > 0,
			"expected ranged-copy steals, got none"
		);
		assert!(
			plan.parts
				.iter()
				.any(|p| matches!(p, PartSpec::Copy { copy_from, .. } if *copy_from > 0)),
			"expected at least one ranged copy (copy_from > 0)"
		);
		fill_crcs_from_raw(&mut plan, &raw);
		assemble_and_validate(&plan, &raw);
	}

	// ---- INVARIANT (clarified): an AVAILABLE small is never wasted by stealing in its place. A
	// small must cross the ENI regardless, so while smalls remain, the bridging part consumes one
	// and the steal covers only the residual gap — never a bare `[hBig][prefix]` that strands a
	// usable small. (Bare steals are allowed only once smalls are exhausted; the steal and
	// smalls-exhausted tests cover that.) Here smalls outnumber bigs, so none run out mid-loop and
	// every steal part must carry a small; a greedy bare-steal would fail the scan. ----
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_uses_available_small_before_stealing() {
		let big = (PART_FLOOR + PART_FLOOR / 2) as usize; // donatable (max_steal = 0.5 floor)
		let small = (PART_FLOOR * 3 / 5) as usize; // 0.6 floor: one alone < floor
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
			vec![10u8; big],
			vec![11u8; small],
			vec![12u8; small],
		];
		let plan = plan_or_panic(&raw);

		assert!(
			plan.stats.stolen_bigs >= 1,
			"expected steals on donatable bigs"
		);
		assert_eq!(
			plan.stats.folded_bigs, 0,
			"donatable bigs must not fold while smalls are available"
		);

		// Every steal part must also stream a small (consumed before the steal). With smalls
		// plentiful none run out, so a bare steal here would mean a wasted available small.
		for part in &plan.parts {
			if let PartSpec::Stream { segments, .. } = part {
				let has_prefix = segments
					.iter()
					.any(|s| matches!(s, Segment::StreamedBigPrefix { .. }));
				if has_prefix {
					let has_small = segments
						.iter()
						.any(|s| matches!(s, Segment::StreamedFile { id } if is_small(&plan, *id)));
					assert!(
						has_small,
						"a steal part carries no small, but smalls were available — wasted small"
					);
				}
			}
		}

		let mut plan = plan;
		fill_crcs_from_raw(&mut plan, &raw);
		assemble_and_validate(&plan, &raw);
	}

	// ---- INVARIANT (requested): trailing leftovers ride WITH the directory. A sub-floor odd
	// leftover small must not form a standalone (undersized, non-last) part — it must sit in the
	// final part alongside the CentralDirectory, which is the genuine last (floor-exempt) part. ----
	#[cfg(feature = "zip_validate")]
	#[test]
	fn single_mpu_trailing_leftovers_ride_with_directory() {
		let big = PART_FLOOR as usize + 77; // floor-hugger: cleared by two smalls, copied whole
		let small = (PART_FLOOR as usize / 2) + 1000; // two > floor, one < floor
		// Sized past the viability floor (4 x PART_FLOOR). Three bigs each consume two smalls; the
		// 7th small (id 9) is the odd leftover that must ride with the directory.
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
			vec![10u8; small], // odd leftover small -> must ride with the directory
		];
		let plan = plan_or_panic(&raw);

		// Exactly one CentralDirectory, and it is the LAST segment of the LAST part.
		let dir_count: usize = plan
			.parts
			.iter()
			.map(|p| match p {
				PartSpec::Stream { segments, .. } => segments
					.iter()
					.filter(|s| matches!(s, Segment::CentralDirectory))
					.count(),
				_ => 0,
			})
			.sum();
		assert_eq!(
			dir_count, 1,
			"exactly one CentralDirectory segment expected"
		);

		let last = plan.parts.last().expect("at least one part");
		let last_segs = match last {
			PartSpec::Stream { segments, .. } => segments,
			_ => panic!("final part must be a Stream part carrying the directory"),
		};
		assert!(
			matches!(last_segs.last(), Some(Segment::CentralDirectory)),
			"directory must be the final segment of the final part"
		);

		// The odd leftover small (id 9) must live in that final part, not in a standalone part.
		let leftover = FileId(9);
		let in_final = last_segs
			.iter()
			.any(|s| matches!(s, Segment::StreamedFile { id } if *id == leftover));
		assert!(
			in_final,
			"leftover small must ride in the final (directory) part"
		);

		let mut plan = plan;
		fill_crcs_from_raw(&mut plan, &raw);
		// assemble_and_validate also asserts every non-last part >= floor, so a mis-placed
		// sub-floor leftover would fail here.
		assemble_and_validate(&plan, &raw);
	}
}
