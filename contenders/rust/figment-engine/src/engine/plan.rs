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

/// A chain = its own MPU. First chain: all Stream parts (bin-packed; bootstraps entry-0 header).
#[derive(Debug, Clone)]
pub struct Chain {
	pub parts: Vec<PartSpec>,
	pub final_merge_part_number: u32,
}

/// A normal chain's layered all-copy recipe. Realised by the assembler as 1-3 sub-MPUs, each
/// copying the previous layer plus one floor-exempt last part, so NO body crosses the ENI:
///   layer1: [copy big][copy header(small) from H]        -> [B][hS]      (skip hS if no small)
///   layer2: [copy layer1][copy body(small) from files/]  -> [B][hS][S]   (skip if no small)
///   layer3: [copy prev][copy header(next_big) from H]    -> [..][hNext]  (skip if no next)
/// The big's own header rides the PREVIOUS chain's tail (next_big_header), bootstrapped by the
/// first chain. Archive fragment = [B][hS][S][hNext], identical bytes to the streamed layout.
#[derive(Debug, Clone)]
pub struct LayeredChain {
	pub big: FileId,
	pub small: Option<FileId>,
	pub next_big_header: Option<FileId>,
	pub final_merge_part_number: u32,
}

/// A chain in the plan: the first chain streams (bootstraps entry-0 header); every other chain
/// is a layered all-copy recipe.
#[derive(Debug, Clone)]
pub enum ChainPlan {
	FirstStream(Chain),
	Layered(LayeredChain),
}

impl ChainPlan {
	pub fn final_merge_part_number(&self) -> u32 {
		match self {
			ChainPlan::FirstStream(c) => c.final_merge_part_number,
			ChainPlan::Layered(l) => l.final_merge_part_number,
		}
	}
}

/// The planner's complete output for the fast path.
#[derive(Debug, Clone)]
pub struct Plan {
	pub order: Vec<FileId>, // canonical ZIP order — source of truth for sequence
	pub entries: HashMap<FileId, Entry>,
	pub chains: Vec<ChainPlan>,
	pub copyable: Vec<FileId>, // need a phase-1 CRC HEAD (chain order)
}

#[derive(Debug, Clone)]
pub enum Routing {
	CopyPart(Plan),
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

/// The planner. Pure, total: `files -> Routing`.
pub fn plan(files: Vec<SourceFile>) -> Routing {
	let total: u64 = files.iter().map(|f| f.size).sum();

	// Split copyable (>= floor) vs streamable (< floor).
	let mut copyable: Vec<SourceFile> = Vec::new();
	let mut streamable: Vec<SourceFile> = Vec::new();
	for f in files {
		if f.size >= PART_FLOOR {
			copyable.push(f);
		} else {
			streamable.push(f);
		}
	}

	// Pair each copyable with one streamable -> normal chains. Leftover streamables -> first chain.
	let mut normal_pairs: Vec<(SourceFile, Option<SourceFile>)> = Vec::new();
	let mut sq = streamable.into_iter();
	let mut leftover: Vec<SourceFile> = Vec::new();
	for big in copyable {
		match sq.next() {
			Some(small) => normal_pairs.push((big, Some(small))),
			None => normal_pairs.push((big, None)),
		}
	}
	leftover.extend(sq);

	// Viability: need >=2 chains (>=1 normal + a first chain), copyable mass, total big enough.
	// If fewer than 2 normal pairs or total too small, fall back to plain streaming.
	if normal_pairs.len() < 2 || total < VIABILITY_MIN_TOTAL {
		return Routing::Fallback;
	}

	// First chain = leftover streamables (all streamed). If too small / empty, borrow the
	// smallest normal pair's files (both become streamed members of the first chain).
	let mut first_chain_files: Vec<SourceFile> = leftover;
	let first_sum: u64 = first_chain_files.iter().map(|f| f.size).sum();
	if first_sum < PART_FLOOR {
		// borrow the pair with the smallest big
		let idx = normal_pairs
			.iter()
			.enumerate()
			.min_by_key(|(_, (big, _))| big.size)
			.map(|(i, _)| i)
			.expect("normal_pairs non-empty (checked >=2)");
		let (big, small) = normal_pairs.remove(idx);
		first_chain_files.push(big);
		if let Some(s) = small {
			first_chain_files.push(s);
		}
	}

	// After a possible borrow we may have dropped below 2 normal pairs; if so, fall back.
	if normal_pairs.is_empty() {
		return Routing::Fallback;
	}

	// --- Build canonical order: first-chain files, then each normal chain's big then small ---
	let mut order: Vec<FileId> = Vec::new();
	for f in &first_chain_files {
		order.push(f.id);
	}
	for (big, small) in &normal_pairs {
		order.push(big.id);
		if let Some(s) = small {
			order.push(s.id);
		}
	}

	// size/name lookup for offsets + entries
	let mut size_of: HashMap<FileId, (String, u64)> = HashMap::new();
	for f in first_chain_files.iter() {
		size_of.insert(f.id, (f.name.clone(), f.size));
	}
	for (big, small) in normal_pairs.iter() {
		size_of.insert(big.id, (big.name.clone(), big.size));
		if let Some(s) = small {
			size_of.insert(s.id, (s.name.clone(), s.size));
		}
	}

	let offs = compute_offsets(&order, &size_of);

	// streamed-ness: first-chain files are streamed; normal big = copied, normal small = streamed.
	let mut streamed: HashMap<FileId, bool> = HashMap::new();
	for f in &first_chain_files {
		streamed.insert(f.id, true);
	}
	for (big, small) in &normal_pairs {
		streamed.insert(big.id, false);
		if let Some(s) = small {
			streamed.insert(s.id, true);
		}
	}

	let mut entries: HashMap<FileId, Entry> = HashMap::new();
	for (i, id) in order.iter().enumerate() {
		let (name, size) = size_of[id].clone();
		entries.insert(
			*id,
			Entry {
				id: *id,
				name,
				size,
				local_header_offset: offs[i],
				crc: None,
				streamed: streamed[id],
			},
		);
	}

	// copyable ids needing a CRC HEAD = the normal-chain bigs (copied), in chain order.
	let copyable_ids: Vec<FileId> = normal_pairs.iter().map(|(b, _)| b.id).collect();

	// --- Build chains with part numbers and trailing-header handoffs ---
	// First chain (final_merge_part_number = 1): bin-pack streamed files into >= floor parts.
	// Its tail carries the header for normal chain 1's big.
	let mut chains: Vec<ChainPlan> = Vec::new();

	let first_big_id = normal_pairs[0].0.id;
	let first_chain = build_first_chain(&first_chain_files, &size_of, first_big_id);
	chains.push(ChainPlan::FirstStream(first_chain));

	// Normal chains: layered all-copy recipe. next_big_header carries the FOLLOWING chain's
	// big header on this chain's tail (None for the last normal chain). small=None for an
	// unpaired big. The assembler derives the 1-3 layer sequence from these.
	for (k, (big, small)) in normal_pairs.iter().enumerate() {
		let next_big = normal_pairs.get(k + 1).map(|(b, _)| b.id);
		chains.push(ChainPlan::Layered(LayeredChain {
			big: big.id,
			small: small.as_ref().map(|s| s.id),
			next_big_header: next_big,
			final_merge_part_number: (k as u32) + 2, // first chain is slot 1
		}));
	}

	Routing::CopyPart(Plan {
		order,
		entries,
		chains,
		copyable: copyable_ids,
	})
}

/// First chain: all streamed files, bin-packed into >= PART_FLOOR parts (last part exempt),
/// with the trailing CopiedFileHeader(first_big) appended to the final part.
fn build_first_chain(
	files: &[SourceFile],
	size_of: &HashMap<FileId, (String, u64)>,
	first_big_id: FileId,
) -> Chain {
	let mut parts: Vec<PartSpec> = Vec::new();
	let mut cur: Vec<Segment> = Vec::new();
	let mut cur_bytes: u64 = 0;
	let mut part_number: u32 = 1;

	for f in files {
		cur.push(Segment::StreamedFile { id: f.id });
		let (name, size) = &size_of[&f.id];
		cur_bytes += zip_format::entry_total_len(name, *size);
		// close the part once it clears the floor, but keep at least one part open for the tail
		if cur_bytes >= PART_FLOOR {
			parts.push(PartSpec::Stream {
				part_number,
				segments: std::mem::take(&mut cur),
			});
			part_number += 1;
			cur_bytes = 0;
		}
	}
	// append the trailing handoff header to the last (open or new) part — this is the exempt last part
	cur.push(Segment::CopiedFileHeader { id: first_big_id });
	parts.push(PartSpec::Stream {
		part_number,
		segments: cur,
	});

	Chain {
		parts,
		final_merge_part_number: 1,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sf(id: u32, name: &str, size: u64) -> SourceFile {
		SourceFile {
			id: FileId(id),
			key: format!("files/{name}"),
			name: name.to_string(),
			size,
		}
	}

	#[test]
	fn tiny_input_routes_to_fallback() {
		let files = vec![sf(0, "a", 10), sf(1, "b", 20)];
		assert!(matches!(plan(files), Routing::Fallback));
	}

	#[test]
	fn no_copyable_routes_to_fallback() {
		// all below floor
		let files = (0..10).map(|i| sf(i, &format!("f{i}"), 1000)).collect();
		assert!(matches!(plan(files), Routing::Fallback));
	}

	#[test]
	fn viable_input_produces_plan_with_first_chain_slot_1() {
		let big = PART_FLOOR + 100;
		// 5 copyable => ~25 MiB total, clears viability.
		let files = vec![
			sf(0, "big0", big),
			sf(1, "small1", 1000),
			sf(2, "big2", big),
			sf(3, "small3", 1000),
			sf(4, "big4", big),
			sf(5, "small5", 2000),
			sf(6, "big6", big),
			sf(7, "small7", 3000),
			sf(8, "big8", big),
			sf(9, "small9", 4000),
		];
		match plan(files) {
			Routing::CopyPart(p) => {
				assert_eq!(
					p.chains[0].final_merge_part_number(),
					1,
					"first chain is slot 1"
				);
				// every entry has an offset and appears once in order
				assert_eq!(p.order.len(), p.entries.len());
				// chain final-merge numbers are 1..=chains.len(), unique & contiguous
				let mut nums: Vec<u32> = p
					.chains
					.iter()
					.map(|c| c.final_merge_part_number())
					.collect();
				nums.sort();
				assert_eq!(nums, (1..=p.chains.len() as u32).collect::<Vec<_>>());
			}
			Routing::Fallback => panic!("expected fast path"),
		}
	}

	// The real anchor: plan a set, ASSEMBLE what the plan describes in memory (resolving
	// copy bodies and streamed bodies from a synthetic store), then run the validator
	// round-trip (open with zip, extract, SHA256==name, CRC verified).
	#[cfg(feature = "zip_validate")]
	#[test]
	fn planned_archive_passes_validator() {
		use crate::engine::zip_format::EntryMeta;
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

		// Synthetic store: content per file; NAME = sha256(content) (like the real objects).
		// 5 copyable (~5MiB each => ~25MiB total, clears viability) + streamables.
		// With 5 pairs and zero leftover, the first chain is empty -> exercises the
		// borrow-smallest-pair branch.
		let mut contents: Vec<Vec<u8>> = Vec::new();
		let big = PART_FLOOR as usize + 123;
		contents.push(vec![1u8; big]); // copyable
		contents.push(vec![2u8; 1000]); // streamable
		contents.push(vec![3u8; big]); // copyable
		contents.push(vec![4u8; 2000]); // streamable
		contents.push(vec![5u8; big]); // copyable
		contents.push(vec![6u8; 3000]); // streamable
		contents.push(vec![7u8; big]); // copyable
		contents.push(vec![8u8; 4000]); // streamable
		contents.push(vec![9u8; big]); // copyable
		contents.push(vec![10u8; 5000]); // streamable

		let names: Vec<String> = contents.iter().map(|c| sha256_hex(c)).collect();
		let files: Vec<SourceFile> = (0..contents.len())
			.map(|i| SourceFile {
				id: FileId(i as u32),
				key: format!("files/{}", names[i]),
				name: names[i].clone(),
				size: contents[i].len() as u64,
			})
			.collect();

		let plan = match plan(files) {
			Routing::CopyPart(p) => p,
			Routing::Fallback => panic!("expected fast path for this input"),
		};

		// Fill CRCs for copyable entries (phase 1 stand-in) from the synthetic content.
		// Build the EntryMeta list in canonical order with offsets from the plan.
		let by_id = |id: FileId| -> usize { id.0 as usize };
		let metas: Vec<EntryMeta> = plan
			.order
			.iter()
			.map(|id| {
				let e = &plan.entries[id];
				EntryMeta {
					name: e.name.clone(),
					size: e.size,
					crc: crc32(&contents[by_id(*id)]),
					local_header_offset: e.local_header_offset,
				}
			})
			.collect();

		// Assemble archive bytes in canonical order: [local header][body] per entry.
		// (We assemble by ENTRY ORDER, which is what the byte layout is; the chain/part
		//  structure governs HOW bytes get there in AWS, not the final byte sequence.)
		let mut archive: Vec<u8> = Vec::new();
		for (e, id) in metas.iter().zip(plan.order.iter()) {
			archive.extend_from_slice(&zip_format::local_header(e));
			archive.extend_from_slice(&contents[by_id(*id)]);
		}
		let cd_offset = archive.len() as u64;
		let mut cd_size = 0u64;
		for e in &metas {
			let rec = zip_format::central_dir_entry(e);
			cd_size += rec.len() as u64;
			archive.extend_from_slice(&rec);
		}
		archive.extend_from_slice(&zip_format::end_records(
			metas.len() as u64,
			cd_offset,
			cd_size,
		));

		// Validate exactly like the control Lambda.
		let mut expected: HashSet<String> = metas.iter().map(|m| m.name.clone()).collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("planned archive must parse with the standard zip reader");
		assert_eq!(za.len(), metas.len());
		let arch_names: Vec<String> = za.file_names().map(ToOwned::to_owned).collect();
		for n in &arch_names {
			assert!(!n.contains('/'), "flat layout required");
			assert!(expected.remove(n), "unknown/duplicate {n}");
		}
		assert!(expected.is_empty(), "missing entries: {expected:?}");
		for n in &arch_names {
			let mut entry = za.by_name(n).unwrap();
			let mut buf = Vec::new();
			entry.read_to_end(&mut buf).expect("extract (CRC verified)");
			assert_eq!(&sha256_hex(&buf), n, "content hash == name");
		}
	}

	// ===================================================================================
	// PRODUCTION-LAYOUT ANCHOR — assemble the way assemble.rs does, then validate.
	//
	// The earlier test assembles bytes in canonical ENTRY order. Production instead builds
	// each CHAIN as its own object (concatenating its PartSpec parts) and then concatenates
	// the chain objects in final_merge_part_number order, finally appending the directory.
	// The central directory's offsets are computed from plan.order. THIS TEST PROVES the two
	// layouts produce identical bytes — i.e. concatenating chain objects in merge order
	// reproduces the entry-order byte sequence the directory's offsets assume. If they ever
	// diverge, the archive is invalid even though every other test passes.
	//
	// Pure: a synthetic in-memory store maps FileId -> content. No AWS.
	// Run with: cargo test -p figment-engine --features zip_validate chain_layout_matches
	// ===================================================================================
	#[cfg(feature = "zip_validate")]
	#[test]
	fn chain_layout_matches_directory_offsets() {
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

		// ---- synthetic store: id -> content; name = sha256(content) ----
		let big = PART_FLOOR as usize + 77;
		let raw: Vec<Vec<u8>> = vec![
			vec![1u8; big],   // 0 copyable
			vec![2u8; 1500],  // 1 streamable
			vec![3u8; big],   // 2 copyable
			vec![4u8; 2500],  // 3 streamable
			vec![5u8; big],   // 4 copyable
			vec![6u8; 3500],  // 5 streamable
			vec![7u8; big],   // 6 copyable
			vec![8u8; 4500],  // 7 streamable
			vec![9u8; big],   // 8 copyable
			vec![10u8; 5500], // 9 streamable
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

		let mut plan = match plan(files) {
			Routing::CopyPart(p) => p,
			Routing::Fallback => panic!("expected fast path"),
		};

		// Fill copyable CRCs (phase-1 stand-in) from the synthetic content.
		let ids: Vec<FileId> = plan.copyable.clone();
		for id in ids {
			let content = &raw[id.0 as usize];
			if let Some(e) = plan.entries.get_mut(&id) {
				e.crc = Some(crc32(content));
			}
		}

		// ---- Build the headers blob H exactly as the assembler will (all entries) ----
		use crate::engine::header_blob::build_header_blob;
		let metas: Vec<(FileId, EntryMeta)> = plan
			.order
			.iter()
			.map(|id| {
				let e = &plan.entries[id];
				let crc = e.crc.unwrap_or_else(|| crc32(&raw[id.0 as usize]));
				(
					*id,
					EntryMeta {
						name: e.name.clone(),
						size: e.size,
						crc,
						local_header_offset: e.local_header_offset,
					},
				)
			})
			.collect();
		let (h_bytes, h_blob) = build_header_blob(metas);
		// header bytes for id, sourced from H by byte-range (what the assembler copies).
		let h_range = |id: FileId| -> Vec<u8> {
			let r = h_blob.ranges[&id];
			h_bytes[r.offset as usize..r.end() as usize].to_vec()
		};
		// Streamed (inline) header for first-chain files — same bytes, computed locally.
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

		// ---- Build each chain object the PRODUCTION way ----
		// FirstStream: concat its parts (StreamedFile => [header][body], CopiedFileHeader => [header]).
		// Layered:     [B][hS][S][hNext], with hS/hNext copied as byte-ranges of H, B/S as bodies.
		//              (skip hS/S if no small; skip hNext if last normal chain) — exactly the
		//              1-3 layer sequence the assembler will run, but the RESULT bytes are what
		//              we validate here.
		let mut chain_objs: Vec<(u32, Vec<u8>)> = Vec::new();
		for chain in &plan.chains {
			let mut obj: Vec<u8> = Vec::new();
			match chain {
				ChainPlan::FirstStream(c) => {
					for part in &c.parts {
						match part {
							PartSpec::Copy { id, .. } => {
								obj.extend_from_slice(&raw[id.0 as usize]);
							}
							PartSpec::Stream { segments, .. } => {
								for seg in segments {
									match seg {
										Segment::StreamedFile { id } => {
											obj.extend_from_slice(&header_bytes(*id));
											obj.extend_from_slice(&raw[id.0 as usize]);
										}
										Segment::CopiedFileHeader { id } => {
											obj.extend_from_slice(&header_bytes(*id));
										}
									}
								}
							}
						}
					}
					chain_objs.push((c.final_merge_part_number, obj));
				}
				ChainPlan::Layered(l) => {
					// [B]
					obj.extend_from_slice(&raw[l.big.0 as usize]);
					// [hS][S]
					if let Some(s) = l.small {
						obj.extend_from_slice(&h_range(s)); // small's header from H
						obj.extend_from_slice(&raw[s.0 as usize]); // small body
					}
					// [hNext] — next chain's big header rides this tail
					if let Some(nb) = l.next_big_header {
						obj.extend_from_slice(&h_range(nb));
					}
					chain_objs.push((l.final_merge_part_number, obj));
				}
			}
		}
		// Concatenate chain objects in merge-part-number order (the archive's entry region).
		chain_objs.sort_by_key(|(n, _)| *n);
		let mut archive: Vec<u8> = Vec::new();
		for (_, obj) in &chain_objs {
			archive.extend_from_slice(obj);
		}

		// ---- Append the central directory + end records, exactly like build_central_directory ----
		let mut cd_offset = 0u64;
		for id in &plan.order {
			let e = &plan.entries[id];
			cd_offset += zip_format::entry_total_len(&e.name, e.size);
		}
		// sanity: the entry region we built by concatenating chains must equal cd_offset
		assert_eq!(
			archive.len() as u64,
			cd_offset,
			"chain-concatenated entry region ({}) != directory's cd_offset ({}). \
             Chain/merge layout disagrees with plan.order offsets — archive would be invalid.",
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

		// ---- Validate exactly like the control Lambda ----
		let mut expected: HashSet<String> = plan
			.order
			.iter()
			.map(|id| plan.entries[id].name.clone())
			.collect();
		let mut za = zip::ZipArchive::new(Cursor::new(&archive))
			.expect("production-layout archive must parse with the standard zip reader");
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
}
