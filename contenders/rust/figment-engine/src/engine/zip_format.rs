//! Pure ZIP byte-layout for STORE-mode (method 0) archives, ZIP64-aware.
//!
//! NO `aws_sdk_s3` IMPORTS IN THIS MODULE — it is part of the pure engine and must
//! remain testable with no AWS and no I/O.
//!
//! Scope, and why it is simple here:
//! - STORE only: compressed_size == uncompressed_size == the object's byte length.
//! - All sizes known up front, so no data descriptors, GP-flag bit 3 is never set.
//! - Individual entries are small (~5 MB), so per-entry *sizes* always fit in 32 bits.
//!   Only the central-directory *offset* of an entry can exceed 4 GiB (the archive is
//!   ~15 GB), so the only place ZIP64 is conditionally needed per-entry is the
//!   local-header offset in the central directory record.
//!
//! Everything is fixed-layout little-endian record packing. The functions below each
//! return an owned `Vec<u8>`; callers concatenate them in order.

// ---- Signatures ----
const SIG_LOCAL: u32 = 0x0403_4b50; // "PK\x03\x04"
const SIG_CENTRAL: u32 = 0x0201_4b50; // "PK\x01\x02"
const SIG_EOCD: u32 = 0x0605_4b50; // "PK\x05\x06"
const SIG_ZIP64_EOCD: u32 = 0x0606_4b50; // "PK\x06\x06"
const SIG_ZIP64_LOCATOR: u32 = 0x0706_4b50; // "PK\x06\x07"

const ZIP64_EXTRA_TAG: u16 = 0x0001;

/// Version-needed-to-extract. 4.5 (=45) signals ZIP64 support. We use 45 everywhere
/// for uniformity; readers accept a higher-than-needed value.
const VERSION_NEEDED: u16 = 45;
/// Version-made-by: upper byte 0 (MS-DOS / FAT), lower byte = spec version (45).
const VERSION_MADE_BY: u16 = 45;

const METHOD_STORE: u16 = 0;

/// Sentinel written into a 32-bit field when the real value lives in a ZIP64 extra.
const U32_MAX: u32 = 0xFFFF_FFFF;
const U16_MAX: u16 = 0xFFFF;

// ---- Small LE push helpers ----
fn p16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn p32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn p64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Metadata needed to emit an entry's records. `size` is the STORE size (== compressed
/// == uncompressed). `crc` is the CRC-32 over the file's (plaintext) bytes, as a host
/// `u32` (callers decode S3's base64 big-endian checksum into this before calling).
#[derive(Debug, Clone)]
pub struct EntryMeta {
    pub name: String,
    pub size: u64,
    pub crc: u32,
    /// Offset of this entry's local header within the archive. Only consulted by the
    /// central-directory packer (local headers don't contain their own offset).
    pub local_header_offset: u64,
}

/// Local file header (the bytes that sit immediately before the file body).
///
/// Layout (sizes < 4 GiB so no ZIP64 extra is needed in the *local* header):
/// ```text
///  0  u32 signature  PK\x03\x04
///  4  u16 version needed (45)
///  6  u16 gp flag (0)
///  8  u16 method (0 = store)
/// 10  u16 mod time (0)
/// 12  u16 mod date (0)
/// 14  u32 crc-32
/// 18  u32 compressed size   (== size; < 4 GiB here)
/// 22  u32 uncompressed size (== size)
/// 26  u16 file name length
/// 28  u16 extra field length (0)
/// 30  .. file name
/// ```
pub fn local_header(e: &EntryMeta) -> Vec<u8> {
    debug_assert!(
        e.size <= U32_MAX as u64,
        "entry size must fit 32 bits in STORE local header"
    );
    let name = e.name.as_bytes();
    let mut b = Vec::with_capacity(30 + name.len());
    p32(&mut b, SIG_LOCAL);
    p16(&mut b, VERSION_NEEDED);
    p16(&mut b, 0); // gp flag
    p16(&mut b, METHOD_STORE);
    p16(&mut b, 0); // mod time
    p16(&mut b, 0); // mod date
    p32(&mut b, e.crc);
    p32(&mut b, e.size as u32); // compressed
    p32(&mut b, e.size as u32); // uncompressed
    p16(&mut b, name.len() as u16);
    p16(&mut b, 0); // extra len
    b.extend_from_slice(name);
    b
}

/// Length of a local header in bytes (for offset accumulation), without building it.
pub fn local_header_len(name: &str) -> u64 {
    30 + name.len() as u64
}

/// Total bytes an entry contributes to the archive body region: its local header + body.
pub fn entry_total_len(e_name: &str, size: u64) -> u64 {
    local_header_len(e_name) + size
}

/// Central directory file header for one entry.
///
/// The local-header offset is the only field that may need ZIP64 here. When
/// `local_header_offset >= 4 GiB`, the 32-bit offset field holds 0xFFFFFFFF and the
/// real 64-bit offset goes into a ZIP64 extra field (tag 0x0001).
///
/// Layout:
/// ```text
///  0  u32 signature PK\x01\x02
///  4  u16 version made by (45)
///  6  u16 version needed (45)
///  8  u16 gp flag (0)
/// 10  u16 method (0)
/// 12  u16 mod time (0)
/// 14  u16 mod date (0)
/// 16  u32 crc-32
/// 20  u32 compressed size   (== size)
/// 24  u32 uncompressed size (== size)
/// 28  u16 file name length
/// 30  u16 extra field length
/// 32  u16 comment length (0)
/// 34  u16 disk number start (0)
/// 36  u16 internal attrs (0)
/// 38  u32 external attrs (0)
/// 42  u32 local header offset (or 0xFFFFFFFF sentinel)
/// 46  .. file name
///     .. extra (ZIP64: tag 0x0001, size 8, u64 offset) when offset needs 64 bits
/// ```
pub fn central_dir_entry(e: &EntryMeta) -> Vec<u8> {
    debug_assert!(
        e.size <= U32_MAX as u64,
        "entry size must fit 32 bits in STORE central record"
    );
    let name = e.name.as_bytes();
    let needs_zip64_offset = e.local_header_offset > U32_MAX as u64;

    // ZIP64 extra holds only the offset here (sizes fit 32 bits). 2 (tag) + 2 (size) + 8 (offset).
    let extra_len: u16 = if needs_zip64_offset { 12 } else { 0 };

    let mut b = Vec::with_capacity(46 + name.len() + extra_len as usize);
    p32(&mut b, SIG_CENTRAL);
    p16(&mut b, VERSION_MADE_BY);
    p16(&mut b, VERSION_NEEDED);
    p16(&mut b, 0); // gp flag
    p16(&mut b, METHOD_STORE);
    p16(&mut b, 0); // mod time
    p16(&mut b, 0); // mod date
    p32(&mut b, e.crc);
    p32(&mut b, e.size as u32); // compressed
    p32(&mut b, e.size as u32); // uncompressed
    p16(&mut b, name.len() as u16);
    p16(&mut b, extra_len);
    p16(&mut b, 0); // comment len
    p16(&mut b, 0); // disk number start
    p16(&mut b, 0); // internal attrs
    p32(&mut b, 0); // external attrs
    if needs_zip64_offset {
        p32(&mut b, U32_MAX); // offset sentinel
    } else {
        p32(&mut b, e.local_header_offset as u32);
    }
    b.extend_from_slice(name);
    if needs_zip64_offset {
        p16(&mut b, ZIP64_EXTRA_TAG);
        p16(&mut b, 8); // extra data size: just the 8-byte offset
        p64(&mut b, e.local_header_offset);
    }
    b
}

/// Length of a central-directory record without building it (for end-record offset math).
#[allow(dead_code)] // used in tests
pub fn central_dir_entry_len(name: &str, local_header_offset: u64) -> u64 {
    let extra = if local_header_offset > U32_MAX as u64 {
        12
    } else {
        0
    };
    46 + name.len() as u64 + extra
}

/// The trailer: ZIP64 EOCD record + ZIP64 EOCD locator + classic EOCD.
///
/// We always emit the ZIP64 trailer because the archive exceeds 4 GiB (its central
/// directory offset and total size do not fit 32 bits). The classic EOCD then carries
/// 0xFFFF / 0xFFFFFFFF sentinels pointing readers at the ZIP64 records.
///
/// - `entry_count`: number of entries.
/// - `cd_offset`: byte offset where the central directory begins.
/// - `cd_size`: total byte length of the central directory.
pub fn end_records(entry_count: u64, cd_offset: u64, cd_size: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(56 + 20 + 22);

    // ---- ZIP64 end of central directory record (size = 56 total) ----
    //  0 u32 sig PK\x06\x06
    //  4 u64 size of zip64 EOCD record (= total - 12) = 44
    // 12 u16 version made by
    // 14 u16 version needed
    // 16 u32 disk number (0)
    // 20 u32 disk with CD start (0)
    // 24 u64 entries on this disk
    // 32 u64 total entries
    // 40 u64 size of central directory
    // 48 u64 offset of central directory
    let zip64_eocd_offset = cd_offset + cd_size; // where this record starts
    p32(&mut b, SIG_ZIP64_EOCD);
    p64(&mut b, 44); // size of remaining record (56 - 12)
    p16(&mut b, VERSION_MADE_BY);
    p16(&mut b, VERSION_NEEDED);
    p32(&mut b, 0); // disk number
    p32(&mut b, 0); // disk with CD start
    p64(&mut b, entry_count); // entries this disk
    p64(&mut b, entry_count); // total entries
    p64(&mut b, cd_size);
    p64(&mut b, cd_offset);

    // ---- ZIP64 EOCD locator (size = 20) ----
    //  0 u32 sig PK\x06\x07
    //  4 u32 disk with zip64 EOCD (0)
    //  8 u64 offset of zip64 EOCD record
    // 16 u32 total number of disks (1)
    p32(&mut b, SIG_ZIP64_LOCATOR);
    p32(&mut b, 0);
    p64(&mut b, zip64_eocd_offset);
    p32(&mut b, 1);

    // ---- classic EOCD (size = 22) with sentinels ----
    //  0 u32 sig PK\x05\x06
    //  4 u16 disk number (0)
    //  6 u16 disk with CD start (0)
    //  8 u16 entries this disk   (0xFFFF sentinel)
    // 10 u16 total entries        (0xFFFF sentinel)
    // 12 u32 size of CD           (0xFFFFFFFF sentinel)
    // 16 u32 offset of CD         (0xFFFFFFFF sentinel)
    // 20 u16 comment length (0)
    p32(&mut b, SIG_EOCD);
    p16(&mut b, 0);
    p16(&mut b, 0);
    p16(
        &mut b,
        if entry_count >= U16_MAX as u64 {
            U16_MAX
        } else {
            entry_count as u16
        },
    );
    p16(
        &mut b,
        if entry_count >= U16_MAX as u64 {
            U16_MAX
        } else {
            entry_count as u16
        },
    );
    p32(
        &mut b,
        if cd_size >= U32_MAX as u64 {
            U32_MAX
        } else {
            cd_size as u32
        },
    );
    p32(
        &mut b,
        if cd_offset >= U32_MAX as u64 {
            U32_MAX
        } else {
            cd_offset as u32
        },
    );
    p16(&mut b, 0); // comment len

    b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str, size: u64, crc: u32, off: u64) -> EntryMeta {
        EntryMeta {
            name: name.to_string(),
            size,
            crc,
            local_header_offset: off,
        }
    }

    #[test]
    fn local_header_fixed_fields_and_len() {
        let e = meta("a.txt", 13, 0xDEAD_BEEF, 0);
        let h = local_header(&e);
        assert_eq!(h.len() as u64, local_header_len(&e.name));
        assert_eq!(&h[0..4], &SIG_LOCAL.to_le_bytes());
        assert_eq!(u16::from_le_bytes([h[4], h[5]]), VERSION_NEEDED);
        assert_eq!(u16::from_le_bytes([h[8], h[9]]), METHOD_STORE);
        assert_eq!(
            u32::from_le_bytes([h[14], h[15], h[16], h[17]]),
            0xDEAD_BEEF
        );
        assert_eq!(u32::from_le_bytes([h[18], h[19], h[20], h[21]]), 13); // compressed
        assert_eq!(u32::from_le_bytes([h[22], h[23], h[24], h[25]]), 13); // uncompressed
        assert_eq!(u16::from_le_bytes([h[26], h[27]]), 5); // name len
        assert_eq!(u16::from_le_bytes([h[28], h[29]]), 0); // extra len
        assert_eq!(&h[30..], b"a.txt");
    }

    #[test]
    fn central_no_zip64_when_offset_small() {
        let e = meta("a.txt", 13, 1, 100);
        let c = central_dir_entry(&e);
        assert_eq!(
            c.len() as u64,
            central_dir_entry_len(&e.name, e.local_header_offset)
        );
        assert_eq!(u16::from_le_bytes([c[30], c[31]]), 0); // extra len == 0
        assert_eq!(u32::from_le_bytes([c[42], c[43], c[44], c[45]]), 100); // real offset inline
        assert_eq!(&c[46..], b"a.txt");
    }

    #[test]
    fn central_zip64_when_offset_large() {
        let big = (U32_MAX as u64) + 1; // 4 GiB exactly over the line
        let e = meta("b", 7, 9, big);
        let c = central_dir_entry(&e);
        assert_eq!(c.len() as u64, central_dir_entry_len(&e.name, big));
        assert_eq!(u16::from_le_bytes([c[30], c[31]]), 12); // extra len
        assert_eq!(u32::from_le_bytes([c[42], c[43], c[44], c[45]]), U32_MAX); // sentinel
        // name (1 byte) then extra
        assert_eq!(&c[46..47], b"b");
        let ex = &c[47..];
        assert_eq!(u16::from_le_bytes([ex[0], ex[1]]), ZIP64_EXTRA_TAG);
        assert_eq!(u16::from_le_bytes([ex[2], ex[3]]), 8);
        assert_eq!(u64::from_le_bytes(ex[4..12].try_into().unwrap()), big);
    }

    #[test]
    fn end_records_sizes_and_sentinels() {
        let r = end_records(3000, 14_000_000_000, 200_000);
        assert_eq!(r.len(), 56 + 20 + 22);
        assert_eq!(&r[0..4], &SIG_ZIP64_EOCD.to_le_bytes());
        assert_eq!(u64::from_le_bytes(r[4..12].try_into().unwrap()), 44);
        // total entries in zip64 eocd
        assert_eq!(u64::from_le_bytes(r[32..40].try_into().unwrap()), 3000);
        assert_eq!(u64::from_le_bytes(r[40..48].try_into().unwrap()), 200_000); // cd size
        assert_eq!(
            u64::from_le_bytes(r[48..56].try_into().unwrap()),
            14_000_000_000
        ); // cd offset
        // locator
        let loc = &r[56..76];
        assert_eq!(&loc[0..4], &SIG_ZIP64_LOCATOR.to_le_bytes());
        assert_eq!(
            u64::from_le_bytes(loc[8..16].try_into().unwrap()),
            14_000_000_000 + 200_000
        );
        assert_eq!(u32::from_le_bytes(loc[16..20].try_into().unwrap()), 1);
        // classic eocd sentinels (offset > 4 GiB so sentinel)
        let eo = &r[76..98];
        assert_eq!(&eo[0..4], &SIG_EOCD.to_le_bytes());
        assert_eq!(u32::from_le_bytes(eo[16..20].try_into().unwrap()), U32_MAX); // cd offset sentinel
    }
}
