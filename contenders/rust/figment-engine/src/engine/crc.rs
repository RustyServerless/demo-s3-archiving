//! Pure CRC helpers. NO `aws_sdk_s3` IMPORTS — testable with no AWS.
//!
//! S3 returns a full-object CRC32 as base64 of 4 big-endian bytes (e.g. "ou5p3A==").
//! The ZIP record wants the CRC as a host `u32` (it is then written little-endian by
//! `zip_format`). This module decodes S3's representation into that `u32`.

/// Decode a base64 big-endian CRC32 (as S3 returns in `ChecksumCRC32`) into a host u32.
///
/// Returns None if the base64 is malformed or not exactly 4 bytes.
pub fn decode_s3_crc32(b64: &str) -> Option<u32> {
    let bytes = base64_decode(b64)?;
    if bytes.len() != 4 {
        return None;
    }
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Minimal standard-base64 decoder (no external dep). Handles '=' padding.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let c0 = val(chunk[0])?;
        let c1 = val(chunk[1])?;
        let (c2, pad2) = if chunk[2] == b'=' {
            (0, true)
        } else {
            (val(chunk[2])?, false)
        };
        let (c3, pad3) = if chunk[3] == b'=' {
            (0, true)
        } else {
            (val(chunk[3])?, false)
        };
        let n = ((c0 as u32) << 18) | ((c1 as u32) << 12) | ((c2 as u32) << 6) | (c3 as u32);
        out.push((n >> 16) as u8);
        if !pad2 {
            out.push((n >> 8) as u8);
        }
        if !pad3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_live_s3_value() {
        // Live value observed from `head-object --checksum-mode ENABLED` on a test object.
        // base64 "ou5p3A==" -> bytes a2 ee 69 dc -> big-endian u32 0xA2EE69DC.
        assert_eq!(decode_s3_crc32("ou5p3A=="), Some(0xA2EE_69DC));
    }

    #[test]
    fn rejects_wrong_length() {
        // "AAAA" decodes to 3 bytes, not 4 -> None.
        assert_eq!(decode_s3_crc32("AAAA"), None);
    }

    #[test]
    fn rejects_malformed() {
        assert_eq!(decode_s3_crc32("not!base64!!"), None);
        assert_eq!(decode_s3_crc32("abc"), None); // not multiple of 4
    }

    #[test]
    fn roundtrip_known_crc() {
        // A 4-byte value 0x01020304 big-endian is bytes 01 02 03 04 -> base64 "AQIDBA=="
        assert_eq!(decode_s3_crc32("AQIDBA=="), Some(0x0102_0304));
    }
}
