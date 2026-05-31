// CRC32 with the IEEE polynomial (0xEDB88320 reflected), the polynomial ZIP
// uses. Pure Swift, no C shim, no platform intrinsics — Slicing-by-8.
//
// Slicing-by-8 processes 8 bytes per loop iteration via 8 parallel table
// lookups. About 5–10× faster than the byte-at-a-time implementation;
// slower than the ARMv8 `__crc32{b,h,w,d}` intrinsics but the difference
// is bounded by the data size we hash. At 15 GiB total across the run
// and ~2.6 mean concurrent downloaders, the per-task CRC contribution
// is small relative to network wait time.
//
// Reference: Intel "Fast CRC Computation Using PCLMULQDQ Instruction"
// describes the slicing approach. The 8 256-entry tables are computed at
// process start (lazy on first use).
struct CRC32 {
    private(set) var value: UInt32 = 0

    @inline(__always)
    mutating func update(_ bytes: UnsafeBufferPointer<UInt8>) {
        guard let base = bytes.baseAddress, !bytes.isEmpty else { return }
        value = CRC32Tables.update(crc: value, data: base, length: bytes.count)
    }

    mutating func update<S: Sequence<UInt8>>(_ bytes: S) {
        let arr = ContiguousArray(bytes)
        arr.withUnsafeBufferPointer { update($0) }
    }
}

// One-shot pre-computed slicing-by-8 tables. Allocated once at first use
// and never freed.
private enum CRC32Tables {
    static let tables: [[UInt32]] = makeTables()

    static func makeTables() -> [[UInt32]] {
        // Table 0 is the standard CRC32 byte table.
        var t = [[UInt32]](repeating: [UInt32](repeating: 0, count: 256), count: 8)
        for n in 0..<256 {
            var c = UInt32(n)
            for _ in 0..<8 {
                c = (c & 1) != 0 ? (0xEDB88320 ^ (c >> 1)) : (c >> 1)
            }
            t[0][n] = c
        }
        // Tables 1..7: each derived from table 0 by shifting through one more byte.
        for n in 0..<256 {
            var c = t[0][n]
            for k in 1..<8 {
                c = t[0][Int(c & 0xFF)] ^ (c >> 8)
                t[k][n] = c
            }
        }
        return t
    }

    // Slicing-by-8: process 8 bytes per iteration, then drain remainder one
    // byte at a time. Bit-flips at start/end match zlib/ZIP convention.
    @inline(__always)
    static func update(crc: UInt32, data: UnsafePointer<UInt8>, length: Int) -> UInt32 {
        var c = crc ^ 0xFFFF_FFFF
        var p = data
        var n = length

        // Pre-fetch the eight tables into locals so the compiler keeps the
        // base pointers in registers across the inner loop.
        let t = tables
        return t.withUnsafeBufferPointer { tBuf -> UInt32 in
            let t0 = tBuf[0]
            let t1 = tBuf[1]
            let t2 = tBuf[2]
            let t3 = tBuf[3]
            let t4 = tBuf[4]
            let t5 = tBuf[5]
            let t6 = tBuf[6]
            let t7 = tBuf[7]
            return t0.withUnsafeBufferPointer { p0 -> UInt32 in
                t1.withUnsafeBufferPointer { p1 -> UInt32 in
                    t2.withUnsafeBufferPointer { p2 -> UInt32 in
                        t3.withUnsafeBufferPointer { p3 -> UInt32 in
                            t4.withUnsafeBufferPointer { p4 -> UInt32 in
                                t5.withUnsafeBufferPointer { p5 -> UInt32 in
                                    t6.withUnsafeBufferPointer { p6 -> UInt32 in
                                        t7.withUnsafeBufferPointer { p7 -> UInt32 in
                                            while n >= 8 {
                                                // Load 8 bytes at once. Endian-independent because
                                                // we index each byte explicitly.
                                                let b0 = UInt32(p[0])
                                                let b1 = UInt32(p[1])
                                                let b2 = UInt32(p[2])
                                                let b3 = UInt32(p[3])
                                                let b4 = UInt32(p[4])
                                                let b5 = UInt32(p[5])
                                                let b6 = UInt32(p[6])
                                                let b7 = UInt32(p[7])
                                                let c0 = c & 0xFF
                                                let c1 = (c >> 8) & 0xFF
                                                let c2 = (c >> 16) & 0xFF
                                                let c3 = c >> 24
                                                c = p7[Int(c0 ^ b0)]
                                                  ^ p6[Int(c1 ^ b1)]
                                                  ^ p5[Int(c2 ^ b2)]
                                                  ^ p4[Int(c3 ^ b3)]
                                                  ^ p3[Int(b4)]
                                                  ^ p2[Int(b5)]
                                                  ^ p1[Int(b6)]
                                                  ^ p0[Int(b7)]
                                                p = p.advanced(by: 8)
                                                n -= 8
                                            }
                                            while n > 0 {
                                                c = p0[Int((c ^ UInt32(p.pointee)) & 0xFF)] ^ (c >> 8)
                                                p = p.advanced(by: 1)
                                                n -= 1
                                            }
                                            return c ^ 0xFFFF_FFFF
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
