#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Streaming ZIP encoder. Method = STORED (no compression). GP flag bit 3
// is set so CRC + sizes are written in a data descriptor *after* each
// file's body — necessary because we stream the body before knowing the
// CRC. ZIP64 records are emitted unconditionally so archives larger than
// 4 GiB are valid.

enum ZipSig: UInt32 {
    case localFileHeader = 0x04034b50
    case dataDescriptor = 0x08074b50
    case centralDirectory = 0x02014b50
    case zip64EndOfCentralDirectory = 0x06064b50
    case zip64EndOfCentralDirectoryLocator = 0x07064b50
    case endOfCentralDirectory = 0x06054b50
}

struct ZipEntry {
    let name: String
    let crc32: UInt32
    let size: UInt64
    let localHeaderOffset: UInt64
}

enum ZipHeaders {
    static let storedMethod: UInt16 = 0
    // GP flag bit 3 = data descriptor follows (sizes/CRC unknown at LFH time).
    static let gpFlag: UInt16 = 0x0008
    static let versionNeededZip64: UInt16 = 45
    static let versionMadeByUnix: UInt16 = (3 << 8) | 45
    // Fixed mtime (2010-01-01 00:00:00) so two runs over the same input
    // produce byte-identical archives.
    static let dosTime: UInt16 = 0
    static let dosDate: UInt16 = (30 << 9) | (1 << 5) | 1  // 2010-01-01

    static func localFileHeader(name: String) -> Data {
        let nameBytes = Array(name.utf8)
        var data = Data()
        data.reserveCapacity(30 + nameBytes.count)
        data.appendLE(ZipSig.localFileHeader.rawValue)
        data.appendLE(versionNeededZip64)
        data.appendLE(gpFlag)
        data.appendLE(storedMethod)
        data.appendLE(dosTime)
        data.appendLE(dosDate)
        data.appendLE(UInt32(0))  // CRC (in data descriptor)
        data.appendLE(UInt32(0))  // compressed size (in data descriptor)
        data.appendLE(UInt32(0))  // uncompressed size (in data descriptor)
        data.appendLE(UInt16(nameBytes.count))
        data.appendLE(UInt16(0))  // extra field length
        data.append(contentsOf: nameBytes)
        return data
    }

    // ZIP64 data descriptor: 24 bytes including signature.
    // Sizes are 8-byte little-endian because GP flag bit 3 + ZIP64 implies 64-bit.
    static func dataDescriptor(crc32: UInt32, size: UInt64) -> Data {
        var data = Data()
        data.reserveCapacity(24)
        data.appendLE(ZipSig.dataDescriptor.rawValue)
        data.appendLE(crc32)
        data.appendLE(size)  // compressed size
        data.appendLE(size)  // uncompressed size
        return data
    }

    // Central directory header for one entry, with mandatory ZIP64 extra field
    // (sizes always present, LFH offset always present — total 28 bytes:
    // tag(2) + size(2) + uncompressed(8) + compressed(8) + offset(8)).
    static func centralDirectoryHeader(_ entry: ZipEntry) -> Data {
        let nameBytes = Array(entry.name.utf8)
        var data = Data()
        data.reserveCapacity(46 + nameBytes.count + 28)

        data.appendLE(ZipSig.centralDirectory.rawValue)
        data.appendLE(versionMadeByUnix)
        data.appendLE(versionNeededZip64)
        data.appendLE(gpFlag)
        data.appendLE(storedMethod)
        data.appendLE(dosTime)
        data.appendLE(dosDate)
        data.appendLE(entry.crc32)
        data.appendLE(UInt32(0xFFFFFFFF))  // compressed size (use ZIP64 extra)
        data.appendLE(UInt32(0xFFFFFFFF))  // uncompressed size (use ZIP64 extra)
        data.appendLE(UInt16(nameBytes.count))
        data.appendLE(UInt16(28))  // extra field length
        data.appendLE(UInt16(0))   // file comment length
        data.appendLE(UInt16(0))   // disk number start
        data.appendLE(UInt16(0))   // internal file attributes
        data.appendLE(UInt32(0o644 << 16))  // external file attributes (unix 0644)
        data.appendLE(UInt32(0xFFFFFFFF))  // local header offset (use ZIP64 extra)
        data.append(contentsOf: nameBytes)

        // ZIP64 extended information extra field — order: uncompressed, compressed, offset.
        data.appendLE(UInt16(0x0001))  // tag
        data.appendLE(UInt16(24))      // size of the rest
        data.appendLE(entry.size)      // uncompressed
        data.appendLE(entry.size)      // compressed
        data.appendLE(entry.localHeaderOffset)

        return data
    }

    static func zip64EndOfCentralDirectory(
        entryCount: UInt64,
        cdSize: UInt64,
        cdOffset: UInt64
    ) -> Data {
        var data = Data()
        data.reserveCapacity(56)
        data.appendLE(ZipSig.zip64EndOfCentralDirectory.rawValue)
        data.appendLE(UInt64(44))                 // size of this record - 12
        data.appendLE(versionMadeByUnix)
        data.appendLE(versionNeededZip64)
        data.appendLE(UInt32(0))                  // disk number
        data.appendLE(UInt32(0))                  // disk with central directory
        data.appendLE(entryCount)                 // entries on this disk
        data.appendLE(entryCount)                 // total entries
        data.appendLE(cdSize)
        data.appendLE(cdOffset)
        return data
    }

    static func zip64EndOfCentralDirectoryLocator(zip64EocdOffset: UInt64) -> Data {
        var data = Data()
        data.reserveCapacity(20)
        data.appendLE(ZipSig.zip64EndOfCentralDirectoryLocator.rawValue)
        data.appendLE(UInt32(0))               // disk with ZIP64 EOCD
        data.appendLE(zip64EocdOffset)
        data.appendLE(UInt32(1))               // total disks
        return data
    }

    static func endOfCentralDirectory() -> Data {
        var data = Data()
        data.reserveCapacity(22)
        data.appendLE(ZipSig.endOfCentralDirectory.rawValue)
        data.appendLE(UInt16(0))                       // disk number
        data.appendLE(UInt16(0))                       // disk with central directory
        data.appendLE(UInt16(0xFFFF))                  // entries on this disk → see ZIP64
        data.appendLE(UInt16(0xFFFF))                  // total entries → see ZIP64
        data.appendLE(UInt32(0xFFFFFFFF))              // central directory size → see ZIP64
        data.appendLE(UInt32(0xFFFFFFFF))              // central directory offset → see ZIP64
        data.appendLE(UInt16(0))                       // comment length
        return data
    }
}

// Little-endian Data appenders for the fixed-width integer types ZIP uses.
extension Data {
    mutating func appendLE(_ value: UInt16) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { append(contentsOf: $0) }
    }
    mutating func appendLE(_ value: UInt32) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { append(contentsOf: $0) }
    }
    mutating func appendLE(_ value: UInt64) {
        var v = value.littleEndian
        Swift.withUnsafeBytes(of: &v) { append(contentsOf: $0) }
    }
}
