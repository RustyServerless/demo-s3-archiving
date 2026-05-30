import CCRC32

struct CRC32 {
    private(set) var value: UInt32 = 0

    mutating func update(_ bytes: UnsafeBufferPointer<UInt8>) {
        guard let base = bytes.baseAddress, !bytes.isEmpty else { return }
        value = ccrc32_update(value, base, bytes.count)
    }

    mutating func update<S: Sequence<UInt8>>(_ bytes: S) {
        let arr = ContiguousArray(bytes)
        arr.withUnsafeBufferPointer { update($0) }
    }
}
