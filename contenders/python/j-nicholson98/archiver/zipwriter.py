"""Streaming ZIP writer. Method=Stored only. ZIP64 is emitted strictly CONDITIONALLY
-- only on the records whose value actually overflows its 32-bit/16-bit slot, exactly
as the APPNOTE intends and as every standard ZIP library (Info-ZIP, Python zipfile,
the Rust `zip` crate) does. We previously forced ZIP64 on every entry "for code-path
uniformity"; that is spec-valid (APPNOTE: "ZIP64 format MAY be used regardless of the
size of a file") but non-idiomatic, and the contest's Rust-`zip`-crate validator reads
the conditional form far faster, so we follow the common form precisely."""
import struct

# Fixed DOS timestamp: 1980-01-01 00:00:00 (year-offset=0 (=> 1980), month=1, day=1).
_DOS_TIME = 0
_DOS_DATE = (1 << 5) | 1  # 0x0021
_U16_MAX = 0xFFFF
_U32_MAX = 0xFFFFFFFF
_VERSION_BASE = 20  # 2.0 => base (Stored, no ZIP64 extensions in this record)
_VERSION_Z64 = 45   # 4.5 => this record uses ZIP64 format extensions
_VERSION_MADE = 45  # writer supports up to 4.5

_SIG_LOCAL = 0x04034B50
_SIG_CENTRAL = 0x02014B50
_SIG_Z64_EOCD = 0x06064B50
_SIG_Z64_LOC = 0x07064B50
_SIG_EOCD = 0x06054B50


def local_file_header(name: bytes, crc: int, size: int) -> bytes:
    # Every object is < 4 GB, so sizes fit inline and the local record needs no ZIP64
    # extra -- version-needed is therefore the base 2.0 (APPNOTE reserves 4.5 for records
    # that actually carry a ZIP64 extra). A >=4 GB object would require a ZIP64 local
    # extra carrying BOTH sizes; the contest corpus is 2-8 MB so that path is unused.
    return struct.pack(
        "<IHHHHHIIIHH",
        _SIG_LOCAL, _VERSION_BASE, 0x0800, 0, _DOS_TIME, _DOS_DATE,  # bit 11 = filename is UTF-8
        crc & 0xFFFFFFFF, size, size, len(name), 0,
    ) + name


def central_dir_header(name: bytes, crc: int, size: int, offset: int) -> bytes:
    # Sizes are < 4 GB (inline); only the local-header offset can exceed 4 GB. Per
    # APPNOTE the ZIP64 extra carries ONLY the fields whose slot is the sentinel, so we
    # add it (and bump version-needed to 4.5) ONLY when the offset actually overflows.
    if offset > _U32_MAX:
        extra = struct.pack("<HHQ", 0x0001, 8, offset)  # tag, data-size=8, 8-byte offset
        off_field = _U32_MAX
        vneed = _VERSION_Z64
    else:
        extra = b""
        off_field = offset
        vneed = _VERSION_BASE
    return struct.pack(
        "<IHHHHHHIIIHHHHHII",
        _SIG_CENTRAL, _VERSION_MADE, vneed, 0x0800, 0, _DOS_TIME, _DOS_DATE,  # bit 11 = UTF-8
        crc & 0xFFFFFFFF, size, size, len(name), len(extra),
        0, 0, 0, 0, off_field,
    ) + name + extra


def zip64_eocd(count: int, cd_size: int, cd_offset: int) -> bytes:
    return struct.pack(
        "<IQHHIIQQQQ",
        _SIG_Z64_EOCD, 44, _VERSION_MADE, _VERSION_Z64, 0, 0,
        count, count, cd_size, cd_offset,
    )


def zip64_eocd_locator(z64_eocd_offset: int) -> bytes:
    return struct.pack("<IIQI", _SIG_Z64_LOC, 0, z64_eocd_offset, 1)


def eocd(count: int, cd_size: int, cd_offset: int) -> bytes:
    # Each slot holds the real value when it fits, else the 0xFFFF/0xFFFFFFFF sentinel
    # that points the reader at the ZIP64 EOCD (which is only written when needed).
    return struct.pack(
        "<IHHHHIIH", _SIG_EOCD, 0, 0,
        min(count, _U16_MAX), min(count, _U16_MAX),
        cd_size if cd_size <= _U32_MAX else _U32_MAX,
        cd_offset if cd_offset <= _U32_MAX else _U32_MAX, 0,
    )


class Zip64Assembler:
    """Appends entries to a single linear stream in completion order and finalizes
    with ZIP64 records. `sink` must implement write(bytes) and tell() -> int."""

    def __init__(self, sink):
        self._sink = sink
        self._central = []  # list[bytes]
        self._count = 0

    @property
    def total_bytes(self) -> int:
        return self._sink.tell()

    def add_entry(self, name: str, crc: int, data: bytes) -> None:
        """Build header + write data. Used directly in tests."""
        name_b = name.encode("utf-8")
        offset = self._sink.tell()
        self._sink.write(local_file_header(name_b, crc, len(data)))
        self._sink.write(data)
        self._record(name_b, crc, len(data), offset)

    def add_prebuilt(self, name: str, crc: int, size: int, payload: bytes) -> None:
        """Write a worker-prebuilt [local header + data] blob; record central entry.
        This is the hot path used by the pipeline (header built off the assembler).

        Precondition: `size`, `crc`, and `name` MUST match the values baked into the
        local header inside `payload`. A mismatch makes the local and central-directory
        records disagree and silently corrupts that entry. The caller (archive.py)
        builds payload and these args together from the same source, upholding this.
        """
        offset = self._sink.tell()
        self._sink.write(payload)
        self._record(name.encode("utf-8"), crc, size, offset)

    def add_segments(self, name: str, crc: int, size: int, header, body) -> None:
        """Hot path for the slab pool: write the local `header` then the `body`
        (a memoryview over a recycled slab, or bytes) as two segments -- no fresh
        per-object payload concat. The combined bytes are identical to add_prebuilt
        with payload == header + body, so the produced archive is byte-for-byte the
        same. Same precondition as add_prebuilt: size/crc/name must match `header`."""
        offset = self._sink.tell()
        self._sink.write(header)
        self._sink.write(body)
        self._record(name.encode("utf-8"), crc, size, offset)

    def _record(self, name_b: bytes, crc: int, size: int, offset: int) -> None:
        self._central.append(central_dir_header(name_b, crc, size, offset))
        self._count += 1

    def finish(self) -> None:
        cd_offset = self._sink.tell()
        for h in self._central:
            self._sink.write(h)
        cd_size = self._sink.tell() - cd_offset
        # Emit the ZIP64 EOCD + locator only when a value overflows its EOCD slot
        # (cd_offset passes 4 GB on the full run; small/local archives omit them, just
        # like a standard library). The EOCD then sentinels exactly the overflowed slots.
        if cd_offset > _U32_MAX or cd_size > _U32_MAX or self._count > _U16_MAX:
            z64_offset = self._sink.tell()
            self._sink.write(zip64_eocd(self._count, cd_size, cd_offset))
            self._sink.write(zip64_eocd_locator(z64_offset))
        self._sink.write(eocd(self._count, cd_size, cd_offset))
