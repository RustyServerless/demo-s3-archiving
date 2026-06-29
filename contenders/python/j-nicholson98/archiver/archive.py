"""The streaming pipeline: N download+CRC worker threads -> bounded queue ->
single assembler -> PartChunker -> storage multipart upload.

Download bodies are read into recycled slabs from a fixed BufferPool, so peak
download-side memory is O(pool), flat in worker count -- this is what lets the
archiver run at high concurrency inside 512 MB without the per-object allocation
churn the free-threaded allocator otherwise retains."""
import queue
import threading
import zlib

from .bufferpool import BufferPool
from .storage import PartChunker, Storage
from .zipwriter import Zip64Assembler, local_file_header

DEFAULT_PART_SIZE = 10 * 1024 * 1024  # 10 MB (>= S3's 5 MB minimum)
DEFAULT_SLAB_SIZE = 12 * 1024 * 1024  # holds a 5 MB object with wide margin; larger
                                      # objects fall back to fresh bytes (still correct)


class _Entry:
    __slots__ = ("name", "crc", "size", "header", "body", "slab")

    def __init__(self, name, crc, size, header, body, slab):
        self.name = name
        self.crc = crc
        self.size = size
        self.header = header  # local file header bytes (~90 B)
        self.body = body      # memoryview over `slab`, or fresh bytes (oversized)
        self.slab = slab      # borrowed slab to release after the write


class _Err:
    __slots__ = ("exc",)

    def __init__(self, exc):
        self.exc = exc


def archive(storage: Storage, src_prefix: str, dst_key: str, *,
            n_download: int = 4, n_upload: int = 4,
            part_size: int = DEFAULT_PART_SIZE, queue_depth: int = 8,
            slab_size: int = DEFAULT_SLAB_SIZE, pool_size: int | None = None) -> dict:
    keys = storage.list_keys(src_prefix)

    key_q: "queue.Queue[str]" = queue.Queue()
    for k in keys:
        key_q.put(k)
    entry_q: "queue.Queue[object]" = queue.Queue(maxsize=queue_depth)
    stop = threading.Event()

    # Size the pool to cover every slab that can be live at once: in the queue
    # (queue_depth), in download (n_download), and the one the assembler holds.
    # +2 is that headroom. The assembler only ever *releases*, so it always drains
    # the queue and a blocked downloader is eventually fed -- no deadlock.
    if pool_size is None:
        pool_size = queue_depth + n_download + 2
    pool = BufferPool(pool_size, slab_size)

    def _put(item) -> bool:
        # Stop-aware put: never block forever. On stop, returns False so the
        # worker can exit promptly instead of leaking on a full queue.
        while not stop.is_set():
            try:
                entry_q.put(item, timeout=0.2)
                return True
            except queue.Full:
                continue
        return False

    def downloader():
        while not stop.is_set():
            try:
                key = key_q.get_nowait()
            except queue.Empty:
                return
            slab = pool.borrow(stop)
            if slab is None:        # stop set while waiting for a slab
                return
            try:
                data, n = storage.get_object_into(key, slab)
                body = data if data is not None else memoryview(slab)[:n]
                crc = zlib.crc32(body) & 0xFFFFFFFF
                name = key.rsplit("/", 1)[-1]
                header = local_file_header(name.encode("utf-8"), crc, n)
                entry = _Entry(name, crc, n, header, body, slab)
            except Exception as exc:  # nothing emitted -> return the slab, propagate
                pool.release(slab)
                _put(_Err(exc))
                return
            if not _put(entry):       # stop set: we still own the slab, return it
                pool.release(slab)
                return

    workers = [threading.Thread(target=downloader, daemon=True)
               for _ in range(n_download)]
    for t in workers:
        t.start()

    mp = storage.open_multipart_upload(dst_key, n_upload=n_upload)
    sink = PartChunker(mp, part_size)
    writer = Zip64Assembler(sink)

    try:
        # Each worker emits exactly one item per key (an _Entry, or one _Err then
        # exits), so consuming exactly len(keys) items terminates cleanly.
        for _ in range(len(keys)):
            item = entry_q.get()
            if isinstance(item, _Err):
                raise item.exc
            try:
                writer.add_segments(item.name, item.crc, item.size,
                                    item.header, item.body)
            finally:
                # PartChunker has copied the body into its part buffer by now, so
                # the slab is free to recycle. Release in finally so a write error
                # can't strand a slab and deadlock the remaining workers.
                pool.release(item.slab)
        writer.finish()
        sink.flush_final()
        mp.complete()
    except BaseException:
        stop.set()      # release any workers blocked on a full queue / empty pool
        mp.abort()
        raise
    finally:
        stop.set()
        for t in workers:
            t.join(timeout=5)

    return {"objects": len(keys), "bytes": writer.total_bytes}
