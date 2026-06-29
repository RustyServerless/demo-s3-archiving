"""A fixed pool of recycled byte buffers ("slabs").

The download pipeline borrows a slab, reads an object body straight into it
(zero-copy via readinto), hands it through the queue, and the assembler releases
it back. Because the pool is fixed-size, peak download-side memory is O(pool),
flat in the number of worker threads -- this is what lets the archiver run at
high concurrency inside 512 MB without the per-object allocation churn that the
free-threaded build's allocator otherwise retains.

The pool also *is* the backpressure: borrow() blocks when every slab is out, so
downloaders cannot run ahead of the assembler.
"""
import queue


class BufferPool:
    def __init__(self, count: int, slab_size: int):
        self._slab_size = slab_size
        self._free: "queue.Queue[bytearray]" = queue.Queue()
        for _ in range(count):
            self._free.put(bytearray(slab_size))

    @property
    def capacity(self) -> int:
        return self._slab_size

    def borrow(self, stop) -> bytearray | None:
        """Block until a slab is free, returning it. Stop-aware: if `stop` is set
        (abort), wake promptly and return None instead of blocking forever."""
        while not stop.is_set():
            try:
                return self._free.get(timeout=0.2)
            except queue.Empty:
                continue
        return None

    def release(self, slab: bytearray) -> None:
        self._free.put(slab)
