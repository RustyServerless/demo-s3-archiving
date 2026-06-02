"""Python contender Lambda for the demo-s3-archiving benchmark.

Reads every object under s3://{bucket}/{files_prefix}/, streams it into a
flat uncompressed ZIP, and writes the result to s3://{bucket}/{archive_key}
via S3 multipart upload.

Pipeline (bounded producer-consumer with REAL backpressure):
  - Download stage: a fixed pool of N worker threads pulls files off a
    `work_q` and pushes (FileInfo, bytes) onto a BOUNDED `done_q`. When
    `done_q` is full the workers BLOCK on `done_q.put()`, which is the
    actual mechanism that caps in-flight memory. A naive
    ThreadPoolExecutor with `as_completed` does NOT do this: the pool
    only bounds concurrent requests, not completed-but-unconsumed
    results, so on a 15 GB workload the workers race ahead of the muxer
    and the futures pin every downloaded body in RAM. (We learned this
    the hard way: that pattern OOMs at 1022/1024 MB before producing a
    single zip entry.)
  - Zip stage: a single muxer (the main thread) drains `done_q` and
    writes each entry via zipfile.ZipFile in ZIP_STORED mode. Entry
    order is whichever finishes first; the control Lambda only checks
    flat layout + presence + per-entry hash, not order.
  - Upload stage: zipfile writes flow into _MultipartSink, which
    accumulates bytes and dispatches 8 MiB parts to a small upload
    thread pool.
"""

import io
import logging
import os
import queue
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import boto3
from botocore.config import Config

# ---------- Tunables ----------

# S3 multipart minimum is 5 MiB for non-final parts. 8 MiB balances peak
# part-buffer memory against the 10 000-part cap.
UPLOAD_PART_SIZE = 8 * 1024 * 1024

# Concurrent S3 GETs.
MAX_DOWNLOAD_WORKERS = 16

# Concurrent UploadPart calls. Parts are 8 MiB so 4 in flight overlaps
# upload latency without piling up.
MAX_UPLOAD_WORKERS = 4

# Bounded handoff queue between downloaders and the muxer. Caps
# in-flight downloaded bytes: workers block on q.put() when full.
# 16 slots * ~5 MB avg ≈ 80 MB.
DONE_QUEUE_DEPTH = 16

# Bounded upload backlog. Caps how many ready-to-upload parts can pile
# up between the muxer and the upload workers. The muxer (zipfile copy
# + 8 MiB part assembly) is faster than UploadPart latency, so without
# this bound completed parts queue forever and pin RAM. 4 slots ×
# UPLOAD_PART_SIZE (8 MiB) = 32 MB ceiling for the upload backlog.
UPLOAD_BACKLOG_DEPTH = 4

# ---------- Logging ----------

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# ---------- AWS client ----------

# `parameter_validation=False`: skips boto3's per-call schema check
# (~0.5 ms × thousands of calls). `tcp_keepalive=True`: improves
# connection reuse across the burst of GETs.
_s3 = boto3.client(
    "s3",
    config=Config(
        max_pool_connections=MAX_DOWNLOAD_WORKERS + MAX_UPLOAD_WORKERS + 4,
        retries={"max_attempts": 3, "mode": "standard"},
        parameter_validation=False,
        tcp_keepalive=True,
    ),
)


@dataclass
class FileInfo:
    name: str  # ZIP entry name (S3 key basename)
    key: str
    size: int


def list_files(bucket: str, files_prefix: str) -> list[FileInfo]:
    s3_prefix = f"{files_prefix}/"
    files: list[FileInfo] = []
    for page in _s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=s3_prefix
    ):
        for obj in page.get("Contents", ()):
            key = obj["Key"]
            if key == s3_prefix:
                continue
            name = key[len(s3_prefix):]
            if name:
                files.append(FileInfo(name=name, key=key, size=int(obj["Size"])))
    return files


# ---------- Streaming sink → S3 multipart ----------


class _MultipartSink(io.RawIOBase):
    """Writable file-like that splits its byte stream into S3 multipart parts.

    Uses a list-of-bytes accumulator rather than a single bytearray. A
    bytearray buffer would force `del buf[:N]` after every part, which is
    O(remaining) — multiplied across ~250 parts on a 15 GB stream, that's
    enough avoidable memcpy to dominate the muxer's CPU time. With a list,
    each part boundary splits at most one chunk and concatenates only what
    leaves the buffer.
    """

    def __init__(self, bucket, key, upload_id, executor, backlog_depth: int):
        super().__init__()
        self._bucket = bucket
        self._key = key
        self._upload_id = upload_id
        self._executor = executor
        self._chunks: list[bytes] = []
        self._buffered = 0
        self._part_number = 1
        self._futures = []
        # The muxer outpaces UploadPart latency, so without a bound,
        # ready parts pile up in the executor queue (each holding 8 MiB).
        # This semaphore caps the count of dispatched-but-not-finished
        # parts; the muxer blocks here if uploaders are behind.
        self._backlog = threading.BoundedSemaphore(backlog_depth)

    def writable(self):
        return True

    def tell(self):
        # zipfile reads tell() to compute offsets in the central directory.
        # All dispatched parts are exactly UPLOAD_PART_SIZE (only the final
        # part, written from close(), is short).
        return (self._part_number - 1) * UPLOAD_PART_SIZE + self._buffered

    def write(self, b):
        if not isinstance(b, bytes):
            b = bytes(b)
        n = len(b)
        if not n:
            return 0
        self._chunks.append(b)
        self._buffered += n
        while self._buffered >= UPLOAD_PART_SIZE:
            self._emit(UPLOAD_PART_SIZE)
        return n

    def close(self):
        if self.closed:
            return
        if self._buffered:
            self._emit(self._buffered)
        super().close()

    def _emit(self, size: int) -> None:
        # Pull exactly `size` bytes off the front of self._chunks. At most
        # one chunk gets sliced; the rest are forwarded by reference.
        needed = size
        pieces: list[bytes] = []
        chunks = self._chunks
        while needed > 0:
            head = chunks[0]
            head_len = len(head)
            if head_len <= needed:
                pieces.append(head)
                needed -= head_len
                chunks.pop(0)
            else:
                pieces.append(head[:needed])
                chunks[0] = head[needed:]
                needed = 0
        part = pieces[0] if len(pieces) == 1 else b"".join(pieces)
        self._buffered -= size
        pn = self._part_number
        self._part_number += 1
        # Block here if uploaders are too far behind. The semaphore is
        # released by _upload_part when the part finishes uploading,
        # capping in-flight upload memory at backlog_depth × part_size.
        self._backlog.acquire()
        self._futures.append(self._executor.submit(self._upload_part, pn, part))

    def _upload_part(self, part_number: int, data: bytes) -> dict:
        try:
            resp = _s3.upload_part(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._upload_id,
                PartNumber=part_number,
                Body=data,
            )
            return {"PartNumber": part_number, "ETag": resp["ETag"]}
        finally:
            self._backlog.release()

    def completed_parts(self) -> list[dict]:
        parts = [f.result() for f in self._futures]
        parts.sort(key=lambda p: p["PartNumber"])
        return parts


# ---------- Pipeline ----------

# Sentinel values on the queues. `_DONE` ends a downloader's loop;
# `_END` tells the muxer there are no more downloads.
_DONE = object()
_END = object()


def _download_worker(
    bucket: str,
    work_q: queue.Queue,
    done_q: queue.Queue,
    error_box: list,
    stop: threading.Event,
) -> None:
    while not stop.is_set():
        item = work_q.get()
        if item is _DONE:
            return
        try:
            obj = _s3.get_object(Bucket=bucket, Key=item.key)
            data = obj["Body"].read()
            done_q.put((item, data))  # blocks when queue full → backpressure
        except BaseException as e:  # noqa: BLE001
            error_box.append(e)
            stop.set()
            return


def _create_archive(bucket: str, archive_key: str, files: list[FileInfo]) -> None:
    create = _s3.create_multipart_upload(
        Bucket=bucket, Key=archive_key, ContentType="application/zip"
    )
    upload_id = create["UploadId"]
    logger.info("multipart upload started: %s", upload_id)

    upload_pool = ThreadPoolExecutor(
        max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix="up"
    )
    sink = _MultipartSink(bucket, archive_key, upload_id, upload_pool, UPLOAD_BACKLOG_DEPTH)

    work_q: queue.Queue = queue.Queue()
    done_q: queue.Queue = queue.Queue(maxsize=DONE_QUEUE_DEPTH)
    error_box: list = []
    stop = threading.Event()

    workers = [
        threading.Thread(
            target=_download_worker,
            args=(bucket, work_q, done_q, error_box, stop),
            name=f"dl-{i}",
            daemon=True,
        )
        for i in range(MAX_DOWNLOAD_WORKERS)
    ]
    for w in workers:
        w.start()
    for f in files:
        work_q.put(f)
    for _ in workers:
        work_q.put(_DONE)

    try:
        total = len(files)
        log_every = max(1, total // 30)
        consumed = 0

        with zipfile.ZipFile(
            sink, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True
        ) as zf:
            while consumed < total:
                if error_box:
                    raise error_box[0]
                file, data = done_q.get()
                zf.writestr(file.name, data)
                # Drop the local reference promptly so the bytes can be
                # GC'd as soon as zipfile finishes copying them into its
                # internal buffer; otherwise the muxer's stack frame
                # holds a 5-8 MB body across the next .get() that may
                # block.
                del data
                consumed += 1
                if consumed % log_every == 0:
                    logger.info("zipped %d/%d", consumed, total)

        sink.close()
        parts = sink.completed_parts()

        _s3.complete_multipart_upload(
            Bucket=bucket,
            Key=archive_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info("multipart upload completed (%d parts)", len(parts))
    except BaseException:
        stop.set()
        try:
            _s3.abort_multipart_upload(
                Bucket=bucket, Key=archive_key, UploadId=upload_id
            )
        except Exception:
            logger.exception("abort_multipart_upload failed")
        raise
    finally:
        stop.set()
        # Drain done_q so any blocked workers can exit.
        while True:
            try:
                done_q.get_nowait()
            except queue.Empty:
                break
        for w in workers:
            w.join(timeout=5)
        upload_pool.shutdown(wait=True)


# ---------- Lambda entrypoint ----------


def handler(event, _context):
    bucket = event["bucket_name"]
    files_prefix = event["files_prefix"]
    archive_key = event["archive_key"]

    logger.info(
        "start: bucket=%s prefix=%s archive=%s",
        bucket, files_prefix, archive_key,
    )
    files = list_files(bucket, files_prefix)
    logger.info("found %d files", len(files))

    _create_archive(bucket, archive_key, files)
    return {"ok": True, "files": len(files)}
