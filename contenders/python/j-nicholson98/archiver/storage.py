"""Storage abstraction with two backends. The core pipeline depends only on the
Storage / MultipartUpload protocols; it never branches on backend."""
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


class MultipartUpload(ABC):
    @abstractmethod
    def upload_part(self, part_no: int, data: bytes) -> None: ...
    @abstractmethod
    def complete(self) -> None: ...
    @abstractmethod
    def abort(self) -> None: ...


class Storage(ABC):
    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...
    @abstractmethod
    def get_object(self, key: str) -> bytes: ...

    def get_object_into(self, key: str, buf: bytearray) -> tuple[bytes | None, int]:
        """Read an object into the caller's slab when the backend supports it,
        returning (None, n) for the zero-copy case. The default here can't read
        into `buf`, so it returns (data, n): the caller uses the bytes and leaves
        the slab unused. RawS3Storage overrides this with a real readinto path --
        that override is what flattens download-side memory under high concurrency."""
        data = self.get_object(key)
        return data, len(data)

    @abstractmethod
    def open_multipart_upload(self, key: str, n_upload: int = 4) -> MultipartUpload: ...


class PartChunker:
    """Accumulates the ZIP byte stream and flushes fixed-size parts in order.
    All non-final parts are exactly `part_size` (>= 5 MB for S3); final part may
    be smaller. Implements write(bytes)/tell() so a Zip64Assembler can target it."""

    def __init__(self, mp: MultipartUpload, part_size: int):
        self._mp = mp
        self._part_size = part_size
        self._buf = bytearray()
        self._part_no = 0
        self._total = 0

    def write(self, data: bytes) -> None:
        self._buf += data
        self._total += len(data)
        while len(self._buf) >= self._part_size:
            self._part_no += 1
            part = bytes(self._buf[:self._part_size])
            del self._buf[:self._part_size]
            self._mp.upload_part(self._part_no, part)

    def tell(self) -> int:
        return self._total

    def flush_final(self) -> None:
        # Always emit at least one part (handles archives smaller than part_size).
        if self._buf or self._part_no == 0:
            self._part_no += 1
            self._mp.upload_part(self._part_no, bytes(self._buf))
            self._buf.clear()


class LocalMultipartUpload(MultipartUpload):
    """Parts arrive in order from PartChunker (single assembler thread), so a plain
    ordered append is correct. A lock guards against accidental concurrent calls."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "wb")
        self._lock = threading.Lock()

    def upload_part(self, part_no: int, data: bytes) -> None:
        with self._lock:
            self._f.write(data)

    def complete(self) -> None:
        self._f.close()

    def abort(self) -> None:
        self._f.close()
        self._path.unlink(missing_ok=True)


class LocalDirStorage(Storage):
    def __init__(self, root, dst_path):
        self._root = Path(root)
        self._dst = Path(dst_path)

    def list_keys(self, prefix: str) -> list[str]:
        base = self._root / prefix
        return [
            str(p.relative_to(self._root))
            for p in base.iterdir()
            if p.is_file()
        ]

    def get_object(self, key: str) -> bytes:
        return (self._root / key).read_bytes()

    def open_multipart_upload(self, key: str, n_upload: int = 4) -> MultipartUpload:
        return LocalMultipartUpload(self._dst)


class S3MultipartUpload(MultipartUpload):
    """Uploads parts concurrently via a thread pool; each pool thread uses its own
    boto3 client (clients are not safe to share across no-GIL threads)."""

    def __init__(self, storage: "S3Storage", key: str, n_upload: int):
        self._storage = storage
        self._bucket = storage._bucket
        self._key = key
        client = storage._client()
        self._upload_id = client.create_multipart_upload(
            Bucket=self._bucket, Key=key)["UploadId"]
        self._pool = ThreadPoolExecutor(max_workers=n_upload)
        self._futures = []  # list[(part_no, Future[str])]
        # Backpressure: cap in-flight upload parts so peak upload-side memory is
        # O(n_upload) parts, not O(total parts). upload_part() blocks when full.
        self._inflight = threading.BoundedSemaphore(n_upload * 2)

    def _do_part(self, part_no: int, data: bytes) -> str:
        resp = self._storage._client().upload_part(
            Bucket=self._bucket, Key=self._key,
            PartNumber=part_no, UploadId=self._upload_id, Body=data)
        return resp["ETag"]

    def upload_part(self, part_no: int, data: bytes) -> None:
        self._inflight.acquire()  # blocks the assembler if uploads fall behind
        fut = self._pool.submit(self._do_part, part_no, data)
        fut.add_done_callback(lambda f: self._inflight.release())
        self._futures.append((part_no, fut))

    def complete(self) -> None:
        try:
            parts = [{"PartNumber": pn, "ETag": fut.result()}
                     for pn, fut in self._futures]
            parts.sort(key=lambda p: p["PartNumber"])
            self._storage._client().complete_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
                MultipartUpload={"Parts": parts})
        finally:
            self._pool.shutdown()

    def abort(self) -> None:
        self._pool.shutdown(cancel_futures=True)
        self._storage._client().abort_multipart_upload(
            Bucket=self._bucket, Key=self._key, UploadId=self._upload_id)


class S3Storage(Storage):
    """boto3 backend. endpoint_url points at MinIO locally and real S3 in prod.

    ONE client per thread. Sharing a single botocore client across free-threaded
    (no-GIL) worker threads corrupts the connection pool -- observed as
    urllib3 IncompleteRead / ResponseStreamingError races that the GIL used to
    mask. So each worker thread gets its own client (own connection pool), which
    is safe but memory-heavy: this per-client overhead is the dominant reason the
    boto3-based archiver does not fit the 512 MB Lambda tier at high concurrency.
    """

    def __init__(self, bucket: str, endpoint_url: str | None = None,
                 region: str = "us-east-1"):
        self._bucket = bucket
        self._endpoint = endpoint_url
        self._region = region
        self._tls = threading.local()

    def _client(self):
        c = getattr(self._tls, "client", None)
        if c is None:
            import boto3
            c = boto3.client("s3", endpoint_url=self._endpoint,
                             region_name=self._region)
            self._tls.client = c
        return c

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        paginator = self._client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])
        return keys

    def get_object(self, key: str) -> bytes:
        return self._client().get_object(Bucket=self._bucket, Key=key)["Body"].read()

    def open_multipart_upload(self, key: str, n_upload: int = 4) -> MultipartUpload:
        return S3MultipartUpload(self, key, n_upload)
