"""Lean, stdlib-only S3 client (raw HTTPS + SigV4 + multipart upload), used to
avoid boto3's per-thread-client memory cost under the free-threaded build. One
http.client connection per worker thread (connections are not shared), so it is
free-threading-safe by construction. Implements the Storage / MultipartUpload
protocols so the archiver core uses it unchanged.

Credentials come from the standard Lambda env vars (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN). Bodies are sent/received without a
content hash (x-amz-content-sha256: UNSIGNED-PAYLOAD) to avoid hashing 5-10 MB
payloads.
"""
import datetime
import hashlib
import hmac
import http.client
import os
import random
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

from .storage import MultipartUpload, Storage

_ALGO = "AWS4-HMAC-SHA256"
_UNSIGNED = "UNSIGNED-PAYLOAD"

# --- retry policy for transient S3 failures (503 SlowDown, other 5xx, dropped
# connections). The cold files/ prefix 503-throttles under concurrent archive
# jobs; with only 2 immediate attempts a single object that double-503s aborts the
# whole run -- the random "1-of-N runs fails" we see under the benchmark's parallel
# fan-out. boto3 retries 503 with exponential backoff over several attempts; since
# we deleted boto3 we reproduce that here. All knobs are env-tunable. ---
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = max(1, int(os.environ.get("S3_MAX_ATTEMPTS", "6")))
_BACKOFF_BASE = float(os.environ.get("S3_BACKOFF_BASE", "0.05"))  # seconds, first retry
_BACKOFF_CAP = float(os.environ.get("S3_BACKOFF_CAP", "2.0"))     # seconds, per-sleep cap


class _Retry(Exception):
    """Internal signal that an attempt failed in a retryable way. Carries the
    underlying error as __cause__ so the final give-up raise is meaningful."""


def _backoff(attempt: int) -> None:
    """Full-jitter exponential backoff (AWS's recommended scheme): sleep a random
    duration in [0, min(cap, base * 2**attempt)). The jitter decorrelates the many
    concurrent workers' retries so they don't re-stampede the throttled prefix in
    lockstep -- which is what turns a transient 503 into a sustained one."""
    ceil = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** attempt))
    time.sleep(random.uniform(0, ceil))


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _signing_key(secret: str, datestamp: str, region: str, service: str = "s3") -> bytes:
    k = hmac.new(("AWS4" + secret).encode(), datestamp.encode(), hashlib.sha256).digest()
    for part in (region, service, "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    return k


def _enc_path(key: str) -> str:
    return "/".join(urllib.parse.quote(seg, safe="") for seg in key.split("/"))


def _canonical_query(query: dict) -> str:
    return "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(query.items()))


def _canonical_request(method: str, canon_uri: str, query: dict, headers: dict) -> str:
    canon_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    signed = ";".join(sorted(headers))
    return "\n".join([method, canon_uri, _canonical_query(query),
                      canon_headers, signed, _UNSIGNED])


class _Endpoint:
    """Resolves host/port/scheme and request path style. With endpoint_url set
    (MinIO) we use path-style http; otherwise virtual-hosted-style HTTPS S3."""

    def __init__(self, bucket: str, endpoint_url: str | None, region: str):
        self.bucket = bucket
        self.region = region
        if endpoint_url:
            u = urllib.parse.urlparse(endpoint_url)
            self.use_tls = u.scheme == "https"
            self.host = u.hostname
            self.port = u.port or (443 if self.use_tls else 80)
            self.path_style = True
        else:
            self.use_tls = True
            self.host = f"{bucket}.s3.{region}.amazonaws.com"
            self.port = 443
            self.path_style = False

    @property
    def host_header(self) -> str:
        default = 443 if self.use_tls else 80
        return self.host if self.port == default else f"{self.host}:{self.port}"

    def canon_uri(self, key: str) -> str:
        if self.path_style:
            return "/" + _enc_path(f"{self.bucket}/{key}" if key else self.bucket)
        return "/" + _enc_path(key)


class _Conn(threading.local):
    """Per-thread keep-alive connection holder (http.client conns aren't shared)."""
    conn = None


class RawS3Storage(Storage):
    def __init__(self, bucket: str, endpoint_url: str | None = None,
                 region: str = "us-east-1"):
        self._ep = _Endpoint(bucket, endpoint_url, region)
        self._access = os.environ["AWS_ACCESS_KEY_ID"]
        self._secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        self._token = os.environ.get("AWS_SESSION_TOKEN")
        self._tls = _Conn()

    def _new_conn(self):
        if self._ep.use_tls:
            return http.client.HTTPSConnection(self._ep.host, self._ep.port, timeout=60)
        return http.client.HTTPConnection(self._ep.host, self._ep.port, timeout=60)

    def _drop_conn(self):
        """Discard this thread's keep-alive connection. After any error a kept
        connection may hold an unread or partial response, so it can't be reused."""
        conn = self._tls.conn
        self._tls.conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _with_retries(self, do_attempt):
        """Run do_attempt() with bounded exponential-backoff retries. do_attempt
        performs ONE request and either returns a result, raises _Retry (transient
        -> drop the connection, back off, retry), or raises anything else (fatal,
        e.g. a 4xx that won't improve -> propagate immediately)."""
        for attempt in range(_MAX_ATTEMPTS):
            try:
                return do_attempt()
            except _Retry as r:
                self._drop_conn()
                if attempt + 1 >= _MAX_ATTEMPTS:
                    raise (r.__cause__ or r)
                _backoff(attempt)

    def _signed(self, method, key, query, body):
        """Build the signed (url, headers) for one request. Shared by _request
        (reads the whole body) and get_object_into (streams the body into a slab)."""
        now = datetime.datetime.now(datetime.UTC)
        amzdate = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        headers = {"host": self._ep.host_header,
                   "x-amz-content-sha256": _UNSIGNED, "x-amz-date": amzdate}
        if self._token:
            headers["x-amz-security-token"] = self._token
        canon_uri = self._ep.canon_uri(key)
        cr = _canonical_request(method, canon_uri, query, headers)
        scope = f"{datestamp}/{self._ep.region}/s3/aws4_request"
        sts = "\n".join([_ALGO, amzdate, scope, _sha256_hex(cr.encode())])
        sig = hmac.new(_signing_key(self._secret, datestamp, self._ep.region),
                       sts.encode(), hashlib.sha256).hexdigest()
        signed = ";".join(sorted(headers))
        headers["Authorization"] = (f"{_ALGO} Credential={self._access}/{scope}, "
                                    f"SignedHeaders={signed}, Signature={sig}")
        url = canon_uri + ("?" + _canonical_query(query) if query else "")
        return url, headers

    def _request(self, method, key, query=None, body=b""):
        query = query or {}
        url, headers = self._signed(method, key, query or {}, body)

        def attempt():
            conn = self._tls.conn or self._new_conn()
            self._tls.conn = conn
            fatal = None
            try:
                conn.request(method, url, body=body, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                if status in _RETRYABLE_STATUS:
                    raise _Retry() from OSError(f"S3 {method} {key} -> {status}: {data[:200]!r}")
                if status >= 400:                       # non-retryable 4xx: fail fast
                    fatal = OSError(f"S3 {method} {key} -> {status}: {data[:300]!r}")
                else:
                    result = (status, {k.lower(): v for k, v in resp.getheaders()}, data)
            except _Retry:
                raise
            except (http.client.HTTPException, OSError) as e:  # dropped conn: retry
                raise _Retry() from e
            if fatal is not None:
                raise fatal
            return result

        return self._with_retries(attempt)

    def list_keys(self, prefix: str) -> list[str]:
        keys, token = [], None
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        while True:
            q = {"list-type": "2", "prefix": prefix}
            if token:
                q["continuation-token"] = token
            _, _, body = self._request("GET", "", query=q)
            root = ET.fromstring(body)
            for c in root.findall(f"{ns}Contents"):
                k = c.findtext(f"{ns}Key")
                if k and not k.endswith("/"):
                    keys.append(k)
            if root.findtext(f"{ns}IsTruncated") == "true":
                token = root.findtext(f"{ns}NextContinuationToken")
            else:
                return keys

    def get_object(self, key: str) -> bytes:
        _, _, data = self._request("GET", key)
        return data

    def get_object_into(self, key: str, buf: bytearray) -> tuple[bytes | None, int]:
        """Zero-copy GET. If the object fits `buf`, stream its body straight into
        the slab via readinto and return (None, n) -- no fresh per-object bytes are
        allocated, which is the whole point of the pool. If the object is larger
        than the slab, read it into fresh bytes from the *same* response and return
        (data, n); the caller then uses `data` and leaves the slab unused. This
        keeps correctness for any object size without a wasted request."""
        url, headers = self._signed("GET", key, {}, b"")

        def attempt():
            conn = self._tls.conn or self._new_conn()
            self._tls.conn = conn
            fatal = None
            try:
                conn.request("GET", url, body=b"", headers=headers)
                resp = conn.getresponse()
                status = resp.status
                if status >= 400:
                    data = resp.read()
                    if status in _RETRYABLE_STATUS:
                        raise _Retry() from OSError(f"S3 GET {key} -> {status}: {data[:200]!r}")
                    fatal = OSError(f"S3 GET {key} -> {status}: {data[:300]!r}")
                    result = None
                else:
                    clen = int(resp.getheader("Content-Length", "-1"))
                    if 0 <= clen <= len(buf):
                        mv = memoryview(buf)
                        total = 0
                        while total < clen:
                            n = resp.readinto(mv[total:clen])
                            if n == 0:
                                break
                            total += n
                        if total != clen:  # premature EOF -> retry rather than truncate
                            raise OSError(f"S3 GET {key}: short read {total}/{clen}")
                        result = (None, total)
                    else:
                        data = resp.read()  # exceeds the slab: fall back to fresh bytes
                        result = (data, len(data))
            except _Retry:
                raise
            except (http.client.HTTPException, OSError) as e:  # dropped conn/short read
                raise _Retry() from e
            if fatal is not None:
                raise fatal
            return result

        return self._with_retries(attempt)

    def open_multipart_upload(self, key: str, n_upload: int = 4) -> MultipartUpload:
        return RawS3MultipartUpload(self, key, n_upload)


class RawS3MultipartUpload(MultipartUpload):
    def __init__(self, storage: RawS3Storage, key: str, n_upload: int):
        self._s = storage
        self._key = key
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        _, _, body = storage._request("POST", key, query={"uploads": ""})
        self._upload_id = ET.fromstring(body).findtext(f"{ns}UploadId")
        self._pool = ThreadPoolExecutor(max_workers=n_upload)
        self._inflight = threading.BoundedSemaphore(n_upload * 2)
        self._futures = []

    def _do_part(self, part_no: int, data: bytes) -> str:
        _, hdrs, _ = self._s._request(
            "PUT", self._key,
            query={"partNumber": str(part_no), "uploadId": self._upload_id}, body=data)
        return hdrs["etag"]

    def upload_part(self, part_no: int, data: bytes) -> None:
        self._inflight.acquire()
        fut = self._pool.submit(self._do_part, part_no, data)
        fut.add_done_callback(lambda f: self._inflight.release())
        self._futures.append((part_no, fut))

    def complete(self) -> None:
        try:
            parts = sorted(((pn, fut.result()) for pn, fut in self._futures))
            xml = ("<CompleteMultipartUpload>" + "".join(
                f"<Part><PartNumber>{pn}</PartNumber><ETag>{etag}</ETag></Part>"
                for pn, etag in parts) + "</CompleteMultipartUpload>")
            self._s._request("POST", self._key,
                             query={"uploadId": self._upload_id}, body=xml.encode())
        finally:
            self._pool.shutdown()

    def abort(self) -> None:
        self._pool.shutdown(cancel_futures=True)
        try:
            self._s._request("DELETE", self._key, query={"uploadId": self._upload_id})
        except OSError:
            pass
