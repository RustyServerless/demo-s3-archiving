"""AWS Lambda entry point. The same function is called by the local harness and
by Lambda. Tunables come from the environment so the memory/duration frontier can
be swept without code changes."""
import gc
import os

from .archive import DEFAULT_PART_SIZE, DEFAULT_SLAB_SIZE, archive


def tune_gc():
    """Disable cyclic GC for the one-shot Lambda invoke. The pipeline has no
    reference cycles, so refcounting frees every object the moment it's done with
    (peak stays pool-bounded); the cyclic collector would only contribute
    stop-the-world pauses that stall every worker mid-transfer -- the run-to-run
    variance that drags our mean above our min. freeze() moves already-loaded
    objects out of the scan set so any later collection stays cheap. Idempotent."""
    gc.disable()
    gc.freeze()


def maybe_tune_gc():
    """Apply tune_gc() only when GC_DISABLE=1. Default OFF: on the free-threaded
    build the cyclic GC also drives memory reclamation (QSBR / deferred refcount),
    so disabling it is not a guaranteed win -- it must be an opt-in, measured flag.
    Returns True if GC was disabled."""
    if os.environ.get("GC_DISABLE", "0") == "1":
        tune_gc()
        return True
    return False


def _make_storage(event):
    bucket = event["bucket_name"]
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION", "us-east-1")
    if os.environ.get("S3_CLIENT", "boto3").lower() == "raw":
        from .raw_s3 import RawS3Storage
        return RawS3Storage(bucket, endpoint_url=endpoint, region=region)
    from .storage import S3Storage
    return S3Storage(bucket, endpoint_url=endpoint, region=region)


def _arm_watchdog(context):
    """Diagnostic (WATCHDOG=1): if the invoke is still running ~90s before the
    Lambda timeout, print every thread's stack to stdout (reliably captured by
    CloudWatch, unlike a stderr dump at T-12s which is lost on SIGKILL). Three
    shots 15s apart reveal whether threads are MOVING (slow) or FROZEN (deadlock).
    No-op by default."""
    if os.environ.get("WATCHDOG") != "1":
        return lambda: None
    import sys
    import threading
    import traceback
    try:
        remaining = context.get_remaining_time_in_millis() / 1000.0
    except Exception:
        remaining = 600.0
    stop = threading.Event()
    delay = max(5.0, remaining - 90.0)

    def watch():
        if stop.wait(delay):
            return
        for shot in range(3):
            if stop.is_set():
                return
            frames = sys._current_frames()
            print(f"=== WATCHDOG shot {shot}: {len(frames)} threads ===",
                  flush=True)
            for tid, frame in frames.items():
                stk = "".join(traceback.format_stack(frame))
                print(f"--- thread {tid} ---\n{stk}", flush=True)
            if stop.wait(15):
                return

    threading.Thread(target=watch, daemon=True).start()
    return stop.set


def handler(event, context):
    cancel_watchdog = _arm_watchdog(context)
    try:
        storage = _make_storage(event)
        src_prefix = event["files_prefix"].rstrip("/") + "/"
        # POOL_SIZE defaults (None) to queue_depth + n_download + 2 in archive().
        pool_env = os.environ.get("POOL_SIZE")
        result = archive(
            storage, src_prefix, event["archive_key"],
            n_download=int(os.environ.get("N_DOWNLOAD", "4")),
            n_upload=int(os.environ.get("N_UPLOAD", "4")),
            part_size=int(os.environ.get("PART_SIZE", str(DEFAULT_PART_SIZE))),
            queue_depth=int(os.environ.get("QUEUE_DEPTH", "8")),
            slab_size=int(float(os.environ.get(
                "SLAB_MB", str(DEFAULT_SLAB_SIZE / (1024 * 1024)))) * 1024 * 1024),
            pool_size=int(pool_env) if pool_env else None,
        )
        return {"statusCode": 200, **result}
    finally:
        cancel_watchdog()
