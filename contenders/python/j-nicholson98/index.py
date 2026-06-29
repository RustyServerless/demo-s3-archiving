"""Contest contender entry point + pure-Python Lambda Runtime API loop.

We deliberately do NOT use awslambdaric: its compiled `runtime_client` extension
is not marked free-threading-safe, so importing it re-enables the GIL on the
freethreaded build (observed on Lambda: gil_enabled=True). This small pure-Python
loop speaks the Lambda Runtime API directly over stdlib urllib, keeping the GIL
disabled -- which is the entire point of this contender.

`handler` is also importable directly (index.handler) for tests / other runtimes.
"""
import gc
import json
import os
import sys
import urllib.request

from archiver.handler import handler, maybe_tune_gc  # handler re-exported as index.handler


def _runtime_loop():
    api = os.environ["AWS_LAMBDA_RUNTIME_API"]
    base = f"http://{api}/2018-06-01/runtime"
    while True:
        with urllib.request.urlopen(f"{base}/invocation/next") as resp:
            request_id = resp.headers["Lambda-Runtime-Aws-Request-Id"]
            event = json.loads(resp.read() or b"{}")
        try:
            result = handler(event, None)
            path = f"{base}/invocation/{request_id}/response"
            body = json.dumps(result).encode()
        except Exception as exc:  # report and continue to the next invocation
            path = f"{base}/invocation/{request_id}/error"
            body = json.dumps({"errorType": type(exc).__name__,
                               "errorMessage": str(exc)}).encode()
        urllib.request.urlopen(
            urllib.request.Request(path, data=body, method="POST")).read()


if __name__ == "__main__":
    maybe_tune_gc()  # disable cyclic GC at cold start ONLY if GC_DISABLE=1 (measured opt-in)
    print(f"[cold-start] python={sys.version.split()[0]} abiflags={sys.abiflags!r} "
          f"gil_enabled={sys._is_gil_enabled()} gc_enabled={gc.isenabled()}", flush=True)
    _runtime_loop()
