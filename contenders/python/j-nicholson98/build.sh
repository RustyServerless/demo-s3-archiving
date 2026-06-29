#!/usr/bin/env bash
# Build the contender bundle: download the free-threaded CPython 3.14 standalone
# interpreter into runtime/. The archiver/ package (stdlib-only raw-S3 + SigV4
# client and streaming ZIP64 writer) is committed alongside -- no pip deps, no
# boto3. Runs directly on an AL2023 aarch64 host (CodeBuild) or in a local
# `amazonlinux:2023` arm64 container; the Lambda runs the same OS/arch.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PY_URL="https://github.com/astral-sh/python-build-standalone/releases/download/20260602/cpython-3.14.5%2B20260602-aarch64-unknown-linux-gnu-freethreaded-install_only_stripped.tar.gz"
PBS="$HERE/.pbs"
rm -rf "$HERE/runtime" "$PBS" && mkdir -p "$PBS"
curl -fsSL "$PY_URL" -o "$PBS/py.tgz"
tar -xzf "$PBS/py.tgz" -C "$PBS"
mv "$PBS/python" "$HERE/runtime"
rm -rf "$PBS"
find "$HERE/runtime" "$HERE/archiver" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
du -sh "$HERE/runtime"
echo "bundle ready at $HERE"
