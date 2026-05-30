#!/usr/bin/env bash
# Reproduces the buildspec install + Swift build phase locally inside a stock
# `amazonlinux:2023` container, the same OS as CodeBuild's
# `aws/codebuild/amazonlinux-aarch64-standard:4.0` image.
#
# Usage (from the host):
#
#   docker run --rm --platform linux/arm64 \
#     -v "$(pwd):/work" -w /work \
#     amazonlinux:2023 \
#     bash scripts/test-codebuild-locally.sh
#
# No `set -e` on purpose — failures should surface but the script must keep
# going so we always see the final ldd / file output even if a step fails.

echo "================================================================"
echo " STAGE 1 — install Swift 6.3.2 (mirrors buildspec install phase)"
echo "================================================================"

dnf -y install \
  binutils gcc git unzip glibc-static gzip libbsd \
  libcurl-devel libedit libicu libstdc++-static libuuid \
  libxml2-devel openssl-devel tar tzdata
dnf -y swap gnupg2-minimal gnupg2-full

SWIFT_VERSION=swift-6.3.2-RELEASE
SWIFT_BRANCH=swift-6.3.2-release
SWIFT_PLATFORM=amazonlinux2023
SWIFT_ARCH_SUFFIX=-aarch64
SWIFT_BIN_URL="https://download.swift.org/${SWIFT_BRANCH}/${SWIFT_PLATFORM}${SWIFT_ARCH_SUFFIX}/${SWIFT_VERSION}/${SWIFT_VERSION}-${SWIFT_PLATFORM}${SWIFT_ARCH_SUFFIX}.tar.gz"
SWIFT_SIG_URL="${SWIFT_BIN_URL}.sig"
# GPG fingerprint of the official Swift 6.x release signing key
# (https://swift.org/keys/all-keys.asc). Public; not a secret.
SWIFT_GPG_FINGERPRINT=52BB7E3DE28A71BE22EC05FFEF80A866B47A981F

export GNUPGHOME="$(mktemp -d)"
curl -fsSL "$SWIFT_BIN_URL" -o swift.tar.gz
curl -fsSL "$SWIFT_SIG_URL" -o swift.tar.gz.sig
gpg --batch --quiet --keyserver keyserver.ubuntu.com --recv-keys "$SWIFT_GPG_FINGERPRINT"
gpg --batch --verify swift.tar.gz.sig swift.tar.gz
tar -xzf swift.tar.gz --directory / --strip-components=1
chmod -R o+r /usr/lib/swift
rm -rf "$GNUPGHOME" swift.tar.gz swift.tar.gz.sig
unset GNUPGHOME
swift --version

echo
echo "================================================================"
echo " STAGE 2 — build (mirrors buildspec SWIFT BUILD phase)"
echo "================================================================"

# Locally we mount the package directory as /work. In CodeBuild, the
# buildspec iterates over `./contenders/swift/*` from the repo root.
# Both paths converge on the same `swift build` invocation below.
cd /work || { echo "ERROR: /work mount missing"; exit 0; }

swift package clean
swift build -c release --static-swift-stdlib -Xswiftc -Osize
BUILD_RC=$?
echo "swift build exit code: $BUILD_RC"

BIN=$(swift build -c release --static-swift-stdlib --show-bin-path)/bootstrap
echo
echo "----- binary -----"
ls -la "$BIN" 2>&1
file "$BIN" 2>&1
echo
echo "----- ldd -----"
ldd "$BIN" 2>&1
echo
echo "----- size after strip -----"
strip "$BIN" 2>/dev/null
ls -la "$BIN" 2>&1
echo

echo "================================================================"
echo " STAGE 3 — runtime check on a no-Swift environment"
echo "================================================================"
# Save the binary for a follow-up `docker run` against a stock AL2023
# image WITHOUT a Swift install — that's what proves --static-swift-stdlib
# is enough for the Lambda runtime.
mkdir -p /work/scripts/out
cp "$BIN" /work/scripts/out/bootstrap 2>&1
echo "wrote /work/scripts/out/bootstrap"
echo
echo "Run this on the host to verify it loads on a clean AL2023:"
echo
echo "  docker run --rm --platform linux/arm64 \\"
echo "    -v \"\$(pwd)/scripts/out:/lambda\" \\"
echo "    amazonlinux:2023 \\"
echo "    bash -c 'file /lambda/bootstrap; ldd /lambda/bootstrap; AWS_LAMBDA_RUNTIME_API=127.0.0.1:9999 timeout 3 /lambda/bootstrap; echo exit=\$?'"
