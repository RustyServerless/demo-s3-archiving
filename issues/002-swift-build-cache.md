# Issue #2 — Swift Build Cache

Ref: https://github.com/RustyServerless/demo-s3-archiving/issues/2

## Problem

Swift builds re-execute entirely on every CI run, ignoring the CodeBuild
S3 cache. The previous approach (caching `contenders/swift/*/.build/**/*`
directly) was disabled because it bloated the cache without helping.

### Root cause

`install.finally` and `build.finally` in `ci-config/buildspec.yml` reset
all file timestamps to `2010-01-01 00:00:00`:

```bash
find . -not -path '*/.git/*' -exec touch -a -m -t"201001010000.00" {} \;
```

SwiftPM uses mtime-based incremental compilation. After the touch, every
source file and build artifact has the same timestamp, so SwiftPM either
rebuilds everything or skips everything incorrectly. The raw `.build`
directory is useless once timestamps are flattened.

This is the same class of problem Rust had (cargo also uses timestamps),
which required the elaborate dependency-tree-diffing logic in `pre_build`.

## Solution

Tar the `.build` directory (preserving internal timestamps) after each
build, and restore it before the next build. The tar's contents retain
correct timestamps regardless of the `touch` commands that run after.

Cache validity is keyed on `sha256(Package.swift)` — if dependencies
change, the cache is invalidated and a full rebuild occurs.

### Storage

- `.swift-build-cache/<contender>.tar` — the tarred `.build` directory
- `.swift-build-cache/<contender>.package-hash` — the SHA-256 of
  `Package.swift` at the time the tar was created

Both are persisted via CodeBuild S3 cache paths.

### Flow per contender

1. Compute `sha256sum Package.swift`
2. Compare with stored hash:
   - **Match** → extract tar → SwiftPM incremental build
   - **Mismatch or missing** → full rebuild
3. After build: create new tar + save hash
4. Continue with existing packaging (strip, copy bootstrap, delete sources)

### Why this works

- The tar preserves timestamps *inside* it regardless of the tar file's
  own mtime on disk
- We extract the tar immediately before `swift build`, overwriting any
  touched `.build` with correct timestamps
- After build, we save a new tar (still with correct timestamps) before
  `build.finally` touches things again
- CodeBuild S3 cache persists the tar files between builds

## Changes

Single file: `ci-config/buildspec.yml`

1. **Swift build section**: cache-restore before `swift build`, cache-save
   after
2. **Cache paths**: added `.swift-build-cache/**/*` and
   `.swift-cache/**/*` (toolchain tarball, already downloaded but not
   previously persisted)

## Verification

| Build | Expected behavior |
|---|---|
| 1st (cold) | Cache miss, full build, tar created |
| 2nd (no Package.swift change) | Cache hit, tar extracted, incremental build (faster) |
| 3rd (Package.swift modified) | Cache miss (hash mismatch), full rebuild, new tar saved |
