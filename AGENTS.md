# AGENTS.md — `demo-s3-archiving`

Focused on the only contributor task: **adding a contender Lambda**. For
broader context (deploy flow, Step Function internals, cleanup order),
read `README.md`. This file only captures what an agent would otherwise
get wrong.

## What a contender is

A Lambda that, in a single invocation, reads every object under
`s3://${bucket_name}/${files_prefix}/` and uploads a flat ZIP to
`s3://${bucket_name}/${archive_key}`. A Step Function times it and
invokes the internal `control` Lambda to validate the ZIP byte-for-byte.

## Runtime contract (do not deviate)

**Event payload** — read all three fields from the event:

```json
{
  "bucket_name": "<project>-<account>-<region>",
  "files_prefix": "files",
  "archive_key": "archives/<lang>-<dev_id>.zip"
}
```

- `bucket_name` — source + destination bucket
- `files_prefix` — source key prefix, without trailing slash
- `archive_key` — destination key the produced ZIP must be written to

These values are injected by the benching Step Function (see
`templates/benching.asl.json`, `InvokeContender` state).

**ZIP requirements** — `benching/control-lambda/` rejects anything else:

- Flat layout (no `/` in entry names)
- Exactly one entry per source object (no duplicates, no extras)
- Entry name == source S3 key basename == SHA256 hex of decompressed content
- Bit-exact content (control re-hashes and compares)

## Naming scheme (strict — Step Function relies on it)

Pick `<lang>` (e.g. `rust`, `python`, `go`) and `<dev_id>` (your GitHub
handle). Lowercase, hyphen-safe. Then **every** name below derives from
that pair:

| Where | Value |
|---|---|
| Source directory | `contenders/<lang>/<dev_id>/` |
| Cargo `[package].name` (Rust only) | `<dev_id>` — **must** equal directory name |
| Lambda `FunctionName` | `!Sub ${ProjectName}-<lang>-<dev_id>` |
| CFN logical IDs | `<Lang><DevId>Function`, `<Lang><DevId>FunctionLogGroup` (PascalCase, no hyphens) |
| Resulting archive key | `archives/<lang>-<dev_id>.zip` (derived by SFN from function ARN) |

If the Cargo package name does not match the directory name, CI silently
fails: it builds with `cargo lambda build --package <dir_name>` and
copies `target/lambda/<dir_name>/bootstrap`. Same trap exists for the
internal Lambdas — e.g. `benching/control-lambda/` package is
`control-lambda`, not `control`.

## The three edits to add a contender

All in one PR. There is no fourth file to touch unless your language is
new to CI (see next section).

1. **Source dir** `contenders/<lang>/<dev_id>/`. For Rust, the workspace
   root `Cargo.toml` already globs `contenders/rust/*`, so a new crate
   is picked up automatically.

2. **Two resources** in `templates/contenders.yml`, inserted between the
   `BEGIN CONTENDERS` / `END CONTENDERS` markers. Copy the
   `RustJeremieRodonFunction` + `RustJeremieRodonFunctionLogGroup` block
   and adapt logical IDs, `FunctionName`, `CodeUri`, `Runtime`,
   `Handler`, `Architectures`. **Do not** create a new IAM
   role — reuse `Role: !GetAtt LambdaContenderRole.Arn`.

3. **One line** in `Outputs.ContenderArns`, after the
   `INSERT YOUR CONTENDER ARN HERE` marker:

   ```yaml
   - !GetAtt <Lang><DevId>Function.Arn
   ```

### Runtime / Handler cheat sheet

| Language | `Runtime` | `Handler` |
|---|---|---|
| Rust (custom runtime) | `provided.al2023` | `rust.handler` (ignored — `bootstrap` is executed) |
| Go (custom runtime) | `provided.al2023` | `bootstrap` (ignored) |
| Python 3.x | `python3.x` | `index.handler` |
| Node.js 22.x | `nodejs22.x` | `index.handler` |
| Java 21 | `java21` | `pkg.Cls::method` |

## CI build for non-Rust languages

`ci-config/buildspec.yml` handles two languages out of the box:

- **Rust** — every dir under `contenders/rust/*` is built via
  `cargo lambda build --locked --release --arm64 --package <dev_id>`.
- **Python** — every dir under `contenders/python/*` is scanned for
  `requirements.txt`; if found, `pip install -r requirements.txt -t .`
  is run in place and the requirements file is removed.

For any other language, add a build step in the `build` phase. A
commented-out Go example under the `# GO BUILD` marker is the
copy-paste template.

**Critical CI gotcha**: after building each Rust/Go Lambda, the CI
**deletes everything in the source directory** and leaves only the
`bootstrap` binary:

```sh
rm -rf $LAMBDA_FOLDER/$LAMBDA/*
mv ./target/lambda/$LAMBDA/bootstrap $LAMBDA_FOLDER/$LAMBDA/bootstrap
```

Then `aws cloudformation package` zips that directory. So:

- The Lambda runtime ships **only** what your build leaves in
  `contenders/<lang>/<dev_id>/` at end of the `build` phase.
- Do not rely on source files, `Cargo.toml`, configs, fixtures, etc.
  being available at runtime unless your build step explicitly keeps
  them. For Java/Node, structure your build to leave fat JAR / bundled
  JS in place.

## Local validation (Rust)

The Rust workspace covers `benching/*` and `contenders/rust/*`:

```sh
cargo check                                              # everything
```

Toolchain is pinned to **1.94** in `nix/rust-toolchain.toml` (the root
`rust-toolchain.toml` is a symlink). Nix flake at `nix/` provides
`rustToolchain` + `cargo-lambda` + AWS CLI via `direnv` (`.envrc` is
`use flake path:./nix`). Without Nix: `rustup` + `cargo install
cargo-lambda`. Crate-level `rust-version = "1.85.0"` is a published MSRV,
not the pinned build toolchain — do not downgrade.

## Reference implementation

`contenders/rust/jrodon/` is both a working contender and the
copy-paste template. Keep its CFN block as the last entry in
`templates/contenders.yml` so newcomers always see a registered example.

## Things that look broken but are not

- `Handler: rust.handler` on a `provided.al2023` Lambda — ignored by
  the runtime, just a convention. Keep it when copying.
- `IAM` permissions look minimal but cover everything contenders need:
  `s3:GetObject` + `s3:ListBucket` scoped to the source prefix,
  multipart-upload perms scoped to `archives/*`, CloudWatch Logs.
  Do not add new statements unless your contender genuinely needs them
  (it almost certainly does not).
- `RUST_LOG: info` in the reference contender; a longer per-crate
  filter (commented out) mutes AWS SDK noise — use it as a starting
  point if you raise verbosity.
