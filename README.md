<!-- markdownlint-disable MD029 MD033 MD041 -->
[![License](https://img.shields.io/github/license/RustyServerless/demo-s3-archiving.svg)](https://github.com/RustyServerless/demo-s3-archiving/blob/main/LICENSE)

# demo-s3-archiving

A friendly Lambda benchmark. The harness pre-fills an S3 bucket with **3 000
random objects (~15 GB total)** and exposes a Step Function that, for every
registered contender Lambda, measures how long it takes to read every object
and produce a single valid ZIP archive — in one Lambda invocation, within
strict resource limits.

You bring the Lambda; the harness times it, validates its archive byte-for-byte,
and ranks it against the others.

This repository is the companion code to a [blog post](https://rustysl.com/fr/blog/s3-on-demand-archive) on streaming S3 archive
creation in Rust on Lambda. The reference implementation is mine.

<details>
  <summary>Table of Contents</summary>

- [What it does](#what-it-does)
- [Deploying the benchmark](#deploying-the-benchmark)
  - [Prerequisites](#prerequisites)
  - [Step 1 — Fork the repo](#step-1--fork-the-repo)
  - [Step 2 — Create a CodeStar connection to GitHub](#step-2--create-a-codestar-connection-to-github)
  - [Step 3 — Deploy the CI stack](#step-3--deploy-the-ci-stack)
  - [Running the benchmark](#running-the-benchmark)
  - [Cleanup](#cleanup)
- [Writing a contender](#writing-a-contender)
  - [The contract](#the-contract)
  - [Registering your Lambda](#registering-your-lambda)
  - [CI build for non-Rust languages](#ci-build-for-non-rust-languages)
  - [Submitting a PR](#submitting-a-pr)
- [Project layout](#project-layout)
- [License](#license)
- [Contact](#contact)

</details>

## What it does

On stack creation, a custom resource (`fill-bucket`) populates an S3 bucket
with 3 000 objects under the `files/` prefix. Each object has a uniformly
random body sized from `N(5 MB, 1 MB)` clamped to `[2 MB, 8 MB]`, and its
S3 key is the SHA256 hex of its content.

A Step Function takes a list of contender Lambda ARNs as input and, for each
one in parallel (up to 10 concurrent):

1. Derives the per-contender archive key from the function name.
2. Invokes the contender Lambda with `{ "archive_key": "archives/<lang>-<dev_id>.zip" }`, timing the call.
3. Invokes the internal `control` Lambda, which streams the produced ZIP back
   from S3 and validates it (flat layout, one entry per source object, entry
   name equals SHA256 of decompressed content).
4. Deletes the archive.

The execution output is a single JSON document with two lists:

```json
{
  "success": [
    {
      "arn": "arn:aws:lambda:...:function:demo-s3-archiving-rust-jeremie-rodon",
      "runtime": "provided.al2023",
      "architecture": "arm64",
      "memory_mb": 512,
      "ephemeral_storage_mb": 512,
      "duration_ms": 28412,
      "gb_second": 14.206
    }
  ],
  "failure": [
    { "arn": "arn:aws:lambda:...:function:demo-s3-archiving-python-someone", "reason": "invalid: content hash mismatch for '...': computed ..." }
  ]
}
```

Ranking is by `gb_second` ascending — `(duration_ms / 1000) * (memory_mb / 1024)`.
That mirrors Lambda's compute pricing dimension: the cheapest run wins.

Successes are sorted from cheapest to costier. That's the ranking.

The number and average size of test objects (`TestFileCount`, `TestFileSize`)
are CloudFormation parameters of the `benching` stack — override them on stack
update if you want to play with the harness, but please don't include that in
your PRs.

## Deploying the benchmark

Two stacks are involved: a CI/CD stack (`<ProjectName>-ci`, deployed manually
once) and the actual benchmark stack (`<ProjectName>-root`, deployed by the
CI pipeline). You only ever deploy the first one yourself.

### Prerequisites

- An AWS account with permission to create CloudFormation, CodePipeline,
  CodeBuild, IAM, S3, Lambda, Step Functions and CloudWatch Logs resources.
- A GitHub account.

### Step 1 — Fork the repo

Fork this repository on GitHub. Note the resulting ID `<your-username>/demo-s3-archiving`;
you will need it (case-sensitive) below.

### Step 2 — Create a CodeStar connection to GitHub

If you already have one, reuse it and skip to step 3.

1. Open the CodePipeline console > **Settings > Connections**, choose
   **GitHub** as provider, name the connection, click **Connect to GitHub**.
2. Authorize AWS to act on your GitHub account, pick the AWS-created GitHub
   App, click **Connect**.
3. Copy the connection ARN.

Make sure your AWS console is set to the region you want to use throughout —
the same region must be used for every subsequent step.

### Step 3 — Deploy the CI stack

```bash
aws cloudformation create-stack \
  --stack-name demo-s3-archiving-ci \
  --template-body file://ci-template.yml \
  --parameters \
    ParameterKey=ProjectName,ParameterValue=demo-s3-archiving \
    ParameterKey=CodeStarConnectionArn,ParameterValue=YOUR_CONNECTION_ARN \
    ParameterKey=ForkedRepoId,ParameterValue=YOUR_USERNAME/demo-s3-archiving \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND
```

Or, via the console: create a new stack from `ci-template.yml`, fill the
parameters above, acknowledge IAM resource creation, submit.

The pipeline kicks off automatically. It builds every Rust Lambda, packages
the templates, then deploys the `demo-s3-archiving-root` stack which creates
the bucket, runs `fill-bucket`, and exposes the Step Function. First run takes
~10–20 minutes (cold cargo build); subsequent updates a couple of minutes
thanks to incremental caching.

### Running the benchmark

Once the root stack is `CREATE_COMPLETE`, fetch its outputs and start an
execution:

```bash
CONTENDERS=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`ContenderArns`].OutputValue' --output text)
SM=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`BenchingStateMachineArn`].OutputValue' --output text)

aws stepfunctions start-execution \
  --state-machine-arn "$SM" \
  --input "$CONTENDERS"
```

`ContenderArns` is already a JSON object of the shape the state machine expects
(`{"contenders":[...]}`), so it can be passed as-is. Watch the execution in
the Step Functions console and read the ranked output from its final state.

### Cleanup

The order matters — the root stack uses an IAM role created by the CI stack:

1. Delete `demo-s3-archiving-root` first. Wait for `DELETE_COMPLETE`.
2. Then delete `demo-s3-archiving-ci`.

Deleting them in parallel will fail and is annoying to unwind.

## Writing a contender

The repository ships with one reference contender:
[`contenders/rust/jeremie-rodon/`](contenders/rust/jeremie-rodon/). It is also
the copy-paste template for new ones in Rust. If you want to beat it (or just write
one in another language), the contract below is everything you need to know.

### The contract

**Event** — your handler is invoked with one JSON object:

```json
{ "archive_key": "archives/<lang>-<dev_id>.zip" }
```

Read `archive_key` from the event; do not recompute it.

**Environment variables** — always set by `templates/contenders.yml`:

| Variable | Meaning |
|---|---|
| `BUCKET_NAME` | S3 bucket holding both the source objects and your output archive |
| `FILES_PREFIX` | Key prefix of the source objects, no trailing slash (default `files`) |

**What your Lambda must do**:

1. List and read every object under `s3://${BUCKET_NAME}/${FILES_PREFIX}/`.
2. Produce a ZIP archive and upload it to `s3://${BUCKET_NAME}/${archive_key}`.

**Archive constraints** — the control Lambda rejects anything else:

- Flat layout — no `/` in any entry name.
- Exactly one entry per source object, no duplicates, no extras.
- Entry name == source object's S3 key basename == SHA256 hex of decompressed content.
- Bit-exact content (the control Lambda re-hashes and compares).

Failure modes are surfaced verbatim in the state machine output:

| Cause | Reported `reason` |
|---|---|
| Lambda crashed | `crash: <Error / Cause>` |
| Lambda timed out | `timeout: <Error / Cause>` |
| Nested path in archive | `invalid: archive contains nested path '...', flat layout required` |
| Hash mismatch | `invalid: content hash mismatch for '...': computed ...` |
| Unknown or duplicate entry | `invalid: unknown or duplicate object in archive: '...'` |
| Missing source objects | `invalid: archive missing N expected object(s) (sample: [...])` |

**IAM** — every contender shares the `LambdaContenderRole` defined in
`templates/contenders.yml`. It grants:

- `s3:GetObject` and `s3:ListBucket` on the source bucket, scoped to the
  `${FILES_PREFIX}/` prefix.
- `s3:PutObject`, `s3:AbortMultipartUpload`, `s3:ListMultipartUploadParts`,
  `s3:ListBucketMultipartUploads` on `${BUCKET_NAME}/archives/*`.
- Standard CloudWatch Logs.

**Resource limits** — yours to set. The reference contender uses
`provided.al2023`, ARM64, 512 MB of memory, 600 s timeout. Bumping memory
or switching architecture is fair game; but the winner will
be the cheapest run by Lambda pricing.

### Registering your Lambda

There is a strict naming scheme tying together the source directory, the
Lambda function name, and (for Rust) the cargo package name. Pick:

- `<lang>`: short name of your language (`rust`, `python`, `go`, `java`, …).
  Lowercase, hyphen-safe.
- `<dev_id>`: your GitHub username (or any stable identifier you control).
  Same charset.

| Where | Value | Example |
|---|---|---|
| Source directory | `contenders/<lang>/<dev_id>/` | `contenders/rust/jeremie-rodon/` |
| Cargo package name (Rust only) | `<dev_id>` (must equal directory name) | `jeremie-rodon` |
| Lambda function name | `${ProjectName}-<lang>-<dev_id>` | `demo-s3-archiving-rust-jeremie-rodon` |
| CFN logical ID prefix | `<Lang><DevId>` (PascalCase, no hyphens) | `RustJeremieRodon` |

Adding a contender is **three edits**:

**1. Drop your sources** under `contenders/<lang>/<dev_id>/`. For Rust, the
   workspace at the repo root already includes `contenders/rust/*`, so the
   crate is picked up automatically — but its `[package].name` MUST equal
   `<dev_id>` (the CI uses it to locate the compiled binary).

**2. Add two resources** in [`templates/contenders.yml`](templates/contenders.yml),
   inside the `BEGIN/END CONTENDERS` markers. Copy the `RustJeremieRodonFunction`
   block and adapt logical IDs, `FunctionName`, `CodeUri`, `Runtime`, `Handler`,
   `Architectures`. Common runtime/handler pairs:

| Language | `Runtime` | `Handler` |
|---|---|---|
| Rust / Go / any compiled language | `provided.al2023` | ignored (`bootstrap` is executed) |
| Python 3.13 | `python3.13` | `index.handler` |
| Node.js 22.x | `nodejs22.x` | `index.handler` |
| Java 21 | `java21` | `com.example.MyHandler::handleRequest` |

**3. Add one line** in `Outputs.ContenderArns` of the same file, after the
   `INSERT YOUR CONTENDER ARN HERE` marker:

```yaml
- !GetAtt <Lang><DevId>Function.Arn
```

That's it for the registration. If your language doesn't have a build step in
the CI yet, you also need to touch the buildspec — see below.

### CI build for non-Rust languages

[`ci-config/buildspec.yml`](ci-config/buildspec.yml) handles two languages
out of the box:

- **Rust**: every crate under `contenders/rust/*` is compiled by
  `cargo lambda build --locked --release --arm64`. The compiled `bootstrap`
  binary then replaces the source directory before packaging.
- **Python**: every directory under `contenders/python/*` is scanned for a
  `requirements.txt`; if present, deps are installed in place with
  `pip install -r requirements.txt -t .`.

For any other language, add a build step in the `build` phase of the
buildspec. The contract is simple: when the `post_build` phase runs
`aws cloudformation package`, the directory at `contenders/<lang>/<dev_id>/`
must contain exactly what the Lambda runtime expects to find — a `bootstrap`
binary for compiled languages, a fat JAR for Java, transpiled JS for Node, etc.
A commented-out Go example is provided as a starting point under the
`# GO BUILD` marker.

Important: the CI **replaces the contender source directory with the build
output** before zipping. Don't rely on extra files (sources, configs) being
present in the Lambda's runtime filesystem unless your build step explicitly
keeps them.

### Submitting a PR

If your implementation flies, no matter its performances, please submit a PR!

1. Fork the repo, branch as `contender/<lang>-<dev_id>`.
2. Make the three edits above (plus a buildspec change if needed).
3. Open the PR. Useful things to mention in the description:
   - your `<lang>-<dev_id>`;
   - your approach (compression level, streaming strategy, concurrency model);
   - any non-default resource setting (memory, timeout, architecture).
4. Once merged, the CI redeploys and your contender shows up in the next
   benchmark run.

## Project layout

```
root-template.yml             # Root CF stack — nests benching, then contenders
ci-template.yml               # CI/CD: CodePipeline, CodeBuild, artifact bucket, IAM
templates/
  benching.yml                # Bucket, fill-bucket + control Lambdas, Step Function
  benching.asl.json           # Step Function definition (JSONata)
  contenders.yml              # ← contributors register their Lambda here
benching/
  fill-bucket/                # CFN custom resource: fills the bucket on stack create
  control-lambda/             # Archive validator, invoked by the Step Function
contenders/
  rust/<dev_id>/              # one Cargo crate per Rust contender (crate name == <dev_id>)
  python/<dev_id>/            # optional Python contenders
  <lang>/<dev_id>/            # add new languages by creating a new sub-directory
ci-config/
  buildspec.yml               # Builds every internal + contender Lambda; packages CF templates
nix/                          # Rust toolchain + dev shell (optional)
```

The internal Lambdas and all Rust contenders live in a single cargo workspace
(see the root [`Cargo.toml`](Cargo.toml)). One `cargo check` validates
everything.

## License

Distributed under the GPL-3.0-only License. See [`LICENSE`](LICENSE) for the
full text.

## Contact

Jérémie RODON ([@JeremieRodon](https://github.com/JeremieRodon)) — [RustyServerless](https://github.com/RustyServerless)

Project link: <https://github.com/RustyServerless/demo-s3-archiving>
