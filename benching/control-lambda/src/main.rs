//! Step Function-invoked Lambda that validates a contender's archive.
//!
//! Algorithm:
//! 1. Paginate `ListObjectsV2` under `{FILES_PREFIX}/` in `{BUCKET_NAME}` and
//!    build a `HashSet<String>` of expected SHA256 hex names. Assert size == 5000.
//! 2. Stream-read the archive at `{BUCKET_NAME}/{archive_key}` and, for each entry:
//!    - assert the file name has no `/` (flat layout);
//!    - SHA256-stream the content and compare to the file name;
//!    - remove the name from the expected set.
//! 3. Assert the set is empty.
//!
//! Any assertion failure becomes a typed `ControlError` whose `Display` is
//! propagated to Step Functions through Lambda's `errorMessage` field.

mod s3_object_proxy;

use std::collections::HashSet;

use aws_sdk_s3::Error as S3Error;
use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use tracing::{debug, error, info, instrument};

use crate::s3_object_proxy::S3ObjectReader;

// ---------- Event ----------

#[derive(Debug, Deserialize)]
struct ControlEvent {
    archive_key: String,
    expected_object_count: String,
}

// ---------- Errors ----------
//
// Keep all error mapping in one place: the `ControlError` enum and its `From`
// impls below. If you need a new error category, add a variant here and a
// single `From` impl rather than scattering `map_err` calls across the code.

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("expected {1} test objects in bucket, found {0}")]
    BadObjectCount(usize, usize),
    #[error("archive contains nested path '{0}', flat layout required")]
    HierarchyForbidden(String),
    #[error("content hash mismatch for '{file}': computed {actual}")]
    ContentMismatch { file: String, actual: String },
    #[error("unknown or duplicate object in archive: '{0}'")]
    UnknownOrDuplicate(String),
    #[error("archive missing {count} expected object(s) (sample: {sample:?})")]
    MissingObjects { count: usize, sample: Vec<String> },
    #[error("missing env var {0}")]
    EnvVar(&'static str),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("zip error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("s3 error: {0}")]
    S3(#[from] aws_sdk_s3::Error),
    #[error("s3 bytestream error: {0}")]
    S3ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error("tokio chanel closed, cannot send more bytes")]
    ChannelClosed,
}

// // Centralized mapping for any AWS SDK S3 error (List, Get, Head, byte-stream
// // chunk errors, ...). Anything that comes from the S3 client/stream funnels
// // through here so we have a single source of truth for S3 -> ControlError.
// impl<E> From<E> for ControlError
// where
//     E: Into<aws_sdk_s3::Error> + ProvideErrorMetadata,
// {
//     fn from(err: E) -> Self {
//         ControlError::S3(err.into())
//     }
// }

// ---------- Entry point ----------

#[instrument(
    skip_all,
    fields(archive_key, expected_object_count, bucket, files_prefix)
)]
async fn handler(event: LambdaEvent<ControlEvent>) -> Result<Value, LambdaError> {
    let archive_key = event.payload.archive_key;
    let expected_object_count = event.payload.expected_object_count.parse::<usize>()?;
    tracing::Span::current().record("archive_key", archive_key.as_str());
    tracing::Span::current().record("expected_object_count", expected_object_count);

    let bucket = std::env::var("BUCKET_NAME").map_err(|_| ControlError::EnvVar("BUCKET_NAME"))?;
    let files_prefix =
        std::env::var("FILES_PREFIX").map_err(|_| ControlError::EnvVar("FILES_PREFIX"))?;
    tracing::Span::current().record("bucket", bucket.as_str());
    tracing::Span::current().record("files_prefix", files_prefix.as_str());

    info!(%bucket, %files_prefix, %archive_key, expected_object_count, "control-lambda invoked");

    if let Err(error) = validate(&bucket, &files_prefix, &archive_key, expected_object_count).await
    {
        error!(%error, "control-lambda validation failed");
        return Err(error.into());
    }
    Ok(serde_json::json!({ "ok": true }))
}

// ---------- Validation ----------

#[instrument(skip_all, fields(bucket = %bucket, archive_key = %archive_key, expected_object_count))]
async fn validate(
    bucket: &str,
    files_prefix: &str,
    archive_key: &str,
    expected_object_count: usize,
) -> Result<(), ControlError> {
    // Phase 1: build expected set.
    info!(phase = "list_expected", "starting phase 1");
    let expected = list_expected(bucket, files_prefix, expected_object_count).await?;
    info!(count = expected.len(), "expected set built");
    if expected.len() != expected_object_count {
        error!(
            found = expected.len(),
            expected = expected_object_count,
            "bad object count in bucket"
        );
        return Err(ControlError::BadObjectCount(
            expected.len(),
            expected_object_count,
        ));
    }

    // Phase 2: open the archive stream and validate every entry.
    info!(phase = "validate_archive", "starting phase 2");
    validate_archive(bucket, archive_key, expected).await?;

    info!("archive validated successfully");
    Ok(())
}

#[instrument(skip_all, fields(bucket = %bucket, files_prefix = %files_prefix, expected_object_count))]
async fn list_expected(
    bucket: &str,
    files_prefix: &str,
    expected_object_count: usize,
) -> Result<HashSet<String>, ControlError> {
    let prefix = if files_prefix.ends_with('/') {
        files_prefix.to_string()
    } else {
        format!("{files_prefix}/")
    };
    debug!(%prefix, "computed listing prefix");

    let mut set = HashSet::with_capacity(expected_object_count);
    let mut continuation: Option<String> = None;
    let mut page_index: usize = 0;
    loop {
        let mut req = s3().list_objects_v2().bucket(bucket).prefix(&prefix);
        if let Some(c) = continuation.as_deref() {
            req = req.continuation_token(c);
        }
        let resp = s3_exec(req.send()).await?;

        let page_len = resp.contents().len();
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                // Strip the prefix; remaining part is the SHA256 hex name (no extension).
                if let Some(name) = key.strip_prefix(&prefix) {
                    if !name.is_empty() {
                        set.insert(name.to_string());
                    }
                }
            }
        }
        debug!(
            page_index,
            page_len,
            running_total = set.len(),
            "processed list_objects_v2 page"
        );
        page_index += 1;
        if resp.is_truncated().unwrap_or(false) {
            continuation = resp.next_continuation_token().map(str::to_string);
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    info!(
        pages = page_index,
        total = set.len(),
        "list_expected complete"
    );
    Ok(set)
}

#[instrument(skip_all, fields(bucket = %bucket, archive_key = %archive_key, expected_count = expected.len()))]
async fn validate_archive(
    bucket: &str,
    archive_key: &str,
    mut expected: HashSet<String>,
) -> Result<(), ControlError> {
    info!("Starting Zip validation");

    let s3_obj_reader =
        S3ObjectReader::create(s3(), bucket.to_owned(), archive_key.to_owned()).await?;

    let mut zip_archive = zip::ZipArchive::new(s3_obj_reader)?;
    let entry_count = zip_archive.len();
    info!(entry_count, "ZIP central directory parsed");

    let filenames = zip_archive
        .file_names()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    for file in filenames.iter() {
        debug!(%file, "Controlling entry name");

        if file.contains('/') {
            error!(%file, "archive contains nested path");
            return Err(ControlError::HierarchyForbidden(file.to_owned()));
        }

        if !expected.remove(file) {
            error!(%file, "unknown or duplicate object in archive");
            return Err(ControlError::UnknownOrDuplicate(file.to_owned()));
        }
    }

    if !expected.is_empty() {
        let sample: Vec<String> = expected.iter().take(5).cloned().collect();
        error!(
            missing = expected.len(),
            ?sample,
            "archive missing expected objects"
        );
        return Err(ControlError::MissingObjects {
            count: expected.len(),
            sample,
        });
    }
    let processed = filenames.len();
    info!(processed, "Finished Zip index entries validation");

    for file in filenames {
        let mut entry = zip_archive.by_name(&file)?;

        let size = entry.size();
        debug!(%file, size, "Controlling entry content");

        let mut hasher = Sha256::new();
        let copied = std::io::copy(&mut entry, &mut hasher)?;
        let actual = to_hex(&hasher.finalize());
        debug!(%file, hashed_bytes = copied, "entry hashed");

        if actual != file {
            error!(%file, %actual, "content hash mismatch");
            return Err(ControlError::ContentMismatch { file, actual });
        }
    }
    info!(processed, "Finished Zip validation");

    Ok(())
}

// ---------- Helpers ----------

fn to_hex(bytes: &[u8]) -> String {
    use core::fmt::Write;
    let mut s = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

async fn s3_exec<T, E>(fut: impl Future<Output = Result<T, E>>) -> Result<T, S3Error>
where
    S3Error: From<E>,
{
    Ok(fut.await?)
}

// This macro from `awssdk-instrumentation` generates the entire main() function:
// - Initializes the OTel tracer provider with X-Ray exporter
// - Sets up tracing-subscriber with JSON console output and OTel bridge
// - Loads AWS SDK config from environment
// - Creates an instrumented S3 client accessible via `s3()`
// - Wraps the Lambda runtime with the OTel Tower layer
// - Starts the Lambda runtime
//
// See: https://docs.rs/awssdk-instrumentation/latest/awssdk_instrumentation/macro.make_lambda_runtime.html
awssdk_instrumentation::make_lambda_runtime!(
    handler,
    trigger = awssdk_instrumentation::lambda::layer::OTelFaasTrigger::Other,
    s3() -> aws_sdk_s3::Client
);
