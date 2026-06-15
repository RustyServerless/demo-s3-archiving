//! Benchmarked contender Lambda that archives S3 files into a flat, STORED (uncompressed) ZIP.
//!
//! Receives a [`JobInfo`] event, lists every object under `{bucket}/{files_prefix}/`, then
//! plans a byte-exact ZIP layout upfront so each multipart part can be produced independently
//! and in parallel. Large files are copied S3-side via `UploadPartCopy` (no download);
//! smaller files are streamed into `UploadPart` buffers. The Central Directory is written last,
//! after all entry tasks have reported their CRC32s through a channel.

// The macro must be declared (textually) before the modules that use it so it is
// in scope for `part_job`. `#[macro_use]` keeps it visible to the child modules.
#[macro_use]
mod tracing_macros {
    /// Emits an `INFO` event every [`TRACING_INFO_FREQUENCY`](crate::TRACING_INFO_FREQUENCY)
    /// calls and `DEBUG` otherwise.
    ///
    /// Used on hot paths (e.g. the per-part execution loop) to keep periodic visibility at
    /// `INFO` without drowning the logs with thousands of lines per run.
    macro_rules! intermitent_tracing {
        ($index:expr, $($tt:tt)+) => {
            if $index as usize % $crate::TRACING_INFO_FREQUENCY == 0 {
                tracing::event!(tracing::Level::INFO, $($tt)+);
            } else {
                tracing::event!(tracing::Level::DEBUG, $($tt)+);
            }
        };
    }
}

mod part_job;
mod shared_buffer;
mod zip_layout;

use std::sync::Arc;

use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use serde::Deserialize;

use tracing::{debug, info, instrument};

use crate::{part_job::PartJobExecutor, zip_layout::ZipLayout};

// ---------- Tunables ----------

/// Switch `intermitent_tracing!` to `INFO` once every this many calls (`DEBUG` in between).
pub(crate) const TRACING_INFO_FREQUENCY: usize = 50;

/// Crate-wide error type. The blanket `From<SdkError<E, R>>` impl below funnels all
/// typed AWS SDK operation errors into the [`Error::S3`] variant via `aws_sdk_s3::Error`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("s3 error: {0}")]
    S3(#[from] Box<aws_sdk_s3::Error>),
    #[error("s3 bytestream error: {0}")]
    S3ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Task panicked: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Could not parse CRC32: {0}")]
    Crc32Parse(#[from] base64::DecodeSliceError),
    #[error("{0}")]
    Custom(String),
}
/// Converts any typed SDK error into [`Error::S3`] by erasing the operation-specific type.
impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for Error
where
    aws_sdk_s3::error::SdkError<E, R>: Into<aws_sdk_s3::Error>,
{
    fn from(err: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        Error::S3(Box::new(err.into()))
    }
}
impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Custom(err.to_owned())
    }
}
impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Custom(err)
    }
}

/// One source S3 object to be archived: its bare filename (ZIP entry name), full S3 bucket/key, and byte size.
#[derive(Debug)]
struct FileInfo {
    name: String,
    bucket_name: Arc<str>,
    key: String,
    size: usize,
}

// ---------- Event ----------

/// Lambda event payload describing one archiving job.
///
/// All three fields are `Arc<str>` so they can be cheaply cloned into every spawned task.
/// - `bucket_name` — source and destination bucket.
/// - `files_prefix` — S3 key prefix (without trailing slash) for the source objects.
/// - `archive_key` — destination S3 key for the produced ZIP (e.g. `archives/rust-jrodon.zip`).
#[derive(Debug, Deserialize, Clone)]
struct JobInfo {
    bucket_name: Arc<str>,
    files_prefix: Arc<str>,
    archive_key: Arc<str>,
}

/// Lists all objects under `{key_prefix}/` using the SDK paginator and returns one [`FileInfo`] per object.
///
/// Strips the prefix from each key to obtain the bare filename used as the ZIP entry name.
#[instrument]
async fn list_files(bucket: Arc<str>, key_prefix: &str) -> Result<Vec<FileInfo>, Error> {
    info!("Listing files");

    // Ensure the prefix passed to S3 ends with `/` so we list only objects
    // under the directory and can cleanly strip it to get the filename.
    let s3_prefix = format!("{key_prefix}/");

    let mut paginator = s3()
        .list_objects_v2()
        .bucket(&*bucket)
        .prefix(&s3_prefix)
        .into_paginator()
        .send();

    let mut file_infos = Vec::new();

    while let Some(page) = paginator.next().await {
        for object in page?.contents.unwrap_or_default() {
            let Some(key) = object.key else {
                continue;
            };
            let Some(size) = object.size.map(|s| s as usize) else {
                continue;
            };
            // Skip the prefix "directory marker" itself, if any.
            if key == s3_prefix {
                continue;
            }
            if let Some(filename) = key.strip_prefix(&s3_prefix) {
                if filename.is_empty() {
                    continue;
                }
                file_infos.push(FileInfo {
                    name: filename.to_owned(),
                    bucket_name: bucket.clone(),
                    key,
                    size,
                });
            }
        }
    }

    debug!("Listed {} files", file_infos.len());
    Ok(file_infos)
}

/// Lambda entry point: list source objects, plan the ZIP layout, then execute the multipart upload.
#[instrument(skip_all, fields(job_info = ?event.payload))]
async fn handler(event: LambdaEvent<JobInfo>) -> Result<(), LambdaError> {
    info!("Start processing");

    let job_info = event.payload;

    let JobInfo {
        bucket_name,
        files_prefix,
        archive_key,
    } = job_info;

    // List the files from S3
    let files_info = list_files(bucket_name.clone(), &files_prefix).await?;
    let total_bytes: usize = files_info.iter().map(|fi| fi.size).sum();
    info!(
        file_count = files_info.len(),
        total_bytes, %archive_key, "Creating archive"
    );

    // Plan the ZIP layout (decides Single vs Duo parts, copy ranges, etc.)
    let layout = ZipLayout::from_files_info(files_info);

    // Turn the layout into concrete S3 multipart jobs and run them.
    let job_executor = PartJobExecutor::new(layout, bucket_name, archive_key);
    job_executor.execute().await?;

    info!("Archive created successfully");
    Ok(())
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
