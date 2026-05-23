//! Archive generator Lambda for creating a ZIP of files in S3

mod slabs_ring;
mod zipper;

use std::{io::Write, sync::Arc};

use aws_sdk_s3::{
    Error as S3Error,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};

use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use serde::Deserialize;
use slabs_ring::{Reader, SlabRing, Writer};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
};
use tracing::{debug, error, info, instrument};

// ---------- Tunables ----------

/// Maximum concurrent file downloads
const MAX_CONCURRENT_DOWNLOADS: usize = 20;
/// Maximum memory that pending downloads can consume (100MB)
const MAX_DOWNLOADS_MEMORY: usize = 100 * 1024 * 1024;

/// Maximum concurrent part uploads
const MAX_CONCURRENT_UPLOADS: usize = 7;
/// Size of each multipart upload chunk (10MB)
const CHUNK_SIZE_BYTES: usize = 10 * 1024 * 1024;
/// Slab Ring Buffer size in chuncks
const BUFFER_CHUNKS_COUNT: usize = 3;

/// Info switch every 50
const TRACING_INFO_FREQUENCY: usize = 50;

// ---------- Helpers ----------

/// Error conversion helper function
fn e2s<E: ::core::error::Error>(value: E) -> String {
    value.to_string()
}

/// This is to wrap S3 operations and force the Result::Err variant to be an S3Error instead of an SdkError.
/// This is because SdkError, when stringified, are utter useless shit.
async fn s3_exec<T, E>(fut: impl Future<Output = Result<T, E>>) -> Result<T, S3Error>
where
    S3Error: From<E>,
{
    Ok(fut.await?)
}

fn get_env(var_name: &str) -> String {
    match std::env::var(var_name) {
        Ok(value) => {
            debug!(var_name, value);
            value
        }
        Err(_) => panic!("Mandatory environment variable `{var_name}` is not set"),
    }
}
fn bucket_name() -> String {
    get_env("BUCKET_NAME")
}
fn files_prefix() -> String {
    get_env("FILES_PREFIX")
}

macro_rules! intermitent_tracing {
    ($index:ident, $($tt:tt)+) => {
        if $index as usize % TRACING_INFO_FREQUENCY == 0 {
            tracing::event!(tracing::Level::INFO, $($tt)+);
        } else {
            tracing::event!(tracing::Level::DEBUG, $($tt)+);
        }
    };
}

// ---------- Event ----------

#[derive(Debug, Deserialize)]
struct InputEvent {
    archive_key: String,
}

// ---------- Main logic ----------

#[instrument(err, skip(memory_semaphore))]
async fn download_file(
    task_index: usize,
    bucket: String,
    key_prefix: &str,
    filename: &str,
    memory_semaphore: Option<Arc<Semaphore>>,
) -> Result<(Vec<u8>, Option<OwnedSemaphorePermit>), String> {
    debug!("Downloading file");

    let mut response = s3_exec(
        s3().get_object()
            .bucket(bucket)
            .key(format!("{key_prefix}/{filename}"))
            .send(),
    )
    .await
    .map_err(e2s)?;

    let expected_size = response
        .content_length()
        .map(|s| s as usize)
        .ok_or("No content_length in GetObject response")?;

    let permit = if let Some(memory_semaphore) = memory_semaphore {
        debug!(
            expected_size,
            "Download memory space: {}/{MAX_DOWNLOADS_MEMORY}",
            memory_semaphore.available_permits()
        );
        Some(
            memory_semaphore
                .acquire_many_owned(expected_size as u32)
                .await
                .unwrap(),
        )
    } else {
        None
    };

    // I DO NOT use the `response.body.collect()` helper here because it allocates
    // internally a lot of intermediate buffers that amplify the memory footprint
    // of the download process x3 (a 50MB photo download results in ~150MB of memory consumption).
    // Instead, I'm manually collecting chunks and adding them in a pre-allocated data vec,
    // resulting in a minimal overhead per-download task of the S3 chunk_size (16KB from my tests).
    let mut data = Vec::with_capacity(expected_size);
    while let Some(chunk_result) = response.body.next().await {
        let chunk = chunk_result.map_err(e2s)?;
        data.extend_from_slice(&chunk);
    }

    intermitent_tracing!(task_index, "data.len()" = data.len(), "Downloaded file");

    Ok((data, permit))
}

/// Spawns download jobs
#[instrument(skip_all)]
fn spawn_download_job(
    filenames: Vec<String>,
    zip_queue_tx: mpsc::UnboundedSender<(String, Vec<u8>, Option<OwnedSemaphorePermit>)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS));
        let memory_semaphore = Arc::new(Semaphore::new(MAX_DOWNLOADS_MEMORY));

        let bucket_name = bucket_name();
        let files_prefix: Arc<str> = Arc::from(files_prefix());

        for (i, filename) in filenames.into_iter().enumerate() {
            debug!(
                "Download jobs slot: {}/{MAX_CONCURRENT_DOWNLOADS}",
                semaphore.available_permits()
            );
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let memory_semaphore = memory_semaphore.clone();
            let tx = zip_queue_tx.clone();

            let bucket_name = bucket_name.clone();
            let files_prefix = files_prefix.clone();

            tokio::spawn(async move {
                debug!("Download job for {filename} started");
                let _permit = permit;
                let (data, memory_permits) = download_file(
                    i,
                    bucket_name,
                    &files_prefix,
                    &filename,
                    Some(memory_semaphore),
                )
                .await?;

                debug!("Download job for {filename} completed");
                tx.send((filename, data, memory_permits)).map_err(e2s)
            });
        }
    })
}

/// Spawns the ZIP creation job
///
/// # Returns
///
/// Returns a task handle for the ZIP creation job
#[instrument(skip_all)]
fn spawn_zip_job(
    mut zip_queue_rx: mpsc::UnboundedReceiver<(String, Vec<u8>, Option<OwnedSemaphorePermit>)>,
    ring_buffer_writer: Writer,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut zipper = zipper::Zipper::new(ring_buffer_writer);
            let mut processed_files = 0;

            info!("ZIP creation job started");

            while let Some(photo_result) = zip_queue_rx.blocking_recv() {
                debug!("zip_queue.len(): {}", zip_queue_rx.len());
                processed_files += 1;
                let (filename, data, _permit) = photo_result;

                intermitent_tracing!(
                    processed_files,
                    "Adding {} to ZIP archive ({} bytes)",
                    filename,
                    data.len()
                );
                if let Err(error) = zipper.add_file(filename, data) {
                    error!(error, "Failed to create ZIP");
                    return;
                }
                debug!("Processed {} files", processed_files);
            }

            info!(processed_files, "Finalizing ZIP archive");

            // Finalize ZIP
            match zipper.finish() {
                Ok(mut w) => {
                    if let Err(e) = w.flush() {
                        error!("Failed to flush ZIP buffer: {}", e);
                        return;
                    }
                }
                Err(e) => {
                    error!("Failed to finalize ZIP: {}", e);
                    return;
                }
            }

            info!("ZIP creation job completed");
        })
        .await
        .unwrap()
    })
}

/// Spawns S3 upload jobs
///
/// # Arguments
///
/// * `ring_buffer` - Shared ring buffer with ZIP chunks
/// * `multipart_upload` - The multipart upload details
/// * `progress_counter` - Shared progress counter
///
/// # Returns
///
/// Returns the upload task handle
#[instrument(skip_all)]
fn spawn_upload_jobs(
    mut reader: Reader,
    multipart_upload: MultipartUpload,
) -> tokio::task::JoinHandle<Result<Vec<CompletedPart>, String>> {
    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));

        let mut part_number = 1i32;
        let mut job_handles = JoinSet::new();

        info!("Upload job started");

        while let Some(lease) = reader.recv().await {
            debug!("Upload job: Received slab #{part_number}");
            debug!(
                "Upload jobs slot: {}/{MAX_CONCURRENT_UPLOADS}",
                semaphore.available_permits()
            );
            let semaphore = semaphore.clone();
            let multipart_upload = multipart_upload.clone();

            job_handles.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                match upload_part(multipart_upload, part_number, lease.into_vec()).await {
                    Ok(completed_part) => {
                        intermitent_tracing!(
                            part_number,
                            part_number,
                            "Successfully uploaded part"
                        );
                        Ok(completed_part)
                    }
                    Err(e) => {
                        error!("Failed to upload part {}: {}", part_number, e);
                        return Err(e);
                    }
                }
            });

            part_number += 1;
        }

        let job_results = job_handles.join_all().await;
        let all_completed_parts = job_results.into_iter().collect::<Result<Vec<_>, _>>()?;

        info!(
            "Upload job completed with {} parts",
            all_completed_parts.len()
        );
        Ok(all_completed_parts)
    })
}

/// Represents a multipart upload operation
#[derive(Debug, Clone)]
struct MultipartUpload {
    bucket: Arc<str>,
    key: Arc<str>,
    upload_id: Arc<str>,
}

/// Starts a multipart upload
///
/// # Returns
///
/// Returns a MultipartUpload struct with upload details
///
/// # Errors
///
/// Returns an error string if the multipart upload cannot be started
#[instrument(fields(bucket))]
async fn start_multipart_upload(key: String) -> Result<MultipartUpload, String> {
    let bucket = bucket_name();
    let arc_bucket = Arc::from(bucket.as_str());
    let arc_key = Arc::from(key.as_str());

    tracing::Span::current().record("bucket", &bucket);
    info!("Starting multipart upload");

    let response = s3_exec(
        s3().create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .content_type("application/zip")
            .send(),
    )
    .await
    .map_err(e2s)?;

    let upload_id = response
        .upload_id
        .ok_or("No upload ID in multipart upload response")?;

    info!(upload_id, "Multipart upload started");

    Ok(MultipartUpload {
        bucket: arc_bucket,
        key: arc_key,
        upload_id: Arc::from(upload_id),
    })
}

/// Uploads a single part of a multipart upload
///
/// # Arguments
///
/// * `multipart_upload` - The multipart upload details
/// * `part_number` - The part number (1-based)
/// * `data` - The data to upload
///
/// # Returns
///
/// Returns the completed part
///
/// # Errors
///
/// Returns an error string if the upload fails
#[instrument(skip(data))]
async fn upload_part(
    multipart_upload: MultipartUpload,
    part_number: i32,
    data: Vec<u8>,
) -> Result<CompletedPart, String> {
    debug!("data.len()" = data.len(), "Uploading part");

    let response = s3_exec(
        s3().upload_part()
            .bucket(multipart_upload.bucket.as_ref())
            .key(multipart_upload.key.as_ref())
            .upload_id(multipart_upload.upload_id.as_ref())
            .part_number(part_number)
            .body(ByteStream::from(data))
            .send(),
    )
    .await
    .map_err(e2s)?;

    let etag = response.e_tag.ok_or("No ETag in upload part response")?;

    debug!(part_number, etag, "Part uploaded");

    Ok(CompletedPart::builder()
        .part_number(part_number)
        .e_tag(etag)
        .build())
}

/// Completes a multipart upload
///
/// # Arguments
///
/// * `multipart_upload` - The multipart upload details
/// * `completed_parts` - Vector of completed parts
///
/// # Errors
///
/// Returns an error string if the completion fails
#[instrument(skip(completed_parts))]
async fn complete_multipart_upload(
    multipart_upload: MultipartUpload,
    mut completed_parts: Vec<CompletedPart>,
) -> Result<(), String> {
    // Sort parts by part number
    completed_parts.sort_by_key(|part| part.part_number());

    info!(
        "completed_parts.len()" = completed_parts.len(),
        "Completing multipart upload"
    );

    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    s3_exec(
        s3().complete_multipart_upload()
            .bucket(multipart_upload.bucket.as_ref())
            .key(multipart_upload.key.as_ref())
            .upload_id(multipart_upload.upload_id.as_ref())
            .multipart_upload(completed_upload)
            .send(),
    )
    .await
    .map_err(e2s)?;

    info!("Multipart upload completed successfully");
    Ok(())
}
#[instrument(skip(filenames))]
async fn create_multipart_archive(
    filenames: Vec<String>,
    archive_name: String,
) -> Result<(), String> {
    info!(
        "filenames.len()" = filenames.len(),
        "Creating multipart archive"
    );

    // 1. Start multipart upload
    let multipart_upload = start_multipart_upload(archive_name).await?;

    // 2. Set up communication channels
    let (zip_queue_tx, zip_queue_rx) = mpsc::unbounded_channel();
    let (writer, reader) = SlabRing::new(CHUNK_SIZE_BYTES, BUFFER_CHUNKS_COUNT);

    // 3. Spawn all jobs
    let download_handle = spawn_download_job(filenames, zip_queue_tx);
    let zip_handle = spawn_zip_job(zip_queue_rx, writer);
    let upload_handle = spawn_upload_jobs(reader, multipart_upload.clone());

    // 4. Wait for all download jobs to complete
    info!("Waiting for download jobs to complete");
    if let Err(e) = download_handle.await {
        error!("Download job failed: {}", e);
    }

    // 5. Wait for ZIP creation to complete
    info!("Waiting for ZIP creation to complete");
    if let Err(error) = zip_handle.await {
        error!(?error, "ZIP creation job failed");
    }

    // 6. Wait for ZIP parts upload to complete
    info!("Waiting for ZIP parts upload to complete");

    let completed_parts = match upload_handle.await {
        Ok(Ok(parts)) => parts,
        Ok(Err(e)) => {
            return Err(format!("Upload job failed: {}", e));
        }
        Err(e) => {
            return Err(format!("Upload job panicked: {}", e));
        }
    };

    // 7. Complete multipart upload
    complete_multipart_upload(multipart_upload, completed_parts).await?;

    info!("Multipart pack creation completed successfully");
    Ok(())
}

/// Lists all object filenames under the configured files prefix in the bucket.
///
/// Uses `ListObjectsV2` with the SDK paginator to traverse all pages and
/// returns only the filename portion of each key (the part after the prefix).
///
/// # Errors
///
/// Returns an error string if any page request fails.
#[instrument]
async fn list_files(bucket: String, key_prefix: String) -> Result<Vec<String>, String> {
    info!("Listing files");

    // Ensure the prefix passed to S3 ends with `/` so we list only objects
    // under the directory and can cleanly strip it to get the filename.
    let s3_prefix = if key_prefix.ends_with('/') {
        key_prefix
    } else {
        format!("{key_prefix}/")
    };

    let mut paginator = s3()
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&s3_prefix)
        .into_paginator()
        .send();

    let mut filenames = Vec::new();

    while let Some(page) = paginator.next().await {
        for object in page
            .map_err(|e| e2s(S3Error::from(e)))?
            .contents
            .unwrap_or_default()
        {
            let Some(key) = object.key else {
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
                filenames.push(filename.to_owned());
            }
        }
    }

    debug!("Listed {} files", filenames.len());
    Ok(filenames)
}

#[instrument(skip_all, fields(archive_key))]
async fn handler(event: LambdaEvent<InputEvent>) -> Result<(), LambdaError> {
    let archive_key = event.payload.archive_key;
    tracing::Span::current().record("archive_key", &archive_key);

    info!("Start processing");

    // 1. List the files from S3
    let filenames = list_files(bucket_name(), files_prefix()).await?;
    info!("Creating archive with {} files", filenames.len());

    // 2. Launch archive creation
    create_multipart_archive(filenames, archive_key).await?;

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
