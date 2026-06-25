use std::{collections::BTreeMap, io::Write, str::FromStr, sync::Arc};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{ChecksumMode, CompletedMultipartUpload, CompletedPart},
};
use base64::Engine;
use tokio::{
    sync::{
        Semaphore,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinSet,
};
use tracing::{debug, info, instrument};

use crate::{Error, FileInfo, s3, shared_buffer::SharedBuf, zip_layout::ZipLayout};

// ---------- Tunables ----------

/// Total memory that all concurrently running part-job tasks may allocate for their upload buffers.
///
/// Each `UploadPart` task acquires permits equal to its buffer size before spawning, so this
/// semaphore acts as a backpressure valve. `UploadPartCopy` tasks claim a fixed 512 KiB token
/// (they hold no buffer) just to prevent all copy jobs from launching simultaneously.
/// Central Directory headers are excluded from this budget — they are negligible (~48 KiB per
/// 1 000 entries).
const MAX_PART_JOB_TASKS_MEMORY: usize = 50 * 1024 * 1024; // 50MB

// ---------- MultiPart execution ----------

/// Drives the S3 multipart upload: creates the upload, runs all part jobs, then completes it.
pub struct PartJobExecutor {
    bucket_name: Arc<str>,
    archive_key: Arc<str>,
    jobs: Vec<PartJob>,
}

impl PartJobExecutor {
    /// Converts a [`ZipLayout`] into an executor ready to run against the given bucket and key.
    pub fn new(layout: ZipLayout, bucket_name: Arc<str>, archive_key: Arc<str>) -> Self {
        Self {
            bucket_name,
            archive_key,
            jobs: layout.into_part_jobs(),
        }
    }

    /// Runs the full multipart upload: create → spawn parts (memory-gated) → complete.
    ///
    /// Parts are spawned sequentialy upon acquisition of memory permits from a semaphore,
    /// preventing unbounded buffer allocation for many parts.
    #[instrument(skip_all, fields(archive_key = %self.archive_key))]
    pub async fn execute(self) -> Result<(), Error> {
        let part_count = self.jobs.len();
        info!(part_count, "Starting multipart upload");

        let response = s3()
            .create_multipart_upload()
            .bucket(&*self.bucket_name)
            .key(&*self.archive_key)
            .content_type("application/zip")
            .send()
            .await?;

        let upload_id = response
            .upload_id
            .ok_or("No upload ID in multipart upload response")?;
        info!(
            upload_id,
            part_count, "Multipart upload created, dispatching part jobs"
        );

        let memory_semaphore = Arc::new(Semaphore::new(MAX_PART_JOB_TASKS_MEMORY));

        let mut join_set = JoinSet::new();
        for job in self.jobs {
            let budget = job.memory_budget_needed();
            debug!(
                part_number = job.part_number,
                budget,
                available_memory = memory_semaphore.available_permits(),
                "Acquiring memory budget before spawning part job"
            );
            // Block here so we apply backpressure before spawning.
            // The permit is moved into the task and dropped when the task finishes,
            // freeing capacity for the next job.
            let permit = memory_semaphore
                .clone()
                .acquire_many_owned(budget)
                .await
                .map_err(|_| "Semaphore closed")?;
            let bucket_name = (*self.bucket_name).to_owned();
            let archive_key = (*self.archive_key).to_owned();
            let upload_id = upload_id.clone();
            intermitent_tracing!(job.part_number, ?job, "Spawning Part job");
            join_set.spawn(async move {
                let _permit = permit;
                job.execute(bucket_name, archive_key, upload_id).await
            });
        }

        info!(part_count, "All part jobs dispatched, awaiting completion");
        let mut completed_parts = join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<CompletedPart>, _>>()?;
        info!(
            completed_parts = completed_parts.len(),
            "All parts uploaded, completing multipart upload"
        );

        // Sort parts by part number
        // TODO: Verify if it is actually mandated by the S3 API (it costs virtually nothing but still...)
        completed_parts.sort_by_key(|part| part.part_number());

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        s3().complete_multipart_upload()
            .bucket(&*self.bucket_name)
            .key(&*self.archive_key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        info!("Multipart upload completed");
        Ok(())
    }
}

/// Incrementally builds the ordered list of [`PartJob`]s while tracking the running archive offset.
///
/// Call [`add_files`](Self::add_files) / [`copy_file`](Self::copy_file) /
/// [`partial_copy_file`](Self::partial_copy_file) in layout order, then [`finalize`](Self::finalize)
/// to append the Central Directory element and retrieve the completed job list.
#[derive(Debug)]
pub struct PartJobsBuilder {
    /// Running byte offset into the final ZIP archive; advanced as each entry is registered.
    current_archive_offset: usize,
    /// Predicted byte size of each Central Directory File Header, accumulated for the EOCD.
    cdfh_sizes: Vec<usize>,
    /// The builded parts list.
    parts: Vec<PartJob>,
    /// Sender half of the channel through which entry tasks deliver their completed CDFHs.
    cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
    /// Receiver half consumed by the Central Directory element during execution.
    cdfh_receiver: UnboundedReceiver<CentralDirectoryFileHeader>,
}

impl PartJobsBuilder {
    /// Creates a new builder with a fresh CDFH channel and zero archive offset.
    pub fn new() -> Self {
        let (cdfh_sender, cdfh_receiver) = unbounded_channel();
        Self {
            current_archive_offset: 0,
            cdfh_sizes: vec![],
            parts: vec![],
            cdfh_sender,
            cdfh_receiver,
        }
    }

    /// Appends a sequence of regular files (full `GetObject` download) to the current upload part.
    ///
    /// For each file, a LOC header is created at the current archive offset, the offset is
    /// advanced by `loc_size + file_size`, and a CDFH size prediction is recorded. If the last
    /// existing part is already an `Upload` part, the new elements are appended to it; otherwise
    /// a new `Upload` part is created.
    #[instrument(skip_all, fields(current_archive_offset=%self.current_archive_offset, part_count=%self.parts.len(), entry_count=%self.cdfh_sizes.len()))]
    pub fn add_files(&mut self, file_infos: impl Iterator<Item = FileInfo>) {
        let elem_iterator = file_infos.map(|file_info| {
            // Build the LOC at the current offset, then advance the offset past LOC + payload.
            let name_len = file_info.name.len();
            let loc =
                LocalFileHeader::new(self.current_archive_offset, file_info.name, file_info.size);
            self.cdfh_sizes
                .push(CentralDirectoryFileHeader::predict_size(
                    file_info.size,
                    name_len,
                    loc.offset,
                ));
            self.current_archive_offset += loc.size() + file_info.size;

            UploadPartElement::EntryFromS3 {
                loc,
                operation: S3ObjectOperation::Get,
                bucket_name: file_info.bucket_name,
                key: file_info.key,
                cdfh_sender: self.cdfh_sender.clone(),
            }
        });

        // Reuse the last Upload part if possible to avoid creating unnecessary part boundaries.
        match self.parts.last_mut() {
            Some(PartJob {
                part_size,
                part_job_type: PartJobType::Upload(part_elements),
                ..
            }) => {
                part_elements.extend(elem_iterator);
                *part_size = part_elements.iter().map(|upe| upe.elem_size()).sum();
            }
            _ => {
                let part_number = self.parts.len() as i32 + 1;
                let upload_part_elems = elem_iterator.collect::<Vec<_>>();
                let part_size = upload_part_elems.iter().map(|upe| upe.elem_size()).sum();
                debug!(part_number, part_size, "Creating a new Upload PartJob");
                self.parts.push(PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(upload_part_elems),
                });
            }
        };
    }

    /// Appends a full server-side copy entry (LOC via `HeadObject`, body via `UploadPartCopy`).
    pub fn copy_file(&mut self, file_info: FileInfo) {
        self._copy_file(file_info, None);
    }

    /// Appends a partial server-side copy entry: first `start_byte` bytes are downloaded into
    /// the current upload part; the remainder is transferred via `UploadPartCopy`.
    pub fn partial_copy_file(&mut self, file_info: FileInfo, start_byte: usize) {
        self._copy_file(file_info, Some(start_byte));
    }

    /// Shared implementation for [`copy_file`](Self::copy_file) and
    /// [`partial_copy_file`](Self::partial_copy_file).
    ///
    /// Appends the LOC element (and optionally a ranged GET element) to the current upload part,
    /// then creates a new `Copy` part for the server-side-copied body.
    #[instrument(skip_all, fields(file_info, start_byte))]
    fn _copy_file(&mut self, file_info: FileInfo, start_byte: Option<usize>) {
        // A Copy part must always follow an Upload part (the LOC lives in the Upload part).
        let last_part = self.parts.last_mut().expect("CopyPart cannot be first");
        let PartJobType::Upload(part_elements) = &mut last_part.part_job_type else {
            panic!("CopyPart cannot follow another CopyPart");
        };

        // Register the LOC at the current offset and advance past it.
        let name_len = file_info.name.len();
        let loc = LocalFileHeader::new(self.current_archive_offset, file_info.name, file_info.size);
        self.cdfh_sizes
            .push(CentralDirectoryFileHeader::predict_size(
                file_info.size,
                name_len,
                loc.offset,
            ));
        self.current_archive_offset += loc.size();

        // For a FullCopy, only the LOC is in the upload part (HeadObject fetches the CRC32).
        // For a PartialCopy, the first `start_byte` bytes are also downloaded (ranged GetObject),
        // so the archive offset must advance past those bytes too before the Copy part begins.
        let operation = match start_byte {
            Some(start_byte) => {
                self.current_archive_offset += start_byte;
                S3ObjectOperation::PartialGet { size: start_byte }
            }
            None => S3ObjectOperation::Head,
        };

        let copy_source = format!("{}/{}", file_info.bucket_name, file_info.key);

        let last_upload_part_element = UploadPartElement::EntryFromS3 {
            loc,
            operation,
            bucket_name: file_info.bucket_name,
            key: file_info.key,
            cdfh_sender: self.cdfh_sender.clone(),
        };

        last_part.part_size += last_upload_part_element.elem_size();
        debug!(
            last_part.part_number,
            last_part.part_size, "Updated Upload part size"
        );
        part_elements.push(last_upload_part_element);
        debug!(
            last_part.part_number,
            part_element_count = part_elements.len(),
            "Upload Part finalized"
        );

        // Create the Copy part that will carry the server-side-copied body bytes.
        let part_number = self.parts.len() as i32 + 1;
        let part_offset = self.current_archive_offset;
        let part_size = match start_byte {
            Some(start_byte) => file_info.size - start_byte,
            None => file_info.size,
        };
        debug!(
            part_number,
            part_offset, part_size, "Creating a new Copy PartJob"
        );
        self.parts.push(PartJob {
            part_number,
            part_size,
            part_job_type: PartJobType::Copy {
                copy_source,
                range: start_byte
                    .map(|start_byte| format!("bytes={start_byte}-{}", file_info.size - 1)),
            },
        });
        self.current_archive_offset += part_size;
    }

    /// Appends the Central Directory element to the last upload part and returns the job list.
    ///
    /// The EOCD64 is computed from the accumulated CDFH sizes and the current archive offset.
    pub fn finalize(mut self) -> Vec<PartJob> {
        // Compute the Central Directory layout from the sizes accumulated during add_files/_copy_file.
        let record_count = self.cdfh_sizes.len();
        let central_directory_size = self.cdfh_sizes.into_iter().sum();
        let eocd64 = EndOfCentralDirectory64 {
            record_count,
            central_directory_size,
            central_directory_offset: self.current_archive_offset,
            // The EOCD/EOCD64 will directly follow the last Central Directory File Header
            eocd64_offset: self.current_archive_offset + central_directory_size,
        };
        let total_size = central_directory_size + eocd64.size();

        let cd = UploadPartElement::CentralDirectory {
            total_size,
            eocd64,
            cdfh_receiver: self.cdfh_receiver,
        };
        debug!(total_size, ?eocd64, "CentralDirectory element created");

        // If the last part is an UploadPart, extend it, else create one
        match self.parts.last_mut() {
            Some(PartJob {
                part_number,
                part_size,
                part_job_type: PartJobType::Upload(part_elements),
                ..
            }) => {
                *part_size += cd.elem_size();
                part_elements.push(cd);
                debug!(
                    part_number,
                    part_size,
                    part_element_count = part_elements.len(),
                    "Last Upload Part finalized"
                );
            }
            _ => {
                let part_number = self.parts.len() as i32 + 1;
                let part_offset = self.current_archive_offset;
                let part_size = cd.elem_size();
                debug!(
                    part_number,
                    part_offset, part_size, "Pushing Last Upload Part with only CentralDirectory"
                );
                self.parts.push(PartJob {
                    part_number,
                    part_size,
                    part_job_type: PartJobType::Upload(vec![cd]),
                });
            }
        };

        // Log info about the jobs to be done
        let (upload_part_count, copy_part_count, download_size, upload_size, copy_size) = self
            .parts
            .iter()
            .fold((0usize, 0usize, 0, 0, 0), |mut counters, part| {
                match &part.part_job_type {
                    PartJobType::Upload(upload_part_elements) => {
                        // upload_part_count
                        counters.0 += 1;
                        // download_size
                        counters.2 += upload_part_elements
                            .iter()
                            .map(|e| match e {
                                UploadPartElement::EntryFromS3 { loc, operation, .. } => {
                                    match operation {
                                        S3ObjectOperation::Get => loc.file_size,
                                        S3ObjectOperation::PartialGet { size } => *size,
                                        S3ObjectOperation::Head => 0,
                                    }
                                }
                                UploadPartElement::CentralDirectory { .. } => 0,
                            })
                            .sum::<usize>();
                        // upload_size
                        counters.3 += part.part_size;
                    }
                    PartJobType::Copy { .. } => {
                        // copy_part_count
                        counters.1 += 1;
                        // copy_size
                        counters.4 += part.part_size;
                    }
                }
                counters
            });

        info!(
            part_count = self.parts.len(),
            upload_part_count,
            copy_part_count,
            download_size,
            upload_size,
            copy_size,
            "Part Jobs done"
        );

        self.parts
    }
}

/// One unit of work in the multipart upload: either an `UploadPart` or an `UploadPartCopy`.
#[derive(Debug)]
pub struct PartJob {
    /// Part number in the MultiPart upload, begins at 1.
    part_number: i32,
    /// Exact size of this part.
    part_size: usize,
    part_job_type: PartJobType,
}

impl PartJob {
    /// Returns the number of semaphore permits this job must hold while running.
    ///
    /// `Upload` jobs claim their full buffer size. `Copy` jobs claim a fixed 512 KiB token
    /// (they allocate no buffer) to prevent all copy jobs from running simultaneously.
    fn memory_budget_needed(&self) -> u32 {
        match self.part_job_type {
            PartJobType::Upload(_) => self.part_size.min(u32::MAX as usize) as u32,
            PartJobType::Copy { .. } => 512 * 1024, // 512KiB so we don't launch all of these jobs at once
        }
    }

    /// Executes the part job and returns the completed part (ETag + part number) for the manifest.
    async fn execute(
        self,
        bucket_name: String,
        archive_key: String,
        upload_id: String,
    ) -> Result<CompletedPart, Error> {
        let PartJob {
            part_number,
            part_size,
            part_job_type: part_blueprint,
        } = self;

        let etag = match part_blueprint {
            PartJobType::Copy { copy_source, range } => {
                let copy_part_request = s3().upload_part_copy();

                let copy_part_request = if let Some(range) = range {
                    debug!(
                        bucket_name,
                        archive_key,
                        upload_id,
                        part_number,
                        copy_source,
                        range,
                        "UploadPartCopy (ranged)"
                    );
                    copy_part_request.copy_source_range(range)
                } else {
                    debug!(
                        bucket_name,
                        archive_key, upload_id, part_number, copy_source, "UploadPartCopy"
                    );
                    copy_part_request
                };
                let result = copy_part_request
                    .bucket(bucket_name)
                    .key(archive_key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .copy_source(copy_source)
                    .send()
                    .await?;
                result
                    .copy_part_result
                    .and_then(|r| r.e_tag)
                    .ok_or("No ETag in UploadPartCopy response")?
            }
            PartJobType::Upload(upload_part_elems) => {
                // Allocate one contiguous buffer for the entire part, then carve it into
                // non-overlapping slices — one per element — so tasks can write in parallel
                // without any locking.
                let buffer = SharedBuf::with_capacity(part_size);

                let mut remain_buf = buffer.slice()?;
                let mut tasks = JoinSet::new();
                for elem in upload_part_elems {
                    let elem_size = elem.elem_size();

                    // Split off exactly `elem_size` bytes for this element; `remain` covers
                    // the rest of the buffer for subsequent elements.
                    let (mut buf, remain) = remain_buf.split(elem_size);
                    remain_buf = remain;
                    tasks.spawn(async move { elem.resolve_and_write(&mut buf).await });
                }

                // Drop the now-empty remainder so the Arc refcount can reach 1 when all
                // element slices are also dropped, allowing `into_inner` to succeed.
                drop(remain_buf);

                tasks
                    .join_all()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                let buffer = buffer.into_inner().expect("All slices have been droped");

                debug!(
                    bucket_name,
                    archive_key,
                    upload_id,
                    part_number,
                    "buffer.len()" = buffer.len(),
                    "UploadPart"
                );
                let result = s3()
                    .upload_part()
                    .bucket(bucket_name)
                    .key(archive_key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(buffer))
                    .send()
                    .await?;

                result.e_tag.ok_or("No ETag in UploadPart response")?
            }
        };

        intermitent_tracing!(
            part_number,
            part_number,
            part_size,
            etag = %etag,
            "Part uploaded"
        );

        Ok(CompletedPart::builder()
            .part_number(part_number)
            .e_tag(etag)
            .build())
    }
}

/// Distinguishes between a locally-built upload buffer and a server-side copy.
#[derive(Debug)]
enum PartJobType {
    /// Build a buffer from [`UploadPartElement`]s and upload it via `UploadPart`.
    Upload(Vec<UploadPartElement>),
    /// Transfer bytes directly between S3 objects via `UploadPartCopy`.
    Copy {
        /// `"bucket/key"` string required by the S3 `copy_source` parameter.
        copy_source: String,
        /// Optional `"bytes=start-end"` range for partial copies.
        range: Option<String>,
    },
}

/// The S3 operation used to fetch a file's content and CRC32 for an upload-part element.
#[derive(Debug)]
enum S3ObjectOperation {
    /// Full `GetObject` — downloads the entire file body.
    Get,
    /// Ranged `GetObject` for the first `size` bytes, plus a `HeadObject` for the full CRC32.
    PartialGet { size: usize },
    /// `HeadObject` only — no body download; used when the body is server-side copied.
    Head,
}

/// One logical segment within an `UploadPart` buffer.
#[derive(Debug)]
enum UploadPartElement {
    /// A ZIP entry: LOC header + (optionally) file body fetched from S3.
    EntryFromS3 {
        loc: LocalFileHeader<NoCRC>,
        operation: S3ObjectOperation,
        bucket_name: Arc<str>,
        key: String,
        /// Channel used to deliver the completed [`CentralDirectoryFileHeader`] after the CRC32 is known.
        cdfh_sender: UnboundedSender<CentralDirectoryFileHeader>,
    },
    /// The Central Directory block: drains all CDFHs from the channel, then writes EOCD.
    CentralDirectory {
        total_size: usize,
        eocd64: EndOfCentralDirectory64,
        cdfh_receiver: UnboundedReceiver<CentralDirectoryFileHeader>,
    },
}
impl UploadPartElement {
    /// Resolves the element's S3 data and writes ZIP bytes into `buf`.
    ///
    /// For `EntryFromS3`: fetches the CRC32 from S3 (S3 stores it as a base64-encoded,
    /// big-endian u32; [`CRC32::from_str`] decodes and byte-reverses it to little-endian for
    /// the ZIP field), writes the LOC header, sends the completed CDFH to the channel, then
    /// streams the file body (if any) into the remaining buffer space.
    ///
    /// For `CentralDirectory`: drains the CDFH channel until all entries are received, sorts
    /// them by archive offset via a `BTreeMap` (entries arrive in completion order, not layout
    /// order), then writes all CDFHs followed by the EOCD record(s).
    async fn resolve_and_write(self, buf: &mut [u8]) -> Result<(), Error> {
        match self {
            UploadPartElement::EntryFromS3 {
                loc,
                operation,
                bucket_name,
                key,
                cdfh_sender,
            } => {
                debug!(%bucket_name, %key, ?operation, loc_offset = loc.offset, buf_size = buf.len(), "Resolving entry from S3");
                let (crc, byte_stream) = match operation {
                    S3ObjectOperation::Get => {
                        // S3 returns the CRC32 as a base64-encoded big-endian u32 in the
                        // response header when the object was stored with a checksum.
                        let resp = s3()
                            .get_object()
                            .bucket(&*bucket_name)
                            .key(key)
                            .send()
                            .await?;

                        (
                            resp.checksum_crc32
                                .ok_or("No CRC32 in the GetObject response")?
                                .parse()?,
                            Some(resp.body),
                        )
                    }
                    S3ObjectOperation::PartialGet { size } => {
                        // Issue the ranged GET and the HeadObject concurrently: the GET fetches
                        // the partial body while the HEAD retrieves the full-file CRC32 (S3 only
                        // returns the checksum for the full object, not for byte ranges unfortunately).
                        let (get_obj_resp, head_obj_resp) = tokio::join!(
                            s3().get_object()
                                .bucket(&*bucket_name)
                                .key(key.clone())
                                .range(format!("bytes=0-{}", size - 1))
                                .send(),
                            s3().head_object()
                                .bucket(&*bucket_name)
                                .key(key)
                                .checksum_mode(ChecksumMode::Enabled)
                                .send()
                        );
                        (
                            head_obj_resp?
                                .checksum_crc32
                                .ok_or("No CRC32 in the (partial) GetObject response")?
                                .parse()?,
                            Some(get_obj_resp?.body),
                        )
                    }
                    S3ObjectOperation::Head => (
                        // no body to download; only the CRC32 is needed for the LOC of a subsequent FullCopy.
                        s3().head_object()
                            .bucket(&*bucket_name)
                            .key(key)
                            .checksum_mode(ChecksumMode::Enabled)
                            .send()
                            .await?
                            .checksum_crc32
                            .ok_or("No CRC32 in the HeadObject response")?
                            .parse()?,
                        None,
                    ),
                };

                // Promote the LOC from NoCRC to CRC32, write it, then notify the Central
                // Directory element that this entry's CDFH is ready.
                let loc = loc.set_crc(crc);
                let mut offset = loc.dump(buf.as_mut())?;
                cdfh_sender
                    .send(CentralDirectoryFileHeader::from_loc(loc))
                    .map_err(|_| "Could not send the CentralDirectoryFileHeader: Channel closed")?;
                if let Some(mut byte_stream) = byte_stream {
                    while let Some(chunk_result) = byte_stream.next().await {
                        let chunk = chunk_result?;
                        offset += (&chunk as &[u8]).dump(&mut buf[offset..])?;
                    }
                }
                debug!(bytes_written = offset, "Entry written to buffer");
                Ok(())
            }
            UploadPartElement::CentralDirectory {
                eocd64,
                mut cdfh_receiver,
                ..
            } => {
                debug!(
                    record_count = eocd64.record_count,
                    "Assembling Central Directory (waiting for all entry CRCs)"
                );
                // Collect CDFHs from all entry tasks. Tasks run concurrently so they arrive
                // out of order; a BTreeMap keyed by archive offset restores layout order.
                let mut entries: BTreeMap<usize, CentralDirectoryFileHeader> = BTreeMap::new();

                while let Some(cdfh) = cdfh_receiver.recv().await {
                    entries.insert(cdfh.0.offset, cdfh);
                    // Stop as soon as all expected entries have arrived.
                    if eocd64.record_count == entries.len() {
                        break;
                    }
                }

                let mut offset = 0;
                for cdfh in entries.into_values() {
                    offset += cdfh.dump(&mut buf[offset..])?;
                }

                // Then write the End Of Central Directory records
                let eocd_written = eocd64.dump(&mut buf[offset..])?;
                debug!(
                    cd_bytes = offset,
                    eocd_bytes = eocd_written,
                    "Central Directory written"
                );

                Ok(())
            }
        }
    }
    /// Returns the number of bytes this element will write into the upload buffer.
    fn elem_size(&self) -> usize {
        match self {
            UploadPartElement::EntryFromS3 { loc, operation, .. } => match operation {
                S3ObjectOperation::Get => loc.size() + loc.file_size,
                S3ObjectOperation::PartialGet { size } => loc.size() + *size,
                S3ObjectOperation::Head => loc.size(),
            },
            UploadPartElement::CentralDirectory { total_size, .. } => *total_size,
        }
    }
}

/// Serializes a value as raw ZIP bytes into a `Write` target, returning the number of bytes written.
///
/// All integer implementations write little-endian, matching the ZIP specification.
trait ZipSerialize {
    fn dump(&self, buf: impl Write) -> Result<usize, Error>;
}

/// Generates little-endian [`ZipSerialize`] impls for primitive integer types.
macro_rules! impl_int_zip_ser {
    ($($int_type:ty),+) => {
       $(
           impl ZipSerialize for $int_type {
               fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
                   Ok(buf.write(&self.to_le_bytes())?)
               }
           }
       )+
    };
}
impl_int_zip_ser!(u8, u16, u32, u64);

impl ZipSerialize for &[u8] {
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        Ok(buf.write(self)?)
    }
}

/// A ZIP magic number (signature), written big-endian so the byte sequence matches the spec
/// (e.g. `PK\x03\x04` for a Local File Header).
#[derive(Debug, Clone, Copy, Default)]
struct Magic(u32);
impl ZipSerialize for Magic {
    /// Writes the magic number in big-endian byte order (intentional — ZIP signatures are BE).
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        Ok(buf.write(&self.0.to_be_bytes())?)
    }
}

/// A CRC-32 checksum in the 4-byte little-endian form required by the ZIP specification.
#[derive(Debug, Clone, Copy, Default)]
struct CRC32([u8; 4]);
impl FromStr for CRC32 {
    type Err = Error;

    /// Parses the base64-encoded CRC32 returned by S3 into ZIP-ready little-endian bytes.
    ///
    /// S3 stores the checksum as a base64-encoded big-endian u32. The ZIP spec requires
    /// little-endian, so the decoded bytes are reversed before storing.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use base64::prelude::BASE64_STANDARD;

        let mut bytes = [0u8; 4];
        BASE64_STANDARD.decode_slice(s, &mut bytes)?;
        // S3 encodes the CRC32 big-endian; ZIP wants little-endian — reverse in place.
        bytes.reverse();
        Ok(Self(bytes))
    }
}
impl ZipSerialize for CRC32 {
    fn dump(&self, buf: impl Write) -> Result<usize, Error> {
        self.0.as_slice().dump(buf)
    }
}

/// Typestate marker for whether a [`LocalFileHeader`] has its CRC32 filled in.
pub trait CRCStatus {}

/// Typestate indicating the CRC32 is not yet known (before the S3 response is received).
#[derive(Debug)]
pub struct NoCRC;
impl CRCStatus for NoCRC {}
impl CRCStatus for CRC32 {}

/// ZIP Local File Header (LOC), parameterized by whether the CRC32 is known yet.
///
/// Created as `LocalFileHeader<NoCRC>` during layout planning (CRC unknown), then promoted to
/// `LocalFileHeader<CRC32>` via [`set_crc`](LocalFileHeader::set_crc) once the S3 response
/// provides the checksum. Only the `CRC32` variant implements [`ZipSerialize`].
#[derive(Debug)]
pub struct LocalFileHeader<C: CRCStatus> {
    /// Byte offset of this LOC within the final ZIP archive (used by the Central Directory).
    offset: usize,
    name: String,
    crc32: C,
    file_size: usize,
}
impl<C: CRCStatus> LocalFileHeader<C> {
    /// ZIP Local File Header signature (`PK\x03\x04`).
    const MAGIC: Magic = Magic(0x50_4B_03_04);
    /// Minimum version needed to extract (1.0).
    const MIN_VER: u16 = 10;
    const GENERAL_PURPOSE_BIG_FLAG: u16 = 0;
    /// Stored (uncompressed).
    const COMPRESSION_METHOD: u16 = 0;

    const FILE_LAST_MOD_TIME: u16 = 0;
    const FILE_LAST_MOD_DATE: u16 = 0;

    /// Fixed LOC field bytes, excluding the variable-length name and extra field.
    const BASE_LOC_LENGTH: usize = 30;
    /// Size of the ZIP64 extra field block appended when `file_size > u32::MAX`.
    const LOC_ZIP64EXT_LENGTH: usize = 20;

    /// Returns the serialized byte size of this LOC header.
    fn size(&self) -> usize {
        Self::predict_size(self.file_size, self.name.len())
    }

    /// Computes the LOC byte size without constructing a header — used during layout planning.
    ///
    /// Includes the ZIP64 extra field when `file_size > u32::MAX`.
    pub fn predict_size(file_size: usize, name_len: usize) -> usize {
        if file_size > u32::MAX as usize {
            Self::BASE_LOC_LENGTH + Self::LOC_ZIP64EXT_LENGTH + name_len
        } else {
            Self::BASE_LOC_LENGTH + name_len
        }
    }
}
impl LocalFileHeader<NoCRC> {
    /// Creates a new LOC header at the given archive offset, with the CRC32 not yet known.
    pub fn new(offset: usize, name: String, size: usize) -> Self {
        Self {
            offset,
            name,
            crc32: NoCRC,
            file_size: size,
        }
    }
    /// Promotes this header to the serializable form once the CRC32 is available.
    fn set_crc(self, crc32: CRC32) -> LocalFileHeader<CRC32> {
        let Self {
            offset,
            name,
            file_size,
            ..
        } = self;
        LocalFileHeader {
            offset,
            name,
            crc32,
            file_size,
        }
    }
}
impl ZipSerialize for LocalFileHeader<CRC32> {
    /// Writes the LOC record, appending a ZIP64 extra field when `file_size > u32::MAX`.
    ///
    /// When ZIP64 is needed, the compressed/uncompressed size fields in the fixed header are
    /// set to `0xFFFFFFFF` (the ZIP64 sentinel) and the real sizes go in the extra field.
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        // Determine once whether we need the ZIP64 extension for this entry.
        let need_zip64_extension = self.file_size > u32::MAX as usize;

        written += Self::MAGIC.dump(&mut buf)?;
        written += Self::MIN_VER.dump(&mut buf)?;
        written += Self::GENERAL_PURPOSE_BIG_FLAG.dump(&mut buf)?;
        written += Self::COMPRESSION_METHOD.dump(&mut buf)?;
        written += Self::FILE_LAST_MOD_TIME.dump(&mut buf)?;
        written += Self::FILE_LAST_MOD_DATE.dump(&mut buf)?;
        // CRC
        written += self.crc32.dump(&mut buf)?;
        // COMPRESSED SIZE / UNCOMPRESSED SIZE
        // Use 0xFFFFFFFF sentinel when ZIP64 extension carries the real value.
        for _ in 0..2 {
            written += if need_zip64_extension {
                u32::MAX.dump(&mut buf)?
            } else {
                (self.file_size as u32).dump(&mut buf)?
            };
        }
        // NAME LEN
        written += (self.name.len() as u16).dump(&mut buf)?;
        // EXTRA_FIELD LEN — non-zero only when ZIP64 extension is present.
        written += if need_zip64_extension {
            (Self::LOC_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else {
            0u16.dump(&mut buf)?
        };
        // NAME
        written += self.name.as_bytes().dump(&mut buf)?;

        // ZIP64 extra field: header ID 0x0001, 16 bytes of uncompressed + compressed size.
        if need_zip64_extension {
            // HEADER ID (0x0001 = ZIP64 extended information)
            written += 1u16.dump(&mut buf)?;
            // Size of the extra field chunk (two u64s = 16 bytes)
            written += 16u16.dump(&mut buf)?;
            let size = self.file_size as u64;
            // UNCOMPRESSED SIZE / COMPRESSED SIZE (identical for STORED)
            for _ in 0..2 {
                written += size.dump(&mut buf)?;
            }
        };

        Ok(written)
    }
}

/// ZIP Central Directory File Header (CDFH), wrapping the completed [`LocalFileHeader<CRC32>`].
pub struct CentralDirectoryFileHeader(LocalFileHeader<CRC32>);

impl CentralDirectoryFileHeader {
    /// ZIP Central Directory File Header signature (`PK\x01\x02`).
    const MAGIC: Magic = Magic(0x50_4B_01_02);
    const VER_MADE_BY: u16 = 10;

    /// Fixed CDFH field bytes, excluding the variable-length name and extra field.
    const BASE_CDFH_LENGTH: usize = 46; // Without extra-field and name
    /// Extra field size when only the file size overflows u32 (20 bytes: sizes only).
    const CDFH_SIZE_ZIP64EXT_LENGTH: usize = LocalFileHeader::<CRC32>::LOC_ZIP64EXT_LENGTH;
    /// Extra field size when the LOC offset also overflows u32 (adds 8 bytes for the u64 offset).
    const CDFH_OFFSET_ZIP64EXT_LENGTH: usize = Self::CDFH_SIZE_ZIP64EXT_LENGTH + 8;

    /// Constructs a CDFH from a completed LOC header (reuses all fields).
    fn from_loc(loc: LocalFileHeader<CRC32>) -> Self {
        Self(loc)
    }

    /// Computes the CDFH byte size without constructing a header — used during layout planning.
    ///
    /// The ZIP64 extra field grows in two steps: first when `file_size > u32::MAX` (adds size
    /// fields), then again when `loc_offset > u32::MAX` (also adds the offset field).
    pub fn predict_size(file_size: usize, name_len: usize, loc_offset: usize) -> usize {
        if loc_offset > u32::MAX as usize {
            Self::BASE_CDFH_LENGTH + Self::CDFH_OFFSET_ZIP64EXT_LENGTH + name_len
        } else if file_size > u32::MAX as usize {
            Self::BASE_CDFH_LENGTH + Self::CDFH_SIZE_ZIP64EXT_LENGTH + name_len
        } else {
            Self::BASE_CDFH_LENGTH + name_len
        }
    }
}
impl ZipSerialize for CentralDirectoryFileHeader {
    /// Writes the CDFH record with ZIP64 extra fields as needed.
    ///
    /// Two independent overflow conditions are checked:
    /// - `need_size_zip64_extension`: file size or offset exceeds u32 → include size fields in extra.
    /// - `need_offset_zip64_extension`: LOC offset exceeds u32 → also include offset field in extra.
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        // Offset overflow implies size overflow: if the archive is > 4 GiB, both fields need ZIP64.
        let need_offset_zip64_extension = self.0.offset > u32::MAX as usize;
        let need_size_zip64_extension =
            need_offset_zip64_extension || self.0.file_size > u32::MAX as usize;

        written += Self::MAGIC.dump(&mut buf)?;
        written += Self::VER_MADE_BY.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::MIN_VER.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::GENERAL_PURPOSE_BIG_FLAG.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::COMPRESSION_METHOD.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::FILE_LAST_MOD_TIME.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::FILE_LAST_MOD_DATE.dump(&mut buf)?;
        // CRC
        written += self.0.crc32.dump(&mut buf)?;
        // COMPRESSED SIZE / UNCOMPRESSED SIZE — sentinel when ZIP64 extension carries real values.
        for _ in 0..2 {
            written += if need_size_zip64_extension {
                u32::MAX.dump(&mut buf)?
            } else {
                (self.0.file_size as u32).dump(&mut buf)?
            };
        }
        // NAME LEN
        written += (self.0.name.len() as u16).dump(&mut buf)?;
        // EXTRA_FIELD LEN — size depends on which ZIP64 fields are needed.
        written += if need_offset_zip64_extension {
            (Self::CDFH_OFFSET_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else if need_size_zip64_extension {
            (Self::CDFH_SIZE_ZIP64EXT_LENGTH as u16).dump(&mut buf)?
        } else {
            0u16.dump(&mut buf)?
        };
        // FILE COMMENT LEN
        written += 0u16.dump(&mut buf)?;
        // DISK NUMBER
        written += 0u16.dump(&mut buf)?;
        // INTERNAL FILE ATTR
        written += 0u16.dump(&mut buf)?;
        // EXTERNAL FILE ATTR
        written += 0u32.dump(&mut buf)?;

        // RELATIVE OFFSET — sentinel when ZIP64 extension carries the real value.
        written += if need_offset_zip64_extension {
            u32::MAX.dump(&mut buf)?
        } else {
            (self.0.offset as u32).dump(&mut buf)?
        };

        // NAME
        written += self.0.name.as_bytes().dump(&mut buf)?;

        // ZIP64 extra field: always includes sizes; includes offset only when needed.
        if need_size_zip64_extension {
            // HEADER ID (0x0001 = ZIP64 extended information)
            written += 1u16.dump(&mut buf)?;
            // Extra field chunk size: 16 bytes (sizes only) or 24 bytes (sizes + offset).
            written += if need_offset_zip64_extension {
                24u16.dump(&mut buf)?
            } else {
                16u16.dump(&mut buf)?
            };
            let size = self.0.file_size as u64;
            // UNCOMPRESSED SIZE / COMPRESSED SIZE
            for _ in 0..2 {
                written += size.dump(&mut buf)?;
            }
            // RELATIVE OFFSET (only when the LOC offset itself overflows u32)
            if need_offset_zip64_extension {
                written += (self.0.offset as u64).dump(&mut buf)?;
            }
        };

        Ok(written)
    }
}

/// Writes the End of Central Directory records (EOCD, and optionally EOCD64 + locator).
#[derive(Debug, Clone, Copy)]
struct EndOfCentralDirectory64 {
    record_count: usize,
    central_directory_size: usize,
    /// Byte offset of the first CDFH within the archive.
    central_directory_offset: usize,
    /// Byte offset of the EOCD64 record itself (needed by the EOCD64 locator).
    eocd64_offset: usize,
}
impl EndOfCentralDirectory64 {
    /// ZIP End of Central Directory signature (`PK\x05\x06`).
    const EOCD_MAGIC: Magic = Magic(0x50_4B_05_06);
    /// ZIP64 End of Central Directory record signature (`PK\x06\x06`).
    const EOCD64_MAGIC: Magic = Magic(0x50_4B_06_06);
    /// ZIP64 End of Central Directory locator signature (`PK\x06\x07`).
    const EOCD64_LOCATOR_MAGIC: Magic = Magic(0x50_4B_06_07);

    /// Fixed byte size of the classic EOCD record (no comment).
    const EOCD_LENGTH: usize = 22;
    /// Fixed byte size of the EOCD64 record.
    const EOCD64_LENGTH: usize = 56;
    /// Fixed byte size of the EOCD64 locator record.
    const EOCD64_LOCATOR_LENGTH: usize = 20;

    /// Returns true if any field would overflow its classic EOCD field width.
    fn need_eocd64(
        central_directory_size: usize,
        central_directory_offset: usize,
        record_count: usize,
    ) -> bool {
        central_directory_size > u32::MAX as usize
            || central_directory_offset > u32::MAX as usize
            || record_count > u16::MAX as usize
    }

    /// Total byte size of the end-of-archive block (EOCD only, or EOCD64 + locator + EOCD).
    fn size(&self) -> usize {
        if Self::need_eocd64(
            self.central_directory_size,
            self.central_directory_offset,
            self.record_count,
        ) {
            Self::EOCD_LENGTH + Self::EOCD64_LENGTH + Self::EOCD64_LOCATOR_LENGTH
        } else {
            Self::EOCD_LENGTH
        }
    }

    /// Writes the EOCD64 record.
    fn dump_eocd64(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;
        written += Self::EOCD64_MAGIC.dump(&mut buf)?;

        // Per the ZIP spec (APPNOTE 4.3.14), the "size of zip64 end of central directory record"
        // field is the number of bytes remaining in the record *after* this field itself.
        // The record is 56 bytes total; the magic (4) + this size field (8) = 12 bytes precede it,
        // so the value is 56 - 12 = 44.
        written += 44u64.dump(&mut buf)?;

        written += CentralDirectoryFileHeader::VER_MADE_BY.dump(&mut buf)?;
        written += LocalFileHeader::<CRC32>::MIN_VER.dump(&mut buf)?;
        // Number of this disk.
        // Disk where central directory starts.
        for _ in 0..2 {
            written += 0u32.dump(&mut buf)?;
        }
        // Number of central directory records on this disk.
        // Total number of central directory records.
        for _ in 0..2 {
            written += (self.record_count as u64).dump(&mut buf)?;
        }
        // Size of central directory in bytes.
        written += (self.central_directory_size as u64).dump(&mut buf)?;
        // Offset of start of central directory, relative to start of archive.
        written += (self.central_directory_offset as u64).dump(&mut buf)?;

        Ok(written)
    }
    /// Writes the EOCD64 locator record that points to the EOCD64 record.
    fn dump_eocd64_locator(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;
        written += Self::EOCD64_LOCATOR_MAGIC.dump(&mut buf)?;

        // Disk where EOCD64 starts.
        written += 0u32.dump(&mut buf)?;
        // Offset to start of EOCD64, relative to start of archive.
        written += (self.eocd64_offset as u64).dump(&mut buf)?;
        // Total number of disks.
        written += 0u32.dump(&mut buf)?;

        Ok(written)
    }
    /// Writes the classic EOCD record, using sentinel values (`0xFFFF`/`0xFFFFFFFF`) when ZIP64
    /// records are present so readers know to consult the EOCD64 for the real values.
    fn dump_eocd(&self, mut buf: impl Write, need_eocd64: bool) -> Result<usize, Error> {
        let mut written = 0;

        // Magic number. Must be 50 4B 05 06.
        written += Self::EOCD_MAGIC.dump(&mut buf)?;

        if need_eocd64 {
            // Number of this disk (or FF FF for ZIP64).
            // Disk where central directory starts (or FF FF for ZIP64).
            // Number of central directory records on this disk (or FF FF for ZIP64).
            // Total number of central directory records (or FF FF for ZIP64).
            for _ in 0..4 {
                written += u16::MAX.dump(&mut buf)?;
            }
            // Size of central directory in bytes (or FF FF FF FF for ZIP64).
            // Offset of start of central directory, relative to start of archive (or FF FF FF FF for ZIP64).
            for _ in 0..2 {
                written += u32::MAX.dump(&mut buf)?;
            }
        } else {
            // Number of this disk (or FF FF for ZIP64).
            // Disk where central directory starts (or FF FF for ZIP64).
            for _ in 0..2 {
                written += 0u16.dump(&mut buf)?;
            }
            // Number of central directory records on this disk (or FF FF for ZIP64).
            // Total number of central directory records (or FF FF for ZIP64).
            for _ in 0..2 {
                written += (self.record_count as u16).dump(&mut buf)?;
            }
            // Size of central directory in bytes (or FF FF FF FF for ZIP64).
            written += (self.central_directory_size as u32).dump(&mut buf)?;
            // Offset of start of central directory, relative to start of archive (or FF FF FF FF for ZIP64).
            written += (self.central_directory_offset as u32).dump(&mut buf)?;
        }
        // Comment length (n).
        written += 0u16.dump(&mut buf)?;

        Ok(written)
    }
}
impl ZipSerialize for EndOfCentralDirectory64 {
    fn dump(&self, mut buf: impl Write) -> Result<usize, Error> {
        let mut written = 0;

        let need_eocd64 = Self::need_eocd64(
            self.central_directory_size,
            self.central_directory_offset,
            self.record_count,
        );

        if need_eocd64 {
            written += self.dump_eocd64(&mut buf)?;
            written += self.dump_eocd64_locator(&mut buf)?;
        }
        written += self.dump_eocd(&mut buf, need_eocd64)?;

        Ok(written)
    }
}
