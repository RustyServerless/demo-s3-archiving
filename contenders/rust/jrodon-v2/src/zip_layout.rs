use std::collections::VecDeque;

use tracing::{debug, info, instrument};

use crate::{
    FileInfo,
    part_job::{LocalFileHeader, NoCRC, PartJob, PartJobsBuilder},
};

// ---------- Tunables ----------

/// Minimum size accepted for a non-final part in an S3 multipart upload (S3 hard limit).
///
/// Both the `UploadPart` and the `UploadPartCopy` halves of a Duo must individually meet this.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB

/// Preferred part size when there are no S3 minimum-size constraints to satisfy.
///
/// Used to drive how much files we pack into a single part before moving on.
const TARGET_PART_SIZE: usize = 10 * 1024 * 1024; // 10MB

// ---------- Layout planning ----------

/// Plans the complete multipart ZIP layout before any data is transferred.
///
/// Holds an ordered list of [`ZipLayoutPart`]s that can be mapped to S3 multipart parts.
#[derive(Debug)]
pub struct ZipLayout {
    parts: Vec<ZipLayoutPart>,
}
impl ZipLayout {
    /// Builds the ZIP layout from a list of source files.
    ///
    /// The algorithm runs four passes over the file list (sorted smallest-to-largest):
    ///
    /// 1. **Duo pass** — greedily pairs each large file (≥ `MIN_PART_SIZE`) with enough small
    ///    files to form a valid Duo: an `UploadPart` (small files + LOC) followed by an
    ///    `UploadPartCopy` (the large file body). Both halves must individually meet
    ///    `MIN_PART_SIZE`. If the regular half is still too small after draining all remaining
    ///    small files, the large file is folded into a `Single` instead.
    /// 2. **PartialCopy promotion pass** — for Duos where the regular part was still short,
    ///    the large file was split (`PartialCopy`): its first bytes pad the regular part and
    ///    the rest is server-side copied. This pass tries to absorb more small files so the
    ///    split point moves to 0 and the copy becomes a cheaper `FullCopy`.
    /// 3. **Padding pass** — tops up each Duo's regular part toward `TARGET_PART_SIZE` using
    ///    any remaining small files.
    /// 4. **Remainder pass** — packs leftover files into `Single` parts up to `TARGET_PART_SIZE`.
    #[instrument(skip(files_info), fields(files_info.len=%files_info.len()))]
    pub fn from_files_info(mut files_info: Vec<FileInfo>) -> Self {
        info!(file_count = files_info.len(), "Planning ZIP layout");

        // Sort ascending so the front is the smallest and the back is the largest.
        // We pop large files from the back (UploadPartCopy candidates) and small files
        // from the front (UploadPart padding).
        files_info.sort_by_key(|file_info| file_info.size);

        // VecDeque lets us pop from both ends in O(1).
        let mut file_entries = VecDeque::from_iter(files_info.into_iter().map(FileEntry::from));

        let mut parts = vec![];

        // --- Pass 1: Duo loop ---
        // For every file large enough to be an UploadPartCopy candidate (≥ MIN_PART_SIZE),
        // try to build a Duo: a regular UploadPart followed by an UploadPartCopy.
        while let Some(file_entry) = file_entries.back()
            && file_entry.file_size() >= MIN_PART_SIZE
        {
            // Pop the largest remaining file — this will be the UploadPartCopy half.
            let Some(file_entry) = file_entries.pop_back() else {
                break;
            };

            // How many bytes the regular (UploadPart) half must contribute so that
            // the UploadPartCopy half can still be ≥ MIN_PART_SIZE on its own.
            //
            // regular_min = MIN_PART_SIZE
            //             - loc_size          (LOC header goes in the regular part)
            //             - (file_size - MIN_PART_SIZE)  (bytes the copy can "donate" to regular)
            let min_missing_regular_part_size =
                MIN_PART_SIZE - file_entry.loc_size() - (file_entry.file_size() - MIN_PART_SIZE);

            let mut regular = RegularZipPart::new();

            let zip_layout_part = loop {
                if let Some(small_file_entry) = file_entries.pop_front() {
                    regular.push_entry(small_file_entry);
                } else {
                    // Ran out of small files before the regular part was large enough.
                    // Fold the large file into a Single instead of creating an invalid Duo.
                    regular.push_entry(file_entry);
                    debug!(
                        regular_entry_count = regular.0.len(),
                        "Creating Single part"
                    );
                    break ZipLayoutPart::Single(regular);
                };
                let regular_zip_part_size = regular.part_size();
                if regular_zip_part_size >= min_missing_regular_part_size {
                    // The regular part is large enough — we have a valid Duo.

                    // If the regular part alone already satisfies MIN_PART_SIZE (including the
                    // LOC), the copy can start at byte 0 (FullCopy). Otherwise we need to
                    // download the first `copy_start_byte` bytes into the regular part.
                    let copy_start_byte = MIN_PART_SIZE
                        .saturating_sub(regular_zip_part_size)
                        .saturating_sub(file_entry.loc_size());

                    let copy = if copy_start_byte > 0 {
                        CopyZipPart::PartialCopy {
                            file_entry,
                            copy_start_byte,
                        }
                    } else {
                        CopyZipPart::FullCopy(file_entry)
                    };
                    debug!(
                        regular_entry_count = regular.0.len(),
                        ?copy,
                        "Creating Duo part"
                    );
                    break ZipLayoutPart::Duo { regular, copy };
                }
            };

            parts.push(zip_layout_part);
        }

        // --- Pass 2: PartialCopy → FullCopy promotion ---
        // A PartialCopy means we had to download the first N bytes of the large file to pad
        // the regular part. If more small files are available, absorb them into the regular
        // part to reduce (or eliminate) that download.
        'outer: for zip_layout_part in parts.iter_mut() {
            match zip_layout_part {
                ZipLayoutPart::Single(_) => continue,
                ZipLayoutPart::Duo { regular, copy } => match copy {
                    CopyZipPart::FullCopy(_) => continue,
                    CopyZipPart::PartialCopy {
                        copy_start_byte, ..
                    } => {
                        while *copy_start_byte > 0 {
                            // Keep absorbing small files until the split point reaches 0.
                            let Some(file_entry) = file_entries.pop_front() else {
                                break 'outer;
                            };
                            *copy_start_byte =
                                copy_start_byte.saturating_sub(file_entry.entry_size());
                            regular.push_entry(file_entry);
                        }
                        if *copy_start_byte == 0 {
                            copy.convert_to_full_copy();
                        }
                    }
                },
            }
        }

        // --- Pass 3: Pad Duo regular parts toward TARGET_PART_SIZE ---
        'outer: for zip_layout_part in parts.iter_mut() {
            match zip_layout_part {
                ZipLayoutPart::Single(_) => continue,
                ZipLayoutPart::Duo { regular, .. } => {
                    let mut missing_size = TARGET_PART_SIZE.saturating_sub(regular.part_size());
                    while missing_size > 0 {
                        let Some(file_entry) = file_entries.pop_front() else {
                            break 'outer;
                        };
                        missing_size = missing_size.saturating_sub(file_entry.entry_size());
                        regular.push_entry(file_entry);
                    }
                }
            }
        }

        // --- Pass 4: Pack remaining small files into Single parts ---
        while !file_entries.is_empty() {
            let mut new_single = RegularZipPart::new();
            let mut missing_size = TARGET_PART_SIZE;
            while missing_size > 0 {
                let Some(file_entry) = file_entries.pop_front() else {
                    break;
                };
                missing_size = missing_size.saturating_sub(file_entry.entry_size());
                new_single.push_entry(file_entry);
            }
            parts.push(ZipLayoutPart::Single(new_single));
        }

        // Break the part count down by kind so the logs surface the strategy chosen.
        let single_count = parts
            .iter()
            .filter(|p| matches!(p, ZipLayoutPart::Single(_)))
            .count();
        let duo_count = parts.len() - single_count;
        info!(
            part_count = parts.len(),
            single_count, duo_count, "Layout done"
        );

        Self { parts }
    }

    /// Converts the layout into an list of [`PartJob`]s ready for execution.
    pub fn into_part_jobs(self) -> Vec<PartJob> {
        let mut builder = PartJobsBuilder::new();
        for part in self.parts {
            match part {
                ZipLayoutPart::Single(regular_zip_part) => {
                    builder.add_files(regular_zip_part.0.into_iter().map(|fe| fe.0));
                }
                ZipLayoutPart::Duo { regular, copy } => {
                    builder.add_files(regular.0.into_iter().map(|fe| fe.0));
                    match copy {
                        CopyZipPart::FullCopy(file_entry) => builder.copy_file(file_entry.0),
                        CopyZipPart::PartialCopy {
                            file_entry,
                            copy_start_byte,
                        } => builder.partial_copy_file(file_entry.0, copy_start_byte),
                    }
                }
            }
        }
        builder.finalize()
    }
}

/// One logical unit of the ZIP layout, mapping to one or two S3 multipart parts.
///
/// `Single` becomes one `UploadPart`. `Duo` becomes one `UploadPart` (the regular half,
/// containing LOC headers and small file bodies) immediately followed by one `UploadPartCopy`
/// (the large file body, copied S3-side).
#[derive(Debug)]
enum ZipLayoutPart {
    /// A single `UploadPart` containing one or more small files.
    Single(RegularZipPart),
    /// An `UploadPart` + `UploadPartCopy` pair for a large file.
    Duo {
        regular: RegularZipPart,
        copy: CopyZipPart,
    },
}

/// A collection of [`FileEntry`]s that will be serialized into a single `UploadPart` buffer.
#[derive(Debug)]
struct RegularZipPart(Vec<FileEntry>);
impl RegularZipPart {
    /// Creates an empty regular part.
    fn new() -> Self {
        Self(vec![])
    }

    /// Appends a file entry to this part.
    fn push_entry(&mut self, file_entry: FileEntry) {
        self.0.push(file_entry);
    }

    /// Returns the total byte size this part will occupy in the archive (LOC headers + file data).
    fn part_size(&self) -> usize {
        self.0.iter().map(|fe| fe.entry_size()).sum()
    }
}

/// The server-side-copy half of a [`ZipLayoutPart::Duo`].
///
/// `FullCopy` — only the LOC header goes into the regular `UploadPart`; the entire file body
/// is copied via `UploadPartCopy`. `PartialCopy` — the first `copy_start_byte` bytes of the
/// file are downloaded into the regular part to satisfy `MIN_PART_SIZE`; the remainder is
/// server-side copied.
#[derive(Debug)]
enum CopyZipPart {
    /// The entire file body is transferred via `UploadPartCopy` (no download needed).
    FullCopy(FileEntry),
    /// The file is split: bytes `0..copy_start_byte` are downloaded into the regular part,
    /// bytes `copy_start_byte..` are server-side copied.
    PartialCopy {
        file_entry: FileEntry,
        /// Byte offset where the server-side copy begins (exclusive end of the downloaded range).
        copy_start_byte: usize,
    },
}
impl CopyZipPart {
    /// Promotes a `PartialCopy` to a `FullCopy` once `copy_start_byte` has been reduced to 0.
    #[instrument]
    fn convert_to_full_copy(&mut self) {
        debug!("Promoting to FullCopy");
        if let CopyZipPart::PartialCopy { file_entry, .. } = self {
            *self = Self::FullCopy(std::mem::take(file_entry));
        }
    }
}

/// Newtype wrapping [`FileInfo`] with ZIP-layout size helpers.
#[derive(Debug)]
struct FileEntry(FileInfo);
impl From<FileInfo> for FileEntry {
    fn from(file_info: FileInfo) -> Self {
        Self(file_info)
    }
}
/// Required by [`std::mem::take`] when promoting a `PartialCopy` to a `FullCopy`.
impl Default for FileEntry {
    fn default() -> Self {
        Self(FileInfo {
            name: Default::default(),
            bucket_name: Default::default(),
            key: Default::default(),
            size: Default::default(),
        })
    }
}
impl FileEntry {
    /// Byte size of the ZIP Local File Header (LOC) for this entry, including any ZIP64 extension.
    fn loc_size(&self) -> usize {
        LocalFileHeader::<NoCRC>::predict_size(self.file_size(), self.0.name.len())
    }

    /// Raw file payload size in bytes.
    fn file_size(&self) -> usize {
        self.0.size
    }

    /// Total bytes this entry occupies in the archive: LOC header + file payload.
    fn entry_size(&self) -> usize {
        self.loc_size() + self.0.size
    }
}
