//! Thin wrapper around [`zip::ZipWriter`] for streaming a flat ZIP into a `W: Write` sink.
//!
//! `CompressionMethod::Stored` is hardcoded because the source files are already-compressed
//! photo bytes; compressing them again would waste CPU without reducing size.

use std::io::Write;

use zip::{
    CompressionMethod, ZipWriter,
    write::{SimpleFileOptions, StreamWriter},
};

/// Wraps a [`zip::ZipWriter`] configured for streaming output into an arbitrary [`Write`] sink.
pub struct Zipper<W: Write + Send + Default + 'static> {
    inner: ZipWriter<StreamWriter<W>>,
}

impl<W: Write + Send + Default + 'static> Zipper<W> {
    /// Creates a new `Zipper` that writes into `writer`.
    pub fn new(writer: W) -> Self {
        Self {
            inner: ZipWriter::new_stream(writer),
        }
    }

    /// Appends `data` as a stored (uncompressed) entry named `filename`.
    pub fn add_file(&mut self, filename: String, data: Vec<u8>) -> Result<(), String> {
        self.inner
            .start_file(
                &filename,
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )
            .map_err(|e| format!("Failed to start ZIP file {filename}: {e}"))?;
        self.inner
            .write_all(&data)
            .map_err(|e| format!("Failed to write ZIP file {filename}: {e}"))?;
        Ok(())
    }

    /// Writes the ZIP central directory and returns the inner writer back to the caller.
    pub fn finish(self) -> Result<W, String> {
        Ok(self.inner.finish().map_err(|e| e.to_string())?.into_inner())
    }
}
