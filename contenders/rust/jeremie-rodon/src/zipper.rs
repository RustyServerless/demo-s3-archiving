use std::io::Write;

use zip::{
    CompressionMethod, ZipWriter,
    write::{SimpleFileOptions, StreamWriter},
};

pub struct Zipper<W: Write + Send + Default + 'static> {
    inner: ZipWriter<StreamWriter<W>>,
}

impl<W: Write + Send + Default + 'static> Zipper<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: ZipWriter::new_stream(writer),
        }
    }

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

    pub fn finish(self) -> Result<W, String> {
        Ok(self.inner.finish().map_err(|e| e.to_string())?.into_inner())
    }
}
