use std::{
    collections::BTreeSet,
    io::{Read, Seek},
    ops::{Deref, DerefMut, Range},
    sync::mpsc::{Receiver, channel},
};

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use tracing::{debug, error, info, instrument, trace};

use crate::{ControlError, s3_exec};

type Offset = usize;
const MIN_WINDOW_SIZE: usize = 16 * 1024 * 1024;
const MAX_WINDOW_SIZE: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowRange(Range<Offset>);
impl WindowRange {
    fn new(start_position: Offset, length: usize) -> Self {
        Self::from(start_position..start_position + length)
    }
    fn covers(&self, other: &Self) -> bool {
        self.start <= other.start && self.end >= other.end
    }
}
impl From<Range<Offset>> for WindowRange {
    fn from(value: Range<Offset>) -> Self {
        Self(value)
    }
}
impl Deref for WindowRange {
    type Target = Range<Offset>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Ord for WindowRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        match self.0.start.cmp(&other.0.start) {
            Equal => self.0.end.cmp(&other.0.end),
            o => o,
        }
    }
}
impl PartialOrd for WindowRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// #[derive(Debug, Clone, Default)]
// struct WindowRangeMap<T>(BTreeMap<WindowRange, T>);
// impl<T> Deref for WindowRangeMap<T> {
//     type Target = BTreeMap<WindowRange, T>;
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
// impl<T> DerefMut for WindowRangeMap<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }
// impl<T> WindowRangeMap<T> {
//     fn get_position(&self, position: Offset) -> Option<(&WindowRange, &T)> {
//         if let Some(candidate) = self
//             .range(..WindowRange(position..position + 1))
//             .next_back()
//             && candidate.0.contains(&position)
//         {
//             Some(candidate)
//         } else if let Some(candidate) = self.range(WindowRange(position..position + 1)..).next()
//             && candidate.0.contains(&position)
//         {
//             Some(candidate)
//         } else {
//             None
//         }
//     }
// }

#[derive(Debug, Clone, Default)]
struct WindowRangeSet(BTreeSet<WindowRange>);
impl Deref for WindowRangeSet {
    type Target = BTreeSet<WindowRange>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for WindowRangeSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl WindowRangeSet {
    fn get_position_cover(&self, position: Offset) -> Option<&WindowRange> {
        if let Some(candidate) = self
            .range(..WindowRange(position..position + 1))
            .next_back()
            && candidate.contains(&position)
        {
            Some(candidate)
        } else if let Some(candidate) = self.range(WindowRange(position..position + 1)..).next()
            && candidate.contains(&position)
        {
            Some(candidate)
        } else {
            None
        }
    }
    fn get_range_cover(&self, range: WindowRange) -> Option<&WindowRange> {
        if let Some(candidate) = self.range(..range.clone()).next_back()
            && candidate.covers(&range)
        {
            Some(candidate)
        } else if let Some(candidate) = self.range(range.clone()..).next()
            && candidate.covers(&range)
        {
            Some(candidate)
        } else {
            None
        }
    }
}

struct S3ObjectReaderWindow {
    range: WindowRange,
    buf: Vec<u8>,
}
impl core::fmt::Debug for S3ObjectReaderWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ObjectReaderWindow")
            .field("range", &self.range)
            .field("buf.len()", &self.buf.len())
            .finish_non_exhaustive()
    }
}
impl S3ObjectReaderWindow {
    fn start_position(&self) -> Offset {
        self.range.start
    }
    fn end_position_excl(&self) -> Offset {
        self.range.end
    }
    fn position_range(&self) -> WindowRange {
        WindowRange::from(self.start_position()..self.end_position_excl())
    }
    fn contains_position(&self, position: Offset) -> bool {
        self.position_range().contains(&position)
    }
    fn buf_from(&self, position: Offset) -> &[u8] {
        &self.buf[(position - self.start_position())..]
    }
}

pub struct S3ObjectReader {
    current_window: Option<S3ObjectReaderWindow>,
    position: usize,
    object_size: usize,
    windows_request_tx: UnboundedSender<WindowRange>,
    windows_rx: Receiver<S3ObjectReaderWindow>,
    incoming_windows: WindowRangeSet,
}
impl S3ObjectReader {
    #[instrument(skip(client), fields(%bucket, %key))]
    pub async fn create(
        client: aws_sdk_s3::Client,
        bucket: String,
        key: String,
    ) -> Result<Self, ControlError> {
        let resp = s3_exec(client.head_object().bucket(&bucket).key(&key).send()).await?;
        let object_size = resp.content_length.expect("can it really be absent?") as usize;
        info!(
            object_size,
            window_size = MAX_WINDOW_SIZE,
            "S3ObjectReader created"
        );
        let (windows_request_tx, mut windows_request_rx) = unbounded_channel::<WindowRange>();
        let (windows_tx, windows_rx) = channel();

        tokio::spawn(async move {
            while let Some(window_range) = windows_request_rx.recv().await {
                let range_start = window_range.start;
                let range_end_inclusive = window_range.end - 1;
                let range_len = window_range.len();
                debug!(
                    range_start,
                    range_end_inclusive, range_len, "fetching S3 byte range"
                );
                let mut resp = s3_exec(
                    client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .range(format!("bytes={range_start}-{range_end_inclusive}"))
                        .send(),
                )
                .await?;
                // I DO NOT use the `response.body.collect()` helper here because it allocates
                // internally a lot of intermediate buffers that amplify the memory footprint
                // of the download process x3 (a 50MB photo download results in ~150MB of memory consumption).
                // Instead, I'm manually collecting chunks and adding them in a pre-allocated data vec,
                // resulting in a minimal overhead per-download task of the S3 chunk_size (16KB from my tests).
                let mut buf = Vec::with_capacity(window_range.len());
                let mut chunks: usize = 0;
                while let Some(chunk_result) = resp.body.next().await {
                    let chunk = chunk_result?;
                    trace!(chunk_size = chunk.len(), "received S3 chunk");
                    buf.extend_from_slice(&chunk);
                    chunks += 1;
                }
                debug!(?window_range, chunks, "S3 byte range download complete");
                windows_tx
                    .send(S3ObjectReaderWindow {
                        range: window_range,
                        buf,
                    })
                    .map_err(|error| {
                        error!(?error, "Channel Closed");
                        ControlError::ChannelClosed
                    })?;
            }
            Ok::<_, ControlError>(())
        });

        Ok(Self {
            current_window: None,
            position: 0,
            object_size,
            windows_request_tx,
            windows_rx,
            incoming_windows: WindowRangeSet::default(),
        })
    }

    #[instrument(skip_all, fields(position = self.position))]
    fn current_window(&mut self) -> &S3ObjectReaderWindow {
        use std::sync::mpsc::TryRecvError;
        loop {
            if self
                .current_window
                .as_ref()
                .is_some_and(|cw| cw.contains_position(self.position))
            {
                trace!("Found window");
                self.ask_next_window();
                break self.current_window.as_ref().unwrap();
            }
            self.current_window = match self.windows_rx.try_recv() {
                Ok(new_window) => {
                    debug!(
                        window_range = ?new_window.range,
                        window_len = new_window.buf.len(),
                        "received window (try_recv)"
                    );
                    self.incoming_windows.remove(&new_window.position_range());
                    new_window
                        .contains_position(self.position)
                        .then_some(new_window)
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Window loading channel closed (try_recv disconnected)");
                    panic!("Window loading channel closed")
                }
                _ => {
                    if self
                        .incoming_windows
                        .get_position_cover(self.position)
                        .is_none()
                    {
                        debug!(
                            position = self.position,
                            "no in-flight window covers position; requesting one"
                        );
                        self.ask_position_window();
                    } else {
                        debug!(
                            position = self.position,
                            in_flight = self.incoming_windows.len(),
                            "waiting on in-flight window"
                        );
                    }
                    match self.windows_rx.recv() {
                        Ok(new_window) => {
                            debug!(
                                window_range = ?new_window.range,
                                "received window (blocking recv)"
                            );
                            self.incoming_windows.remove(&new_window.position_range());
                            new_window
                                .contains_position(self.position)
                                .then_some(new_window)
                        }
                        Err(_) => {
                            error!("Window loading channel closed (blocking recv)");
                            panic!("Window loading channel closed")
                        }
                    }
                }
            };
        }
    }

    fn ask_next_window(&mut self) {
        if let Some(ref cw) = self.current_window {
            self.ask_window(
                cw.end_position_excl(),
                MAX_WINDOW_SIZE.min(cw.range.len() * 2),
            );
        } else {
            self.ask_position_window();
        }
    }
    fn ask_position_window(&mut self) {
        self.ask_window(self.position, MIN_WINDOW_SIZE);
    }
    #[instrument(skip(self), fields(object_size = self.object_size))]
    fn ask_window(&mut self, offset: Offset, size: usize) {
        let range = if offset + size > self.object_size {
            if self.object_size.saturating_sub(offset) >= MIN_WINDOW_SIZE {
                WindowRange::new(offset, self.object_size - offset)
            } else {
                WindowRange::new(
                    self.object_size.saturating_sub(MIN_WINDOW_SIZE),
                    MIN_WINDOW_SIZE,
                )
            }
        } else {
            WindowRange::new(offset, size)
        };

        // Don't ask for windows already covered by current or in-flights
        if !(self
            .current_window
            .as_ref()
            .is_some_and(|cw| cw.range.covers(&range))
            || self
                .incoming_windows
                .get_range_cover(range.clone())
                .is_some())
        {
            debug!(
                range_start = range.start,
                range_end = range.end,
                range_len = range.len(),
                in_flight = self.incoming_windows.len(),
                "requesting window"
            );
            self.incoming_windows.insert(range.clone());
            self.windows_request_tx.send(range).unwrap_or_else(|error| {
                error!(?error, "Window request channel closed");
                panic!("Window request channel closed");
            });
        }
    }
}

impl Read for S3ObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let position = self.position;
        let read_buf = self.current_window().buf_from(position);

        let read_size = if read_buf.len() <= buf.len() {
            buf[..read_buf.len()].copy_from_slice(read_buf);
            read_buf.len()
        } else {
            buf.copy_from_slice(&read_buf[..buf.len()]);
            buf.len()
        };
        self.position += read_size;
        trace!(
            position,
            read_size,
            requested = buf.len(),
            "S3ObjectReader::read"
        );
        Ok(read_size)
    }
}
impl Seek for S3ObjectReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Start(new_position) => self.position = new_position as usize,
            std::io::SeekFrom::End(offset) => {
                self.position = if offset >= 0 {
                    self.object_size
                } else {
                    self.object_size.saturating_sub(-offset as usize)
                };
            }
            std::io::SeekFrom::Current(offset) => {
                self.position = self.position.saturating_add_signed(offset as isize);
            }
        };
        trace!(?pos, position = self.position, "S3ObjectReader::seek");
        Ok(self.position as u64)
    }
}
