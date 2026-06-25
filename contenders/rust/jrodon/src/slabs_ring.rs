//! Single-producer single-consumer ring buffer of fixed-size byte slabs.
//!
//! Designed to stream bytes from a synchronous [`std::io::Write`] producer ([`Writer`]) to an
//! async consumer ([`Reader`]) without per-chunk allocation. The buffer is one contiguous
//! `Vec<u8>` carved into `N` slabs; each slab cycles through `Free → Filling → Ready → Free`
//! states tracked by an atomic per-slab cell. When all slabs are `Ready` (consumer is behind),
//! the writer busy-spins with 10 ms sleeps — callers must therefore run [`Writer`] inside
//! `tokio::task::spawn_blocking`. Buffer recycling is driven by [`Drop`] on [`SlabLease`].

use std::{iter, sync::Arc};
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Lifecycle state of a single slab in the ring buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum SlabState {
    /// Available for the writer to claim.
    Free = 0,
    /// Currently being written by the [`Writer`].
    Filling = 1,
    /// Sealed and waiting for (or held by) the [`Reader`].
    Ready = 2,
}
/// Atomic per-slab state cell shared between the writer and the consumer.
mod slab {
    use super::SlabState;
    use std::sync::atomic::{
        AtomicU8,
        Ordering::{Acquire, Relaxed, Release},
    };

    /// Holds the atomic [`SlabState`] for one slab.
    pub struct Slab {
        state: AtomicU8,
    }
    impl Slab {
        /// Creates a slab with the given initial state.
        pub fn new(state: SlabState) -> Self {
            Self {
                state: AtomicU8::new(state as u8),
            }
        }

        /// Loads the current state with `Acquire` ordering so the caller sees all writes
        /// that the thread which last set the state had made before releasing it.
        pub fn state(&self) -> SlabState {
            // SAFETY:
            // - SlabState is repr(u8)
            // - the content of self.state is guaranteed to correspond to a SlabState
            unsafe { std::mem::transmute(self.state.load(Acquire)) }
        }

        /// Atomically swaps to `new_state` with `Release` ordering, publishing all preceding
        /// writes to the slab data before the state change becomes visible to other threads.
        pub fn swap_state(&self, new_state: SlabState) -> SlabState {
            let prev = self.state.swap(new_state as u8, Release);
            // SAFETY:
            // - SlabState is repr(u8)
            // - the content of prev is guaranteed to correspond to a SlabState
            unsafe { std::mem::transmute(prev) }
        }

        /// Sets the state with `Relaxed` ordering; use only during single-threaded initialization.
        pub fn set_state(&self, new_state: SlabState) {
            self.state.store(new_state as u8, Relaxed);
        }
    }
}
use slab::Slab;

/// Shared backing store for the ring buffer; not used directly — construct via [`SlabRing::new`].
pub struct SlabRing {
    buf: Vec<u8>, // single contiguous allocation
    slabs: Vec<Slab>,
    slab_size: usize,
    ready_tx: mpsc::UnboundedSender<SlabLease>,
}

impl SlabRing {
    /// Allocates the ring buffer and returns a `(Writer, Reader)` pair.
    ///
    /// The backing `Vec<u8>` is allocated at full capacity with `set_len` (contents are
    /// uninitialised but never read before being written). The first slab is pre-marked
    /// `Filling` so the [`Writer`] can begin immediately without claiming a free slab.
    pub fn create(slab_size: usize, slab_count: usize) -> (Writer, Reader) {
        info!(
            "Creating SlabRing with slab_size={}, slab_count={}",
            slab_size, slab_count
        );
        // pre-allocate
        let buf = vec![0; slab_size * slab_count];

        let (tx, rx) = mpsc::unbounded_channel::<SlabLease>(); // capacity == #slabs: never blocks producer when a slab is sealed if a free exists
        let ring = Arc::new(Self {
            buf,
            slabs: iter::repeat_with(|| Slab::new(SlabState::Free))
                .take(slab_count)
                .collect(),
            slab_size,
            ready_tx: tx,
        });

        if !ring.slabs.is_empty() {
            // mark the first slab Filling so the writer can start
            ring.slabs[0].set_state(SlabState::Filling);
            debug!("Initialized first slab to Filling state");
        } else {
            debug!("No slabs available - zero capacity SlabRing");
        }
        (
            Writer {
                ring,
                slab_idx: 0,
                offset: 0,
            },
            Reader { ready_rx: rx },
        )
    }

    /// Returns `true` if the ring was constructed with zero capacity (the `Default` writer case).
    fn is_zero_space(&self) -> bool {
        self.buf.is_empty()
    }

    /// Returns the byte offset of `slab_idx` within the backing buffer.
    #[inline]
    fn slab_start(&self, slab_idx: usize) -> usize {
        slab_idx * self.slab_size
    }
}

/// Handle to a sealed slab handed to the consumer.
///
/// While a `SlabLease` is alive the slab remains `Ready` and the writer cannot reclaim it.
/// Dropping the lease transitions the slab back to `Free`, making it available to the writer.
pub struct SlabLease {
    ring: Arc<SlabRing>,
    slab_idx: usize,
    // read-only view over sealed data (exactly SLAB_SIZE bytes)
    data: std::ops::Range<usize>,
}

impl SlabLease {
    /// Borrows the sealed slab data in place.
    pub fn as_slice(&self) -> &[u8] {
        &self.ring.buf[self.data.clone()]
    }
    /// Copies the slab data into an owned `Vec<u8>`.
    ///
    /// Used by the uploader because the AWS SDK requires an owned buffer.
    pub fn into_vec(self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

// When a lease is dropped, mark slab Free.
impl Drop for SlabLease {
    fn drop(&mut self) {
        debug!(
            "Dropping SlabLease for slab_idx={}, freeing {} bytes",
            self.slab_idx,
            self.data.len()
        );

        let slab = &self.ring.slabs[self.slab_idx];
        // Transition Ready/InFlight -> Free
        let prev = slab.swap_state(SlabState::Free);
        if prev != SlabState::Ready {
            unreachable!("Expected Ready, got {:?}", prev);
        }

        debug!("Slab {} marked as Free", self.slab_idx);
    }
}

/// Async consumer side of the ring buffer; yields sealed [`SlabLease`]s as they become ready.
pub struct Reader {
    ready_rx: mpsc::UnboundedReceiver<SlabLease>,
}

impl Reader {
    /// Waits for the next sealed slab, returning `None` when the [`Writer`] has been dropped.
    pub async fn recv(&mut self) -> Option<SlabLease> {
        debug!("Reader waiting for next SlabLease");
        let lease = self.ready_rx.recv().await;
        if let Some(ref lease) = lease {
            debug!(
                "Reader received SlabLease for slab_idx={}, data_len={}",
                lease.slab_idx,
                lease.data.len()
            );
        } else {
            debug!("Reader channel closed");
        }
        lease
    }
}

/// Sync producer side of the ring buffer; implements [`std::io::Write`].
///
/// `write` fills the current slab and spills into subsequent slabs as needed.
/// When all slabs are `Ready` (consumer is behind), [`Writer::find_and_claim_free_slab`]
/// busy-spins with 10 ms sleeps — callers must therefore run the writer inside
/// `tokio::task::spawn_blocking`. `flush` seals the current (possibly partial) slab so the
/// consumer sees the trailing bytes after the ZIP central directory is written.
pub struct Writer {
    ring: Arc<SlabRing>,
    slab_idx: usize,
    offset: usize, // bytes written in current slab
}
impl Default for Writer {
    /// Creates a zero-capacity [`Writer`] backed by an empty [`SlabRing`].
    ///
    /// Required because [`zipper::Zipper`] has a `W: Default` bound (the `zip` crate's
    /// `StreamWriter` swaps the inner writer out on `finish`).
    fn default() -> Self {
        SlabRing::create(0, 0).0
    }
}
impl std::io::Write for Writer {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        debug!(
            "Writer attempting to write {} bytes to slab_idx={}, offset={}",
            buf.len(),
            self.slab_idx,
            self.offset
        );

        if self.ring.is_zero_space() {
            use std::io::{Error, ErrorKind};
            debug!("Write failed: SlabRing has zero space");
            return Err(Error::new(
                ErrorKind::OutOfMemory,
                "This SlabRing buffer has 0 space".to_owned(),
            ));
        }
        let mut total_written = 0usize;
        while !buf.is_empty() {
            // space left in the current slab
            let room = self.ring.slab_size - self.offset;
            if room == 0 {
                debug!("Current slab full, sealing and advancing to next");
                self.seal_and_advance();
            }
            let n = buf.len().min(room);
            // SAFETY: We only write into the slab currently in Filling state, and no consumers read it yet.
            let dst_start = self.ring.slab_start(self.slab_idx) + self.offset;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    self.ring.buf.as_ptr().add(dst_start) as *mut u8,
                    n,
                );
            }
            self.offset += n;
            total_written += n;
            buf = &buf[n..];
        }
        debug!("Writer completed write of {} bytes total", total_written);
        Ok(total_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        debug!("Writer flush requested");
        if self.ring.is_zero_space() {
            debug!("Flush: no-op for zero space ring");
        } else if self.offset == 0 {
            debug!("Flush: nothing to flush");
        } else {
            debug!("Flush: sealing current slab");
            self.seal()
        }
        Ok(())
    }
}
impl Writer {
    // pub fn can_write_without_blocking(&self, len: usize) -> bool {
    //     let immediate_free_space = self.ring.slab_size - self.offset;
    //     if immediate_free_space >= len {
    //         return true;
    //     }
    //     let currently_free_slabs_count = self
    //         .ring
    //         .slabs
    //         .iter()
    //         .filter(|slab| slab.state() == SlabState::Free)
    //         .count();

    //     return immediate_free_space + currently_free_slabs_count * self.ring.slab_size >= len;
    // }
    /// Transitions the current slab from `Filling` to `Ready` and sends a [`SlabLease`] to the consumer.
    fn seal(&mut self) {
        debug!(
            "Sealing slab_idx={} with {} bytes",
            self.slab_idx, self.offset
        );

        // Mark current slab Ready and enqueue a lease
        let slab = &self.ring.slabs[self.slab_idx];
        let prev = slab.swap_state(SlabState::Ready);
        if prev == SlabState::Filling {
            let start = self.ring.slab_start(self.slab_idx);
            let lease = SlabLease {
                ring: self.ring.clone(),
                slab_idx: self.slab_idx,
                data: start..(start + self.offset), // allow partial final slab too
            };
            // Send to consumers (unbounded channel)
            self.ring.ready_tx.send(lease).ok();
            debug!(
                "Sealed slab_idx={}, sent lease with {} bytes to consumers",
                self.slab_idx, self.offset
            );
        } else {
            debug!("Current slab was already sealed")
        }
    }

    /// Claims the next free slab (blocking until one is available) and resets the write offset.
    fn advance(&mut self) {
        // Advance to the next slab if possible
        debug!("Advancing from slab_idx={}", self.slab_idx);
        // Will block until it is possible
        self.slab_idx = self.find_and_claim_free_slab();
        self.offset = 0;
        debug!("Successfully advanced to slab_idx={}", self.slab_idx);
    }

    /// Seals the current slab and advances to the next free one.
    fn seal_and_advance(&mut self) {
        self.seal();
        self.advance();
    }

    /// Scans the ring for a `Free` slab, atomically claims it as `Filling`, and returns its index.
    ///
    /// Busy-spins with 10 ms sleeps when no slab is free; this is why the caller must be on a
    /// blocking thread.
    fn find_and_claim_free_slab(&self) -> usize {
        debug!("Attempting to find a free slab_idx");

        let current_idx = self.slab_idx;

        let mut loop_counter = 0usize;
        // Wait loop
        loop {
            // Initialize the search at the current idx
            let mut next_idx = current_idx;
            // Search loop
            loop {
                // Next candidate
                next_idx = (next_idx + 1) % self.ring.slabs.len();
                // If we looped around, break out to the wait loop
                if next_idx == current_idx {
                    break;
                }

                debug!("Trying to claim slab_idx={}", next_idx);
                // Acquire to observe consumer's Free publication
                let slab = &self.ring.slabs[next_idx];
                if slab.state() == SlabState::Free {
                    // Transition Free -> Filling
                    let prev = slab.swap_state(SlabState::Filling);
                    if prev != SlabState::Free {
                        // raced; panic
                        unreachable!(
                            "RACE!! Single writer guarantee violated: expected Free, got {:?}",
                            prev
                        );
                    }
                    debug!("Successfully claimed slab_idx={}", next_idx);
                    return next_idx;
                }
            }
            // Only log the first time then once every 100 rounds, i.e. 1 second
            if loop_counter % 100 == 0 {
                debug!("No slab is free, waiting...");
            }
            loop_counter += 1;
            // Sleep 10ms, this is why the Writer should be called in a spawn_blocking task
            std::thread::sleep(core::time::Duration::from_millis(10));
        }
    }
}
