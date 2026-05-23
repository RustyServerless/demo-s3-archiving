use std::{iter, sync::Arc};
use tokio::sync::mpsc;
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum SlabState {
    Free = 0,
    Filling = 1,
    Ready = 2,
}
mod slab {
    use super::SlabState;
    use std::sync::atomic::{
        AtomicU8,
        Ordering::{Acquire, Relaxed, Release},
    };

    pub struct Slab {
        state: AtomicU8,
    }
    impl Slab {
        pub fn new(state: SlabState) -> Self {
            Self {
                state: AtomicU8::new(state as u8),
            }
        }
        pub fn state(&self) -> SlabState {
            // SAFETY:
            // - SlabState is repr(u8)
            // - the content of self.state is guaranteed to correspond to a SlabState
            unsafe { std::mem::transmute(self.state.load(Acquire)) }
        }

        pub fn swap_state(&self, new_state: SlabState) -> SlabState {
            let prev = self.state.swap(new_state as u8, Release);
            // SAFETY:
            // - SlabState is repr(u8)
            // - the content of prev is guaranteed to correspond to a SlabState
            unsafe { std::mem::transmute(prev) }
        }
        pub fn set_state(&self, new_state: SlabState) {
            self.state.store(new_state as u8, Relaxed);
        }
    }
}
use slab::Slab;
pub struct SlabRing {
    buf: Vec<u8>, // single contiguous allocation
    slabs: Vec<Slab>,
    slab_size: usize,
    ready_tx: mpsc::UnboundedSender<SlabLease>,
}

impl SlabRing {
    pub fn new(slab_size: usize, slab_count: usize) -> (Writer, Reader) {
        info!(
            "Creating SlabRing with slab_size={}, slab_count={}",
            slab_size, slab_count
        );
        // pre-allocate
        let mut buf = Vec::with_capacity(slab_size * slab_count);
        // safety: set_len to capacity; we manage initialization manually by writing bytes
        unsafe {
            buf.set_len(buf.capacity());
        }

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

    fn is_zero_space(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    fn slab_start(&self, slab_idx: usize) -> usize {
        slab_idx * self.slab_size
    }
}

pub struct SlabLease {
    ring: Arc<SlabRing>,
    slab_idx: usize,
    // read-only view over sealed data (exactly SLAB_SIZE bytes)
    data: std::ops::Range<usize>,
}

impl SlabLease {
    /// Read-only slice for upload
    pub fn as_slice<'a>(&'a self) -> &'a [u8] {
        &self.ring.buf[self.data.clone()]
    }
    /// Read-only slice for upload
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

pub struct Reader {
    ready_rx: mpsc::UnboundedReceiver<SlabLease>,
}

impl Reader {
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

pub struct Writer {
    ring: Arc<SlabRing>,
    slab_idx: usize,
    offset: usize, // bytes written in current slab
}
impl Default for Writer {
    fn default() -> Self {
        SlabRing::new(0, 0).0
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
        while buf.len() > 0 {
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

    fn advance(&mut self) {
        // Advance to the next slab if possible
        debug!("Advancing from slab_idx={}", self.slab_idx);
        // Will block until it is possible
        self.slab_idx = self.find_and_claim_free_slab();
        self.offset = 0;
        debug!("Successfully advanced to slab_idx={}", self.slab_idx);
    }

    fn seal_and_advance(&mut self) {
        self.seal();
        self.advance();
    }

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
