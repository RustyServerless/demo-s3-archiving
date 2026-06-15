use core::ops::{Deref, DerefMut};
use std::{cell::UnsafeCell, sync::Arc};

use tracing::{debug, instrument};

use crate::Error;

/// `UnsafeCell` wrapper that allows shared references across threads.
///
/// # Safety
///
/// `Sync` is sound here because the buffer is never accessed through a shared reference
/// directly. Instead, [`SharedBuf::slice`] hands out non-overlapping [`BufSlice`]s (enforced
/// by the single-owner check and the split protocol), so each byte is written by exactly one
/// task at a time — there is no aliasing.
#[derive(Debug)]
struct InnerSharedBuf(UnsafeCell<Vec<u8>>);
unsafe impl Sync for InnerSharedBuf {}

/// A reference-counted, pre-allocated byte buffer that can be split into non-overlapping slices
/// for parallel writes.
#[derive(Debug)]
pub struct SharedBuf(Arc<InnerSharedBuf>);
impl Clone for SharedBuf {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl SharedBuf {
    /// Allocates a zero-initialized buffer of `len` bytes.
    pub fn with_capacity(len: usize) -> Self {
        Self(Arc::new(InnerSharedBuf(UnsafeCell::new(vec![0; len]))))
    }

    /// Returns a [`BufSlice`] covering the entire buffer.
    ///
    /// Fails if any other `SharedBuf` clone (or existing slice) still holds a reference,
    /// ensuring that at most one slice tree exists at a time.
    pub fn slice(&self) -> Result<BufSlice, Error> {
        if Arc::strong_count(&self.0) == 1 {
            Ok(BufSlice {
                shared_buf: self.clone(),
                start_at: 0,
                length: unsafe { (*self.0.0.get()).len() },
            })
        } else {
            Err("Slice already exists")?
        }
    }

    /// Unwraps the inner `Vec<u8>` once all slices have been dropped (Arc refcount == 1).
    ///
    /// Returns `Err(self)` if other references still exist.
    pub fn into_inner(self) -> Result<Vec<u8>, Self> {
        // strong_count must be 1 here; if it isn't, a BufSlice is still alive and unwrap fails.
        debug!(
            strong_count = Arc::strong_count(&self.0),
            "Unwrapping SharedBuf"
        );
        Arc::try_unwrap(self.0)
            .map(|cell| cell.0.into_inner())
            .map_err(Self)
    }
}

/// An exclusive, non-overlapping view into a region of a [`SharedBuf`].
///
/// Obtained via [`SharedBuf::slice`] and further divided with [`split`](BufSlice::split).
/// Each `BufSlice` is the sole writer for its byte range.
#[derive(Debug)]
pub struct BufSlice {
    shared_buf: SharedBuf,
    /// Byte offset of this slice's first byte within the underlying buffer.
    start_at: usize,
    length: usize,
}

impl BufSlice {
    /// Splits this slice into two non-overlapping sub-slices at byte offset `at`.
    ///
    /// The left slice covers `[start_at, start_at + at)` and the right covers
    /// `[start_at + at, start_at + length)`. The caller must ensure `at <= length`.
    #[instrument(skip(self), fields(start_at=%self.start_at,length=%self.length))]
    pub fn split(self, at: usize) -> (Self, Self) {
        debug!("spliting buffer");
        let Self {
            shared_buf,
            start_at,
            length,
        } = self;
        (
            Self {
                shared_buf: shared_buf.clone(),
                start_at,
                length: at,
            },
            Self {
                // The right slice begins immediately after the left slice ends.
                shared_buf,
                start_at: start_at + at,
                length: length - at,
            },
        )
    }
}

impl Deref for BufSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            // SAFETY: `start_at + length <= buffer.len()` is guaranteed by the split protocol.
            // No other `BufSlice` aliases this range, so constructing a shared reference is sound.
            let ptr = (*self.shared_buf.0.0.get()).as_ptr().add(self.start_at);
            core::slice::from_raw_parts(ptr, self.length)
        }
    }
}
impl DerefMut for BufSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            // SAFETY: same non-overlap guarantee as `Deref`; `&mut self` ensures exclusive access
            // to this slice handle, so the mutable reference cannot alias another live reference.
            let ptr = (*self.shared_buf.0.0.get()).as_mut_ptr().add(self.start_at);
            core::slice::from_raw_parts_mut(ptr, self.length)
        }
    }
}
