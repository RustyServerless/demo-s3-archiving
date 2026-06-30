//! Adaptive issue-rate governor for the copy-only chain (AIAD).
//!
//! ## Why this exists
//!
//! A single chain invocation issues ~22k S3 control-plane calls but, because the
//! links within each segment are serial, it only sustains ~1,500–1,800 calls/s in
//! isolation — under S3's ~3,500/s per-bucket SlowDown knee. So *solo* it never
//! throttles. The benchmark runs several copies concurrently (repeat-runs), and N
//! instances × ~1,600/s crosses the knee. An instance can't see how many siblings
//! share the bucket, so a fixed cap is wrong both ways.
//!
//! ## The approach: AIAD (additive-increase / additive-decrease) from fair share
//!
//! The earlier AIMD (multiplicative ×0.5 backoff) collapsed under sustained
//! contention: frequent 503s halved the rate faster than additive recovery could
//! climb, pinning all instances at the floor. Three instances crawling at the
//! floor used ~1/8th of the bucket's capacity → ~150s builds when the true
//! 3-wide floor is ~30-60s. The fix:
//!
//!   - **Start near the solo rate:** sequential repeats face the bucket alone,
//!     so START_RATE begins close to solo's natural issue rate. Not a low
//!     slow-start, not an optimistic ceiling — the rate one of three instances
//!     should sustain. Solo, this is below the natural ~1,830/s, so recovery
//!     climbs it up; 3-wide, it's right at the shared knee from the first call.
//!   - **Additive decrease on 503:** step DOWN by a FIXED amount (not ×0.5), at
//!     most once per DOWN_WINDOW (so a burst of concurrent 503s from one
//!     contention wave steps once, not once-per-failed-call — that windowing is
//!     what stops the collapse). Converges to just-below the sustainable rate
//!     from above, in small steps.
//!   - **Additive increase when clean:** step UP by a fixed amount per
//!     RECOVER_INTERVAL with no recent 503, capped at CEIL (solo's natural rate).
//!   - **No token drain on 503.** The old code zeroed the bucket on every 503,
//!     fully STALLING the instance until refill — a major contributor to the
//!     150s. Now a 503 only adjusts the rate; in-flight pacing continues.
//!
//! ## Two-tier priority
//!
//! Segment-link acquires (High) take precedence over stitch-copy acquires (Low):
//! a Low acquire yields while any High waiter is queued, so the overlapped stitch
//! can't starve the segment links it depends on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// Start rate: begin near solo's natural rate so there's little ramp tax.
/// Under sequential repeats each invocation is alone, so a high start is safe;
/// the additive down-step still handles any incidental 503.
const START_RATE: f64 = 1_800.0; // was KNEE / ASSUMED_INSTANCES (≈1170)
/// Ceiling: was capping solo at 1830; raise so the governor isn't a solo brake.
/// Chain is latency-bound at ~1316/s solo so it won't actually hit this — the
/// point is to stop the ceiling acting as a throttle on a run that never 503s.
const CEIL_RATE: f64 = 3_200.0; // was 1_830.0
/// Safety floor. Additive-decrease-from-fair-share should rarely approach this;
/// if it does, contention is extreme and we crawl rather than crash.
const FLOOR_RATE: f64 = 200.0;
/// Additive decrease per throttle step (fixed, NOT multiplicative).
const STEP_DOWN: f64 = 100.0;
/// At most one down-step per this window — concurrent 503s from one contention
/// wave collapse to a single step. THIS is what prevents the AIMD-style crash.
const DOWN_WINDOW: Duration = Duration::from_millis(100);
// Recovery step: climb back to ceiling faster after any incidental dip.
const STEP_UP: f64 = 150.0; // was 50.0
const RECOVER_INTERVAL: Duration = Duration::from_millis(500);
/// Token bucket burst ceiling (tokens). Tiny — smoothing only, no cold-start dump.
const MAX_BURST: f64 = 4.0;

/// Call class for two-tier priority.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Priority {
	/// Segment-link calls — the critical path. Served first.
	High,
	/// Stitch copies and other deferrable calls — yield to High.
	Low,
}

/// Shared, cheaply-cloneable handle to the governor.
#[derive(Clone)]
pub struct RateLimiter {
	inner: Arc<Inner>,
}

struct Inner {
	/// Current refill rate (tokens/s) as f64 bits for atomic CAS.
	rate_bits: AtomicU64,
	/// Available tokens, ×1000 fixed-point.
	tokens_milli: AtomicU64,
	/// Last refill instant (millis since start).
	last_refill_ms: AtomicU64,
	/// Count of high-priority waiters; a Low acquire defers while > 0.
	high_waiters: AtomicU64,
	notify: Notify,
	/// millis-since-start of the last applied down-step (for DOWN_WINDOW).
	last_down_ms: AtomicU64,
	/// millis-since-start of the last applied up-step (for RECOVER_INTERVAL).
	last_up_ms: AtomicU64,
	/// millis-since-start of the most recent 503 (recovery waits for quiet).
	last_throttle_ms: AtomicU64,
	start: Instant,

	// ---- instrumentation (diagnostics only) ----
	/// Total acquires granted (≈ total S3 calls issued through the governor).
	acquires: AtomicU64,
	/// Total throttle (503) events observed (per failed call, pre-windowing).
	throttles: AtomicU64,
	/// Total down-steps actually applied (post-windowing).
	down_steps: AtomicU64,
	/// Total up-steps applied.
	up_steps: AtomicU64,
	/// Total retry attempts across all governed calls.
	retries: AtomicU64,
	/// Min rate ever reached (×1000 fixed point), for the end-of-run summary.
	min_rate_milli: AtomicU64,
}

impl RateLimiter {
	pub fn new() -> Self {
		let now = Instant::now();
		RateLimiter {
			inner: Arc::new(Inner {
				rate_bits: AtomicU64::new(START_RATE.to_bits()),
				tokens_milli: AtomicU64::new((MAX_BURST * 1000.0) as u64),
				last_refill_ms: AtomicU64::new(0),
				high_waiters: AtomicU64::new(0),
				notify: Notify::new(),
				last_down_ms: AtomicU64::new(0),
				last_up_ms: AtomicU64::new(0),
				last_throttle_ms: AtomicU64::new(0),
				start: now,
				acquires: AtomicU64::new(0),
				throttles: AtomicU64::new(0),
				down_steps: AtomicU64::new(0),
				up_steps: AtomicU64::new(0),
				retries: AtomicU64::new(0),
				min_rate_milli: AtomicU64::new((START_RATE * 1000.0) as u64),
			}),
		}
	}

	fn now_ms(&self) -> u64 {
		self.inner.start.elapsed().as_millis() as u64
	}

	fn rate(&self) -> f64 {
		f64::from_bits(self.inner.rate_bits.load(Ordering::Relaxed))
	}

	fn set_rate(&self, r: f64) {
		let r = r.clamp(FLOOR_RATE, CEIL_RATE);
		self.inner.rate_bits.store(r.to_bits(), Ordering::Relaxed);
		// Track the minimum for the summary.
		let rm = (r * 1000.0) as u64;
		let mut cur = self.inner.min_rate_milli.load(Ordering::Relaxed);
		while rm < cur {
			match self.inner.min_rate_milli.compare_exchange_weak(
				cur,
				rm,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => cur = actual,
			}
		}
	}

	/// Refill tokens by elapsed×rate; apply an additive UP-step if we've run clean
	/// (no 503 within RECOVER_INTERVAL) since the last up-step.
	fn refill(&self) {
		let now_ms = self.now_ms();
		let last = self.inner.last_refill_ms.swap(now_ms, Ordering::Relaxed);
		if now_ms > last {
			let elapsed_s = (now_ms - last) as f64 / 1000.0;
			let add = elapsed_s * self.rate() * 1000.0;
			let cap = (MAX_BURST * 1000.0) as u64;
			let mut cur = self.inner.tokens_milli.load(Ordering::Relaxed);
			loop {
				let next = ((cur as f64 + add) as u64).min(cap);
				match self.inner.tokens_milli.compare_exchange_weak(
					cur,
					next,
					Ordering::Relaxed,
					Ordering::Relaxed,
				) {
					Ok(_) => break,
					Err(actual) => cur = actual,
				}
			}
		}

		// Additive increase: only when no 503 in the last RECOVER_INTERVAL, and at
		// most once per RECOVER_INTERVAL.
		let last_up = self.inner.last_up_ms.load(Ordering::Relaxed);
		let last_thr = self.inner.last_throttle_ms.load(Ordering::Relaxed);
		let interval_ms = RECOVER_INTERVAL.as_millis() as u64;
		if now_ms.saturating_sub(last_up) >= interval_ms
			&& now_ms.saturating_sub(last_thr) >= interval_ms
			&& self
				.inner
				.last_up_ms
				.compare_exchange(last_up, now_ms, Ordering::Relaxed, Ordering::Relaxed)
				.is_ok()
		{
			let r = self.rate();
			if r < CEIL_RATE {
				self.set_rate(r + STEP_UP);
				self.inner.up_steps.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	fn try_take(&self) -> bool {
		self.refill();
		let mut cur = self.inner.tokens_milli.load(Ordering::Relaxed);
		loop {
			if cur < 1000 {
				return false;
			}
			match self.inner.tokens_milli.compare_exchange_weak(
				cur,
				cur - 1000,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => {
					self.inner.acquires.fetch_add(1, Ordering::Relaxed);
					return true;
				}
				Err(actual) => cur = actual,
			}
		}
	}

	/// Acquire one token, awaiting if necessary. High beats Low.
	pub async fn acquire(&self, prio: Priority) {
		if prio == Priority::High {
			self.inner.high_waiters.fetch_add(1, Ordering::Relaxed);
		}
		loop {
			if prio == Priority::Low && self.inner.high_waiters.load(Ordering::Relaxed) > 0 {
				self.inner.notify.notified().await;
				continue;
			}
			if self.try_take() {
				if prio == Priority::High {
					self.inner.high_waiters.fetch_sub(1, Ordering::Relaxed);
					self.inner.notify.notify_waiters();
				}
				return;
			}
			let _ =
				tokio::time::timeout(Duration::from_millis(2), self.inner.notify.notified()).await;
		}
	}

	/// Signal a throttle (503). Records it, and applies an additive DOWN-step at
	/// most once per DOWN_WINDOW so a wave of concurrent 503s steps once, not
	/// once-per-call. NO token drain — the instance keeps issuing at the (slightly
	/// reduced) rate rather than stalling.
	pub fn on_throttle(&self) {
		let now_ms = self.now_ms();
		self.inner.throttles.fetch_add(1, Ordering::Relaxed);
		self.inner.last_throttle_ms.store(now_ms, Ordering::Relaxed);

		let last_down = self.inner.last_down_ms.load(Ordering::Relaxed);
		let window_ms = DOWN_WINDOW.as_millis() as u64;
		if now_ms.saturating_sub(last_down) >= window_ms
			&& self
				.inner
				.last_down_ms
				.compare_exchange(last_down, now_ms, Ordering::Relaxed, Ordering::Relaxed)
				.is_ok()
		{
			let r = self.rate();
			self.set_rate(r - STEP_DOWN);
			self.inner.down_steps.fetch_add(1, Ordering::Relaxed);
		}
	}

	/// Count a retry attempt (for diagnostics).
	pub fn note_retry(&self) {
		self.inner.retries.fetch_add(1, Ordering::Relaxed);
	}

	/// Snapshot of the diagnostic counters for logging at a phase boundary.
	pub fn stats(&self) -> RateStats {
		RateStats {
			rate: self.rate(),
			min_rate: self.inner.min_rate_milli.load(Ordering::Relaxed) as f64 / 1000.0,
			acquires: self.inner.acquires.load(Ordering::Relaxed),
			throttles: self.inner.throttles.load(Ordering::Relaxed),
			down_steps: self.inner.down_steps.load(Ordering::Relaxed),
			up_steps: self.inner.up_steps.load(Ordering::Relaxed),
			retries: self.inner.retries.load(Ordering::Relaxed),
		}
	}
}

/// Diagnostic snapshot — log these fields at phase boundaries to see what the
/// governor actually did (rate convergence, 503 count, retry pressure).
#[derive(Debug, Clone, Copy)]
pub struct RateStats {
	pub rate: f64,
	pub min_rate: f64,
	pub acquires: u64,
	pub throttles: u64,
	pub down_steps: u64,
	pub up_steps: u64,
	pub retries: u64,
}

impl Default for RateLimiter {
	fn default() -> Self {
		Self::new()
	}
}
