// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

//! Clocks

#[cfg(feature = "standard-clock")]
use futures_timer::Delay;
#[cfg(feature = "standard-clock")]
use std::time::Instant;
use std::{
    convert::TryInto,
    fmt::Debug,
    future::Future,
    marker::Unpin,
    mem,
    ops::{Add, Sub},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

/// A `Clock` controls the passing of time.
///
/// [`Limiter`](crate::Limiter) uses [`sleep()`](Clock::sleep()) to impose speed
/// limit, and it relies on the current and past timestamps to determine how
/// long to sleep. Both of these time-related features are encapsulated into
/// this `Clock` trait.
///
/// # Implementing
///
/// The [`StandardClock`] should be enough in most situation. However, these are
/// cases for a custom clock, e.g. use a coarse clock instead of the standard
/// high-precision clock, or use a specialized future associated with an
/// executor instead of the generic `futures-timer`.
///
/// Types implementing `Clock` must be cheap to clone (e.g. using `Arc`), and
/// the default value must be ready to use.
pub trait Clock: Clone + Default {
    /// Type to represent a point of time.
    ///
    /// Subtracting two instances should return the duration elapsed between
    /// them. The subtraction must never block or panic when they are properly
    /// ordered.
    type Instant: Copy + Sub<Output = Duration>;

    /// Future type returned by [`sleep()`](Clock::sleep()).
    type Delay: Future<Output = ()> + Unpin;

    /// Returns the current time instant. It should be monotonically increasing,
    /// but not necessarily high-precision or steady.
    ///
    /// This function must never block or panic.
    fn now(&self) -> Self::Instant;

    /// Asynchronously sleeps the current task for the given duration.
    ///
    /// This method should return a future which fulfilled as `()` after a
    /// duration of `dur`. It should *not* block the current thread.
    ///
    /// `sleep()` is often called with a duration of 0 s. This should result in
    /// a future being resolved immediately.
    fn sleep(&self, dur: Duration) -> Self::Delay;
}

/// A `BlockingClock` is a [`Clock`] which supports synchronous sleeping.
pub trait BlockingClock: Clock {
    /// Sleeps and blocks the current thread for the given duration.
    fn blocking_sleep(&self, dur: Duration);
}

/// The physical clock using [`std::time::Instant`].
///
/// The sleeping future is based on [`futures-timer`]. Blocking sleep uses
/// [`std::thread::sleep()`].
///
/// [`futures-timer`]: https://docs.rs/futures-timer/
#[cfg(feature = "standard-clock")]
#[derive(Copy, Clone, Debug, Default)]
pub struct StandardClock;

#[cfg(feature = "standard-clock")]
impl Clock for StandardClock {
    type Instant = Instant;
    type Delay = Delay;

    fn now(&self) -> Self::Instant {
        Instant::now()
    }

    fn sleep(&self, dur: Duration) -> Self::Delay {
        Delay::new(dur)
    }
}

#[cfg(feature = "standard-clock")]
impl BlockingClock for StandardClock {
    fn blocking_sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// Number of nanoseconds since an arbitrary epoch.
///
/// This is the instant type of [`ManualClock`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nanoseconds(pub u64);

impl Sub for Nanoseconds {
    type Output = Duration;
    fn sub(self, other: Self) -> Duration {
        Duration::from_nanos(self.0 - other.0)
    }
}

impl Add<Duration> for Nanoseconds {
    type Output = Self;
    fn add(self, other: Duration) -> Self {
        let dur: u64 = other
            .as_nanos()
            .try_into()
            .expect("cannot increase more than 2^64 ns");
        Self(self.0 + dur)
    }
}

/// The future returned by [`ManualClock`]`::sleep()`.
#[derive(Debug)]
pub struct ManualDelay {
    clock: Arc<ManualClockContent>,
    timeout: Nanoseconds,
}

impl Future for ManualDelay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = self.clock.now();
        if now >= self.timeout {
            Poll::Ready(())
        } else {
            self.clock.register(cx);
            Poll::Pending
        }
    }
}

/// Internal, shared part of [`ManualClock`]. `ManualClock` itself is an `Arc`
/// of `ManualClockContent`.
#[derive(Default, Debug)]
struct ManualClockContent {
    now: AtomicU64,
    wakers: Mutex<Vec<Waker>>,
}

impl ManualClockContent {
    fn now(&self) -> Nanoseconds {
        Nanoseconds(self.now.load(Ordering::SeqCst))
    }

    fn set_time(&self, time: u64) {
        let old_time = self.now.swap(time, Ordering::SeqCst);
        assert!(old_time <= time, "cannot move the time backwards");

        let wakers = { mem::take(&mut *self.wakers.lock().unwrap()) };
        wakers.into_iter().for_each(Waker::wake);
    }

    fn register(&self, cx: &mut Context<'_>) {
        self.wakers.lock().unwrap().push(cx.waker().clone());
    }
}

/// A [`Clock`] where the passage of time can be manually controlled.
///
/// This type is mainly used for testing behavior of speed limiter only.
///
/// This clock only supports up to 2<sup>64</sup> ns (about 584.5 years).
///
/// # Examples
///
/// ```rust
/// use async_speed_limit::clock::{Clock, ManualClock, Nanoseconds};
///
/// let clock = ManualClock::new();
/// assert_eq!(clock.now(), Nanoseconds(0));
/// clock.set_time(Nanoseconds(1_000_000_000));
/// assert_eq!(clock.now(), Nanoseconds(1_000_000_000));
/// ```
#[derive(Default, Debug, Clone)]
pub struct ManualClock(Arc<ManualClockContent>);

impl ManualClock {
    /// Creates a new clock with time set to 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the current time of this clock to the given value.
    ///
    /// # Panics
    ///
    /// Since [`now()`](Clock::now()) must be monotonically increasing, if the
    /// new time is less than the previous time, this function will panic.
    pub fn set_time(&self, time: Nanoseconds) {
        self.0.set_time(time.0);
    }
}

impl Clock for ManualClock {
    type Instant = Nanoseconds;
    type Delay = ManualDelay;

    fn now(&self) -> Self::Instant {
        self.0.now()
    }

    fn sleep(&self, dur: Duration) -> Self::Delay {
        ManualDelay {
            timeout: self.0.now() + dur,
            clock: self.0.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_executor::LocalPool;
    use futures_util::task::SpawnExt;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn manual_clock_basics() {
        let clock = ManualClock::new();
        let t1 = clock.now();
        assert_eq!(t1, Nanoseconds(0));

        clock.set_time(Nanoseconds(1_000_000_000));

        let t2 = clock.now();
        assert_eq!(t2, Nanoseconds(1_000_000_000));
        assert_eq!(t2 - t1, Duration::from_secs(1));

        clock.clone().set_time(Nanoseconds(1_000_000_007));

        let t3 = clock.now();
        assert_eq!(t3, Nanoseconds(1_000_000_007));
        assert_eq!(t3 - t2, Duration::from_nanos(7));
    }

    #[test]
    fn manual_clock_sleep() {
        let counter = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();
        let mut pool = LocalPool::new();
        let sp = pool.spawner();

        // expected sequence:
        //
        //   t=0    .     .
        //   t=1    .     .
        //   t=2    +1    .
        //   t=3    .     +16
        //   t=4    .     +64
        //   t=5    +4    .

        sp.spawn({
            let counter = counter.clone();
            let clock = clock.clone();
            async move {
                clock.sleep(Duration::from_secs(2)).await;
                counter.fetch_add(1, Ordering::Relaxed);
                clock.sleep(Duration::from_secs(3)).await;
                counter.fetch_add(4, Ordering::Relaxed);
            }
        })
        .unwrap();

        sp.spawn({
            let counter = counter.clone();
            let clock = clock.clone();
            async move {
                clock.sleep(Duration::from_secs(3)).await;
                counter.fetch_add(16, Ordering::Relaxed);
                clock.sleep(Duration::from_secs(1)).await;
                counter.fetch_add(64, Ordering::Relaxed);
            }
        })
        .unwrap();

        clock.set_time(Nanoseconds(0));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        clock.set_time(Nanoseconds(1_000_000_000));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        clock.set_time(Nanoseconds(2_000_000_000));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        clock.set_time(Nanoseconds(3_000_000_000));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 17);

        clock.set_time(Nanoseconds(4_000_000_000));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 81);

        clock.set_time(Nanoseconds(5_000_000_000));
        pool.run_until_stalled();
        assert_eq!(counter.load(Ordering::Relaxed), 85);

        // all futures should be exhausted.
        assert!(!pool.try_run_one());
    }

    #[test]
    #[cfg(feature = "standard-clock")]
    fn standard_clock() {
        let res = futures_executor::block_on(async {
            let init = StandardClock.now();
            StandardClock.sleep(Duration::from_secs(1)).await;
            StandardClock.now() - init
        });
        assert!(
            Duration::from_millis(900) <= res && res <= Duration::from_millis(1100),
            "standard clock slept too long at {:?}",
            res
        )
    }
}
