// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

//! Speed limiter

#[cfg(feature = "standard-clock")]
use crate::clock::StandardClock;
use crate::clock::{BlockingClock, Clock};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    mem,
    ops::Sub,
    pin::Pin,
    sync::Arc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

/// Stores the current state of the limiter.
#[derive(Debug, Clone, Copy)]
struct Bucket<I> {
    /// Last updated instant of the bucket. This is used to compare with the
    /// current instant to deduce the current bucket value.
    last_updated: I,
    /// The speed limit (unit: B/s).
    speed_limit: f64,
    /// Time needed to refill the entire bucket (unit: s).
    refill: f64,
    /// The number of bytes the bucket is carrying at the time `last_updated`.
    /// This value can be negative.
    value: f64,
}

impl<I> Bucket<I> {
    /// Returns the maximum number of bytes this bucket can carry.
    fn capacity(&self) -> f64 {
        self.speed_limit * self.refill
    }

    /// Consumes the given number of bytes from the bucket.
    ///
    /// Returns the duration we need for the consumed bytes to recover.
    ///
    /// This method should only be called when the speed is finite.
    fn consume(&mut self, size: f64) -> Duration {
        self.value -= size;
        if self.value > 0.0 {
            Duration::from_secs(0)
        } else {
            let sleep_secs = self.refill - self.value / self.speed_limit;
            Duration::from_secs_f64(sleep_secs)
        }
    }

    /// Changes the speed limit.
    ///
    /// The current value will be raised or lowered so that the number of
    /// consumed bytes remains constant.
    fn set_speed_limit(&mut self, new_speed_limit: f64) {
        let old_capacity = self.capacity();
        self.speed_limit = new_speed_limit;
        if new_speed_limit.is_finite() {
            let new_capacity = self.capacity();
            if old_capacity.is_finite() {
                self.value += new_capacity - old_capacity;
            } else {
                self.value = new_capacity;
            }
        }
    }
}

impl<I: Copy + Sub<Output = Duration>> Bucket<I> {
    /// Refills the bucket to match the value at current time.
    ///
    /// This method should only be called when the speed is finite.
    fn refill(&mut self, now: I) {
        let elapsed = (now - self.last_updated).as_secs_f64();
        let refilled = self.speed_limit * elapsed;
        self.value = self.capacity().min(self.value + refilled);
        self.last_updated = now;
    }
}

/// Builder for [`Limiter`].
///
/// # Examples
///
#[cfg_attr(feature = "standard-clock", doc = "```rust")]
#[cfg_attr(not(feature = "standard-clock"), doc = "```ignore")]
/// use async_speed_limit::Limiter;
/// use std::time::Duration;
///
/// let limiter = <Limiter>::builder(1_048_576.0)
///     .refill(Duration::from_millis(100))
///     .build();
/// # drop(limiter);
/// ```
#[derive(Debug)]
pub struct Builder<C: Clock> {
    clock: C,
    bucket: Bucket<C::Instant>,
}

impl<C: Clock> Builder<C> {
    /// Creates a new limiter builder.
    ///
    /// Use [infinity](`std::f64::INFINITY`) to make the speed unlimited.
    pub fn new(speed_limit: f64) -> Self {
        let clock = C::default();
        let mut result = Self {
            bucket: Bucket {
                last_updated: clock.now(),
                speed_limit: 0.0,
                refill: 0.1,
                value: 0.0,
            },
            clock,
        };
        result.speed_limit(speed_limit);
        result
    }

    /// Sets the speed limit of the limiter.
    ///
    /// Use [infinity](`std::f64::INFINITY`) to make the speed unlimited.
    ///
    /// # Panics
    ///
    /// The speed limit must be positive. Panics if the speed limit is negative,
    /// zero, or NaN.
    pub fn speed_limit(&mut self, speed_limit: f64) -> &mut Self {
        assert!(speed_limit > 0.0, "speed limit must be positive");
        self.bucket.speed_limit = speed_limit;
        self
    }

    /// Sets the refill period of the limiter.
    ///
    /// The default value is 0.1 s, which should be good for most use cases. The
    /// refill period is ignored if the speed is [infinity](`std::f64::INFINITY`).
    ///
    /// # Panics
    ///
    /// The duration must not be zero, otherwise this method panics.
    pub fn refill(&mut self, dur: Duration) -> &mut Self {
        assert!(
            dur > Duration::from_secs(0),
            "refill duration must not be zero"
        );
        self.bucket.refill = dur.as_secs_f64();
        self
    }

    /// Sets the clock instance used by the limiter.
    pub fn clock(&mut self, clock: C) -> &mut Self {
        self.clock = clock;
        self
    }

    /// Builds the limiter.
    pub fn build(&mut self) -> Limiter<C> {
        self.bucket.value = self.bucket.capacity();
        self.bucket.last_updated = self.clock.now();
        Limiter {
            bucket: Arc::new(Mutex::new(self.bucket)),
            clock: mem::take(&mut self.clock),
            total_bytes_consumed: Arc::new(AtomicUsize::new(0)),
        }
    }
}

macro_rules! declare_limiter {
    ($($default_clock:tt)*) => {
        /// A type to control the maximum speed limit of multiple streams.
        ///
        /// When a `Limiter` is cloned, the instances would share the same
        /// queue. Multiple tasks can cooperatively respect a global speed limit
        /// via clones. Cloning a `Limiter` is cheap (equals to cloning two
        /// `Arc`s).
        ///
        /// The speed limit is imposed by awaiting
        /// [`consume()`](Limiter::consume()). The method returns a future which
        /// sleeps until rate falls below the limit.
        ///
        /// # Examples
        ///
        /// Upload some small files atomically in parallel, while maintaining a
        /// global speed limit of 1 MiB/s.
        ///
        #[cfg_attr(feature = "standard-clock", doc = "```rust")]
        #[cfg_attr(not(feature = "standard-clock"), doc = "```ignore")]
        /// use async_speed_limit::Limiter;
        /// use futures_util::future::try_join_all;
        ///
        /// # async {
        /// # let files = &[""];
        /// # async fn upload(file: &str) -> Result<(), ()> { Ok(()) }
        /// let limiter = <Limiter>::new(1_048_576.0);
        /// let processes = files
        ///     .iter()
        ///     .map(|file| {
        ///         let limiter = limiter.clone();
        ///         async move {
        ///             limiter.consume(file.len()).await;
        ///             upload(file).await?;
        ///             Ok(())
        ///         }
        ///     });
        /// try_join_all(processes).await?;
        /// # Ok::<_, ()>(()) };
        /// ```
        #[derive(Debug, Clone)]
        pub struct Limiter<C: Clock $($default_clock)*> {
            /// State of the limiter.
            // TODO avoid using Arc<Mutex>?
            bucket: Arc<Mutex<Bucket<C::Instant>>>,
            /// Clock used for time calculation.
            clock: C,
            /// Statistics of the number of bytes consumed for record. When this
            /// number reaches `usize::MAX` it will wrap around.
            total_bytes_consumed: Arc<AtomicUsize>,
        }
    }
}

#[cfg(feature = "standard-clock")]
declare_limiter! { = StandardClock }

#[cfg(not(feature = "standard-clock"))]
declare_limiter! {}

impl<C: Clock> Limiter<C> {
    /// Creates a new speed limiter.
    ///
    /// Use [infinity](`std::f64::INFINITY`) to make the speed unlimited.
    pub fn new(speed_limit: f64) -> Self {
        Builder::new(speed_limit).build()
    }

    /// Makes a [`Builder`] for further configurating this limiter.
    ///
    /// Use [infinity](`std::f64::INFINITY`) to make the speed unlimited.
    pub fn builder(speed_limit: f64) -> Builder<C> {
        Builder::new(speed_limit)
    }

    /// Returns the clock associated with this limiter.
    pub fn clock(&self) -> &C {
        &self.clock
    }

    /// Dynamically changes the speed limit. The new limit applies to all clones
    /// of this instance.
    ///
    /// Use [infinity](`std::f64::INFINITY`) to make the speed unlimited.
    ///
    /// This change will not affect any tasks scheduled _before_ this call.
    pub fn set_speed_limit(&self, speed_limit: f64) {
        debug_assert!(speed_limit > 0.0, "speed limit must be positive");
        self.bucket.lock().unwrap().set_speed_limit(speed_limit);
    }

    /// Returns the current speed limit.
    ///
    /// This method returns [infinity](`std::f64::INFINITY`) if the speed is
    /// unlimited.
    pub fn speed_limit(&self) -> f64 {
        self.bucket.lock().unwrap().speed_limit
    }

    /// Obtains the total number of bytes consumed by this limiter so far.
    ///
    /// If more than `usize::MAX` bytes have been consumed, the count will wrap
    /// around.
    pub fn total_bytes_consumed(&self) -> usize {
        self.total_bytes_consumed.load(Ordering::Relaxed)
    }

    /// Resets the total number of bytes consumed to 0.
    pub fn reset_statistics(&self) {
        self.total_bytes_consumed.store(0, Ordering::Relaxed);
    }

    /// Consumes several bytes from the speed limiter, returns the duration
    /// needed to sleep to maintain the speed limit.
    fn consume_duration(&self, byte_size: usize) -> Duration {
        self.total_bytes_consumed
            .fetch_add(byte_size, Ordering::Relaxed);

        #[allow(clippy::cast_precision_loss)]
        let size = byte_size as f64;

        // Using a lock should be fine,
        // as we're not blocking for a long time.
        let mut bucket = self.bucket.lock().unwrap();
        if bucket.speed_limit.is_finite() {
            bucket.refill(self.clock.now());
            bucket.consume(size)
        } else {
            Duration::from_secs(0)
        }
    }

    /// Consumes several bytes from the speed limiter.
    ///
    /// The consumption happens at the beginning, *before* the speed limit is
    /// applied. The returned future is fulfilled after the speed limit is
    /// satified.
    pub fn consume(&self, byte_size: usize) -> Consume<C, ()> {
        let sleep_dur = self.consume_duration(byte_size);
        Consume {
            future: self.clock.sleep(sleep_dur),
            result: Some(()),
        }
    }

    /// Wraps a streaming resource with speed limiting. See documentation of
    /// [`Resource`] for details.
    ///
    /// If you want to reuse the limiter after calling this function, `clone()`
    /// the limiter first.
    pub fn limit<R>(self, resource: R) -> Resource<R, C> {
        Resource::new(self, resource)
    }

    /// Returns the number of active clones of this limiter.
    ///
    /// Currently only used for testing, and thus not exported.
    #[cfg(test)]
    fn shared_count(&self) -> usize {
        Arc::strong_count(&self.bucket)
    }
}

impl<C: BlockingClock> Limiter<C> {
    /// Consumes several bytes, and sleeps the current thread to maintain the
    /// speed limit.
    ///
    /// The consumption happens at the beginning, *before* the speed limit is
    /// applied. This method blocks the current thread (e.g. using
    /// [`std::thread::sleep()`] given a [`StandardClock`]), and *must not* be
    /// used in `async` context.
    ///
    /// Prefer using this method instead of
    /// [`futures_executor::block_on`]`(limiter.`[`consume`](Limiter::consume())`(size))`.
    ///
    /// [`futures_executor::block_on`]: https://docs.rs/futures-executor/0.3/futures_executor/fn.block_on.html
    pub fn blocking_consume(&self, byte_size: usize) {
        let sleep_dur = self.consume_duration(byte_size);
        self.clock.blocking_sleep(sleep_dur);
    }
}

/// The future returned by [`Limiter::consume()`].
#[derive(Debug)]
pub struct Consume<C: Clock, R> {
    future: C::Delay,
    result: Option<R>,
}

#[allow(clippy::use_self)] // https://github.com/rust-lang/rust-clippy/issues/3410
impl<C: Clock, R> Consume<C, R> {
    /// Replaces the return value of the future.
    pub fn map<T, F: FnOnce(R) -> T>(self, f: F) -> Consume<C, T> {
        Consume {
            future: self.future,
            result: self.result.map(f),
        }
    }
}

impl<C: Clock, R: Unpin> Future for Consume<C, R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if Pin::new(&mut this.future).poll(cx).is_ready() {
            if let Some(value) = this.result.take() {
                return Poll::Ready(value);
            }
        }
        Poll::Pending
    }
}

#[cfg(feature = "fused-future")]
impl<C: Clock, R: Unpin> futures_core::future::FusedFuture for Consume<C, R> {
    fn is_terminated(&self) -> bool {
        self.result.is_none()
    }
}

pin_project! {
    /// A speed-limited wrapper of a byte stream.
    ///
    /// The `Resource` can be used to limit speed of
    ///
    /// * [`AsyncRead`](futures_io::AsyncRead)
    /// * [`AsyncWrite`](futures_io::AsyncWrite)
    ///
    /// Just like [`Limiter`], the delay is inserted *after* the data are sent
    /// or received, in which we know the exact amount of bytes transferred to
    /// give an accurate delay. The instantaneous speed can exceed the limit if
    /// many read/write tasks are started simultaneously. Therefore, restricting
    /// the concurrency is also important to avoid breaching the constraint.
    pub struct Resource<R, C: Clock> {
        limiter: Limiter<C>,
        #[pin]
        resource: R,
        waiter: Option<Consume<C, ()>>,
    }
}

impl<R, C: Clock> Resource<R, C> {
    /// Creates a new speed-limited resource.
    ///
    /// To make the resouce have unlimited speed, set the speed of [`Limiter`]
    /// to [infinity](`std::f64::INFINITY`).
    pub fn new(limiter: Limiter<C>, resource: R) -> Self {
        Self {
            limiter,
            resource,
            waiter: None,
        }
    }

    /// Unwraps this value, returns the underlying resource.
    pub fn into_inner(self) -> R {
        self.resource
    }

    /// Gets a reference to the underlying resource.
    ///
    /// It is inadvisable to directly operate the underlying resource.
    pub fn get_ref(&self) -> &R {
        &self.resource
    }

    /// Gets a mutable reference to the underlying resource.
    ///
    /// It is inadvisable to directly operate the underlying resource.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.resource
    }

    /// Gets a pinned reference to the underlying resource.
    ///
    /// It is inadvisable to directly operate the underlying resource.
    pub fn get_pin_ref(self: Pin<&Self>) -> Pin<&R> {
        self.project_ref().resource
    }

    /// Gets a pinned mutable reference to the underlying resource.
    ///
    /// It is inadvisable to directly operate the underlying resource.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut R> {
        self.project().resource
    }
}

impl<R, C: Clock> Resource<R, C> {
    /// Wraps a poll function with a delay after it.
    ///
    /// This method calls the given `poll` function until it is fulfilled. After
    /// that, the result is saved into this `Resource` instance (therefore
    /// different `poll_***` calls should not be interleaving), while returning
    /// `Pending` until the limiter has completely consumed the result.
    #[allow(dead_code)]
    pub(crate) fn poll_limited<T>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        length: impl FnOnce(&T) -> usize,
        poll: impl FnOnce(Pin<&mut R>, &mut Context<'_>) -> Poll<T>,
    ) -> Poll<T> {
        let this = self.project();

        if let Some(waiter) = this.waiter {
            let res = Pin::new(waiter).poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
            *this.waiter = None;
        }

        let res = poll(this.resource, cx);
        if let Poll::Ready(obj) = &res {
            let len = length(obj);
            if len > 0 {
                *this.waiter = Some(this.limiter.consume(len));
            }
        }
        res
    }
}

//------------------------------------------------------------------------------

#[cfg(test)]
mod tests_with_manual_clock {
    use super::*;
    use crate::clock::{Clock, ManualClock, Nanoseconds};
    use futures_executor::LocalPool;
    use futures_util::task::SpawnExt;
    use std::{future::Future, thread::panicking};

    /// Part of the `Fixture` which is to be shared with the spawned tasks.
    #[derive(Clone)]
    struct SharedFixture {
        limiter: Limiter<ManualClock>,
    }

    impl SharedFixture {
        fn now(&self) -> u64 {
            self.limiter.clock().now().0
        }

        fn sleep(&self, nanos: u64) -> impl Future<Output = ()> + '_ {
            self.limiter.clock().sleep(Duration::from_nanos(nanos))
        }

        fn consume(&self, bytes: usize) -> impl Future<Output = ()> + '_ {
            self.limiter.consume(bytes)
        }
    }

    /// The test fixture used by all test cases.
    struct Fixture {
        shared: SharedFixture,
        pool: LocalPool,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                shared: SharedFixture {
                    limiter: Limiter::builder(512.0)
                        .refill(Duration::from_secs(1))
                        .build(),
                },
                pool: LocalPool::new(),
            }
        }

        fn spawn<F, G>(&self, f: F)
        where
            F: FnOnce(SharedFixture) -> G,
            G: Future<Output = ()> + Send + 'static,
        {
            self.pool.spawner().spawn(f(self.shared.clone())).unwrap();
        }

        fn set_time(&mut self, time: u64) {
            self.shared.limiter.clock().set_time(Nanoseconds(time));
            self.pool.run_until_stalled();
        }

        fn set_speed_limit(&self, limit: f64) {
            self.shared.limiter.set_speed_limit(limit);
        }

        fn total_bytes_consumed(&self) -> usize {
            self.shared.limiter.total_bytes_consumed()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if !panicking() {
                // the count is 1 only if all spawned futures are finished.
                assert_eq!(self.shared.limiter.shared_count(), 1);
            }
        }
    }

    #[test]
    fn under_limit_single_thread() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            sfx.consume(50).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(51).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(52).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(53).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(54).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(55).await;
            assert_eq!(sfx.now(), 0);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 315);
    }

    #[test]
    fn over_limit_single_thread() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| {
            async move {
                sfx.consume(200).await;
                assert_eq!(sfx.now(), 0);
                sfx.consume(201).await;
                assert_eq!(sfx.now(), 0);
                sfx.consume(202).await;
                assert_eq!(sfx.now(), 1_177_734_375);
                // 1_177_734_375 ns = (200+201+202)/512 seconds

                sfx.consume(203).await;
                assert_eq!(sfx.now(), 1_177_734_375);
                sfx.consume(204).await;
                assert_eq!(sfx.now(), 1_177_734_375);
                sfx.consume(205).await;
                assert_eq!(sfx.now(), 2_373_046_875);
            }
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 603);
        fx.set_time(1_177_734_374);
        assert_eq!(fx.total_bytes_consumed(), 603);
        fx.set_time(1_177_734_375);
        assert_eq!(fx.total_bytes_consumed(), 1215);
        fx.set_time(2_373_046_874);
        assert_eq!(fx.total_bytes_consumed(), 1215);
        fx.set_time(2_373_046_875);
        assert_eq!(fx.total_bytes_consumed(), 1215);
    }

    #[test]
    fn over_limit_multi_thread() {
        let mut fx = Fixture::new();

        // Due to how LocalPool does scheduling, the first task is always polled
        // before the second task. Nevertheless, the second task can still send
        // stuff using the timing difference.

        fx.spawn(|sfx| async move {
            sfx.consume(200).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(202).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(204).await;
            assert_eq!(sfx.now(), 1_183_593_750);
            sfx.consume(206).await;
            assert_eq!(sfx.now(), 1_183_593_750);
            sfx.consume(208).await;
            assert_eq!(sfx.now(), 2_384_765_625);
        });
        fx.spawn(|sfx| async move {
            sfx.consume(201).await;
            assert_eq!(sfx.now(), 1_576_171_875);
            sfx.consume(203).await;
            assert_eq!(sfx.now(), 2_781_250_000);
            sfx.consume(205).await;
            assert_eq!(sfx.now(), 2_781_250_000);
            sfx.consume(207).await;
            assert_eq!(sfx.now(), 2_781_250_000);
            sfx.consume(209).await;
            assert_eq!(sfx.now(), 3_994_140_625);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 807);
        fx.set_time(1_183_593_749);
        assert_eq!(fx.total_bytes_consumed(), 807);
        fx.set_time(1_183_593_750);
        assert_eq!(fx.total_bytes_consumed(), 1221);
        fx.set_time(1_576_171_874);
        assert_eq!(fx.total_bytes_consumed(), 1221);
        fx.set_time(1_576_171_875);
        assert_eq!(fx.total_bytes_consumed(), 1424);
        fx.set_time(2_384_765_624);
        assert_eq!(fx.total_bytes_consumed(), 1424);
        fx.set_time(2_384_765_625);
        assert_eq!(fx.total_bytes_consumed(), 1424);
        fx.set_time(2_781_249_999);
        assert_eq!(fx.total_bytes_consumed(), 1424);
        fx.set_time(2_781_250_000);
        assert_eq!(fx.total_bytes_consumed(), 2045);
        fx.set_time(3_994_140_624);
        assert_eq!(fx.total_bytes_consumed(), 2045);
        fx.set_time(3_994_140_625);
        assert_eq!(fx.total_bytes_consumed(), 2045);
    }

    #[test]
    fn over_limit_multi_thread_2() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            sfx.consume(300).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(301).await;
            assert_eq!(sfx.now(), 1_173_828_125);
            sfx.consume(302).await;
            assert_eq!(sfx.now(), 1_173_828_125);
            sfx.consume(303).await;
            assert_eq!(sfx.now(), 2_550_781_250);
            sfx.consume(304).await;
            assert_eq!(sfx.now(), 2_550_781_250);
        });
        fx.spawn(|sfx| async move {
            sfx.consume(100).await;
            assert_eq!(sfx.now(), 1_369_140_625);
            sfx.consume(101).await;
            assert_eq!(sfx.now(), 2_748_046_875);
            sfx.consume(102).await;
            assert_eq!(sfx.now(), 2_748_046_875);
            sfx.consume(103).await;
            assert_eq!(sfx.now(), 2_748_046_875);
            sfx.consume(104).await;
            assert_eq!(sfx.now(), 3_945_312_500);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 701);
        fx.set_time(1_173_828_125);
        assert_eq!(fx.total_bytes_consumed(), 1306);
        fx.set_time(1_369_140_625);
        assert_eq!(fx.total_bytes_consumed(), 1407);
        fx.set_time(2_550_781_250);
        assert_eq!(fx.total_bytes_consumed(), 1711);
        fx.set_time(2_748_046_875);
        assert_eq!(fx.total_bytes_consumed(), 2020);
        fx.set_time(3_945_312_500);
        assert_eq!(fx.total_bytes_consumed(), 2020);
    }

    #[test]
    fn over_limit_multi_thread_yielded() {
        let mut fx = Fixture::new();

        // we're adding 1ns sleeps between each consume() to act as yield points,
        // so the consume() are evenly distributed, and can take advantage of
        // single bursting.

        fx.spawn(|sfx| async move {
            sfx.consume(300).await;
            assert_eq!(sfx.now(), 0);
            sfx.sleep(1).await;
            sfx.consume(301).await;
            assert_eq!(sfx.now(), 1_369_140_625);
            sfx.sleep(1).await;
            sfx.consume(302).await;
            assert_eq!(sfx.now(), 1_369_140_626);
            sfx.sleep(1).await;
            sfx.consume(303).await;
            assert_eq!(sfx.now(), 2_748_046_875);
            sfx.sleep(1).await;
            sfx.consume(304).await;
            assert_eq!(sfx.now(), 2_748_046_876);
        });
        fx.spawn(|sfx| async move {
            sfx.consume(100).await;
            assert_eq!(sfx.now(), 0);
            sfx.sleep(1).await;
            sfx.consume(101).await;
            assert_eq!(sfx.now(), 1_566_406_250);
            sfx.sleep(1).await;
            sfx.consume(102).await;
            assert_eq!(sfx.now(), 2_947_265_625);
            sfx.sleep(1).await;
            sfx.consume(103).await;
            assert_eq!(sfx.now(), 2_947_265_626);
            sfx.sleep(1).await;
            sfx.consume(104).await;
            assert_eq!(sfx.now(), 2_947_265_627);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 400);
        fx.set_time(1);
        assert_eq!(fx.total_bytes_consumed(), 802);
        fx.set_time(1_369_140_625);
        assert_eq!(fx.total_bytes_consumed(), 802);
        fx.set_time(1_369_140_626);
        assert_eq!(fx.total_bytes_consumed(), 1104);
        fx.set_time(1_566_406_250);
        assert_eq!(fx.total_bytes_consumed(), 1407);
        fx.set_time(1_566_406_251);
        assert_eq!(fx.total_bytes_consumed(), 1509);
        fx.set_time(2_748_046_875);
        assert_eq!(fx.total_bytes_consumed(), 1509);
        fx.set_time(2_748_046_876);
        assert_eq!(fx.total_bytes_consumed(), 1813);
        fx.set_time(2_947_265_625);
        assert_eq!(fx.total_bytes_consumed(), 1813);
        fx.set_time(2_947_265_626);
        assert_eq!(fx.total_bytes_consumed(), 1916);
        fx.set_time(2_947_265_627);
        assert_eq!(fx.total_bytes_consumed(), 2020);
    }

    /// Ensures the speed limiter won't forget to enforce until a long pause
    /// i.e. we're observing the _maximum_ speed, not the _average_ speed.
    #[test]
    fn hiatus() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            sfx.consume(400).await;
            assert_eq!(sfx.now(), 0);
            sfx.consume(401).await;
            assert_eq!(sfx.now(), 1_564_453_125);

            sfx.sleep(10_000_000_000).await;
            assert_eq!(sfx.now(), 11_564_453_125);

            sfx.consume(402).await;
            assert_eq!(sfx.now(), 11_564_453_125);
            sfx.consume(403).await;
            assert_eq!(sfx.now(), 13_136_718_750);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 801);
        fx.set_time(1_564_453_125);
        assert_eq!(fx.total_bytes_consumed(), 801);
        fx.set_time(11_564_453_125);
        assert_eq!(fx.total_bytes_consumed(), 1606);
        fx.set_time(13_136_718_750);
        assert_eq!(fx.total_bytes_consumed(), 1606);
    }

    // Ensures we could still send something much higher than the speed limit
    #[test]
    fn burst() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            sfx.consume(5000).await;
            assert_eq!(sfx.now(), 9_765_625_000);
            sfx.consume(5001).await;
            assert_eq!(sfx.now(), 19_533_203_125);
            sfx.consume(5002).await;
            assert_eq!(sfx.now(), 29_302_734_375);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 5000);
        fx.set_time(9_765_625_000);
        assert_eq!(fx.total_bytes_consumed(), 10001);
        fx.set_time(19_533_203_125);
        assert_eq!(fx.total_bytes_consumed(), 15003);
        fx.set_time(29_302_734_375);
        assert_eq!(fx.total_bytes_consumed(), 15003);
    }

    #[test]
    fn change_speed_limit() {
        let mut fx = Fixture::new();

        // we try to send 5120 bytes at granularity of 256 bytes each.
        fx.spawn(|sfx| async move {
            for _ in 0..20 {
                sfx.consume(256).await;
            }
        });

        // at first, we will send 512 B/s.
        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 512);
        fx.set_time(500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 512);
        fx.set_time(1_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1024);
        fx.set_time(1_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1024);

        // decrease the speed to 256 B/s
        fx.set_speed_limit(256.0);
        fx.set_time(1_500_000_001);
        assert_eq!(fx.total_bytes_consumed(), 1024);

        fx.set_time(2_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1280);
        fx.set_time(2_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1280);
        fx.set_time(3_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1280);
        fx.set_time(3_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1280);
        fx.set_time(4_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1536);
        fx.set_time(4_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1536);

        // increase the speed to 1024 B/s
        fx.set_speed_limit(1024.0);
        fx.set_time(4_500_000_001);
        assert_eq!(fx.total_bytes_consumed(), 1536);

        fx.set_time(5_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 2560);
        fx.set_time(5_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 2560);
        fx.set_time(6_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 3584);
        fx.set_time(6_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 3584);
        fx.set_time(7_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 4608);
        fx.set_time(7_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 4608);
        fx.set_time(8_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 5120);
    }

    /// Ensures lots of small takes won't prevent scheduling of a large take.
    #[test]
    fn thousand_cuts() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            for _ in 0..64 {
                sfx.consume(16).await;
            }
        });

        fx.spawn(|sfx| async move {
            sfx.consume(555).await;
            assert_eq!(sfx.now(), 2_083_984_375);
            sfx.consume(556).await;
            assert_eq!(sfx.now(), 3_201_171_875);
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 1067);
        fx.set_time(1_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1083);
        fx.set_time(2_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1083);
        fx.set_time(2_083_984_375);
        assert_eq!(fx.total_bytes_consumed(), 1639);
        fx.set_time(3_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 2055);
        fx.set_time(3_201_171_875);
        assert_eq!(fx.total_bytes_consumed(), 2055);
        fx.set_time(4_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 2055);
        fx.set_time(4_169_921_875);
        assert_eq!(fx.total_bytes_consumed(), 2135);
    }

    #[test]
    fn set_infinite_speed_limit() {
        let mut fx = Fixture::new();

        fx.spawn(|sfx| async move {
            for _ in 0..1000 {
                sfx.consume(512).await;
            }
            sfx.sleep(1).await;
            for _ in 0..1000 {
                sfx.consume(512).await;
            }
            sfx.sleep(1).await;
            sfx.consume(512).await;
            sfx.consume(512).await;
        });

        fx.set_time(0);
        assert_eq!(fx.total_bytes_consumed(), 512);
        fx.set_time(1_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1024);

        // change speed limit to infinity...
        fx.set_speed_limit(std::f64::INFINITY);

        // should not affect tasks still waiting
        fx.set_time(1_500_000_000);
        assert_eq!(fx.total_bytes_consumed(), 1024);

        // but all future consumptions will be unlimited.
        fx.set_time(2_000_000_000);
        assert_eq!(fx.total_bytes_consumed(), 512_000);

        // should act normal for keeping speed limit at infinity.
        fx.set_speed_limit(std::f64::INFINITY);
        fx.set_time(2_000_000_001);
        assert_eq!(fx.total_bytes_consumed(), 1_024_000);

        // reducing speed limit to normal.
        fx.set_speed_limit(512.0);
        fx.set_time(2_000_000_002);
        assert_eq!(fx.total_bytes_consumed(), 1_024_512);
        fx.set_time(3_000_000_002);
        assert_eq!(fx.total_bytes_consumed(), 1_025_024);
        fx.set_time(4_000_000_002);
        assert_eq!(fx.total_bytes_consumed(), 1_025_024);
    }
}

#[cfg(test)]
#[cfg(feature = "standard-clock")]
mod tests_with_standard_clock {
    use super::*;
    use futures_executor::LocalPool;
    use futures_util::{future::join_all, task::SpawnExt};
    use rand::{thread_rng, Rng};
    use std::time::Instant;

    // This test case is ported from RocksDB.
    #[test]
    fn rate() {
        eprintln!("tests_with_standard_clock::rate() will run for 20 seconds, please be patient");

        let mut pool = LocalPool::new();
        let sp = pool.spawner();

        for &i in &[1, 2, 4, 8, 16] {
            let target = i * 10_240;

            let limiter = <Limiter>::new(target as f64);
            for &speed_limit in &[target, target * 2] {
                limiter.reset_statistics();
                limiter.set_speed_limit(speed_limit as f64);
                let start = Instant::now();

                let handles = (0..i).map(|_| {
                    let limiter = limiter.clone();
                    sp.spawn_with_handle(async move {
                        // tests for 2 seconds.
                        let until = Instant::now() + Duration::from_secs(2);
                        while Instant::now() < until {
                            let size = thread_rng().gen_range(1, 1 + target / 10);
                            limiter.consume(size).await;
                        }
                    })
                    .unwrap()
                });

                pool.run_until(join_all(handles));
                assert_eq!(limiter.shared_count(), 1);

                let elapsed = start.elapsed();
                let speed = limiter.total_bytes_consumed() as f64 / elapsed.as_secs_f64();
                let diff_ratio = speed / speed_limit as f64;
                eprintln!(
                    "rate: {} threads, expected speed {} B/s, actual speed {:.0} B/s, elapsed {:?}",
                    i, speed_limit, speed, elapsed
                );
                assert!(0.80 <= diff_ratio && diff_ratio <= 1.25);
                assert!(elapsed <= Duration::from_secs(4));
            }
        }
    }

    #[test]
    fn block() {
        eprintln!("tests_with_standard_clock::block() will run for 20 seconds, please be patient");

        for &i in &[1, 2, 4, 8, 16] {
            let target = i * 10_240;

            let limiter = <Limiter>::new(target as f64);
            for &speed_limit in &[target, target * 2] {
                limiter.reset_statistics();
                limiter.set_speed_limit(speed_limit as f64);
                let start = Instant::now();

                let handles = (0..i)
                    .map(|_| {
                        let limiter = limiter.clone();
                        std::thread::spawn(move || {
                            // tests for 2 seconds.
                            let until = Instant::now() + Duration::from_secs(2);
                            while Instant::now() < until {
                                let size = thread_rng().gen_range(1, 1 + target / 10);
                                limiter.blocking_consume(size);
                            }
                        })
                    })
                    .collect::<Vec<_>>();

                for jh in handles {
                    jh.join().unwrap();
                }

                assert_eq!(limiter.shared_count(), 1);

                let elapsed = start.elapsed();
                let speed = limiter.total_bytes_consumed() as f64 / elapsed.as_secs_f64();
                let diff_ratio = speed / speed_limit as f64;
                eprintln!(
                    "block: {} threads, expected speed {} B/s, actual speed {:.0} B/s, elapsed {:?}",
                    i, speed_limit, speed, elapsed
                );
                assert!(0.80 <= diff_ratio && diff_ratio <= 1.25);
                assert!(elapsed <= Duration::from_secs(4));
            }
        }
    }
}
