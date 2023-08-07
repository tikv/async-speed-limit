// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

use crate::{clock::Clock, limiter::Resource};
use std::{
    io::{self, IoSlice, IoSliceMut},
    pin::Pin,
    task::{Context, Poll},
};

fn length_of_result_usize<B>(a: &io::Result<usize>, _: &B) -> usize {
    *a.as_ref().unwrap_or(&0)
}

impl<R: futures_io::AsyncRead, C: Clock> futures_io::AsyncRead for Resource<R, C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_limited(cx, (), length_of_result_usize, |r, cx, _| {
            r.poll_read(cx, buf)
        })
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_limited(cx, (), length_of_result_usize, |r, cx, _| {
            r.poll_read_vectored(cx, bufs)
        })
    }
}

impl<R: futures_io::AsyncWrite, C: Clock> futures_io::AsyncWrite for Resource<R, C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_limited(cx, (), length_of_result_usize, |r, cx, _| {
            r.poll_write(cx, buf)
        })
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_limited(cx, (), length_of_result_usize, |r, cx, _| {
            r.poll_write_vectored(cx, bufs)
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_pin_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_pin_mut().poll_close(cx)
    }
}

#[cfg(feature = "tokio")]
impl<R: tokio::io::AsyncRead, C: Clock> tokio::io::AsyncRead for Resource<R, C> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = buf.filled().len();

        self.poll_limited(
            cx,
            buf,
            |res: &io::Result<()>, buf| {
                res.is_ok()
                    .then(|| buf.filled().len() - filled)
                    .unwrap_or_default()
            },
            |r, cx, buf| r.poll_read(cx, buf),
        )
    }
}

#[cfg(feature = "tokio")]
impl<R: tokio::io::AsyncWrite, C: Clock> tokio::io::AsyncWrite for Resource<R, C> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_limited(cx, (), length_of_result_usize, |r, cx, _| {
            r.poll_write(cx, buf)
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_pin_mut().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_pin_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::{ManualClock, Nanoseconds},
        Limiter,
    };
    use futures_executor::LocalPool;
    use futures_util::{
        io::{copy_buf, BufReader},
        task::SpawnExt,
    };
    use rand::{thread_rng, RngCore};

    #[test]
    fn limited_read() {
        let mut pool = LocalPool::new();
        let sp = pool.spawner();

        let limiter = Limiter::<ManualClock>::new(512.0);
        let clock = limiter.clock();

        sp.spawn({
            let limiter = limiter.clone();
            let clock = clock.clone();
            async move {
                let mut src = vec![0_u8; 1024];
                thread_rng().fill_bytes(&mut src);
                let mut dst = Vec::new();

                let read = BufReader::with_capacity(256, limiter.limit(&*src));
                let count = copy_buf(read, &mut dst).await.unwrap();

                assert_eq!(clock.now(), Nanoseconds(2_000_000_000));
                assert_eq!(count, src.len() as u64);
                assert!(src == dst);
            }
        })
        .unwrap();

        clock.set_time(Nanoseconds(0));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 256);

        clock.set_time(Nanoseconds(500_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 512);

        clock.set_time(Nanoseconds(1_000_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 768);

        clock.set_time(Nanoseconds(1_500_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 1024);

        clock.set_time(Nanoseconds(2_000_000_000));
        pool.run_until_stalled();

        assert!(!pool.try_run_one());
    }

    #[test]
    fn unlimited_read() {
        let mut pool = LocalPool::new();
        let sp = pool.spawner();

        let limiter = Limiter::<ManualClock>::new(std::f64::INFINITY);

        sp.spawn({
            async move {
                let mut src = vec![0_u8; 1024];
                thread_rng().fill_bytes(&mut src);
                let mut dst = Vec::new();

                let read = BufReader::with_capacity(256, limiter.limit(&*src));
                let count = copy_buf(read, &mut dst).await.unwrap();

                assert_eq!(count, src.len() as u64);
                assert!(src == dst);
            }
        })
        .unwrap();

        pool.run_until_stalled();
        assert!(!pool.try_run_one());
    }

    #[test]
    fn limited_write() {
        let mut pool = LocalPool::new();
        let sp = pool.spawner();

        let limiter = Limiter::<ManualClock>::new(512.0);
        let clock = limiter.clock();

        sp.spawn({
            let limiter = limiter.clone();
            let clock = clock.clone();
            async move {
                let mut src = vec![0_u8; 1024];
                thread_rng().fill_bytes(&mut src);

                let read = BufReader::with_capacity(256, &*src);
                let mut write = limiter.limit(Vec::new());
                let count = copy_buf(read, &mut write).await.unwrap();

                assert_eq!(clock.now(), Nanoseconds(1_500_000_000));
                assert_eq!(count, src.len() as u64);
                assert!(src == write.into_inner());
            }
        })
        .unwrap();

        clock.set_time(Nanoseconds(0));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 256);

        clock.set_time(Nanoseconds(500_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 512);

        clock.set_time(Nanoseconds(1_000_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 768);

        clock.set_time(Nanoseconds(1_500_000_000));
        pool.run_until_stalled();
        assert_eq!(limiter.total_bytes_consumed(), 1024);

        clock.set_time(Nanoseconds(2_000_000_000));
        pool.run_until_stalled();

        assert!(!pool.try_run_one());
    }
}

#[cfg(test)]
#[cfg(feature = "standard-clock")]
mod tokio_tests {
    use std::{
        io,
        time::{Duration, Instant},
    };

    use crate::Limiter;

    use futures_util::{future::ok, TryStreamExt as _};
    use tokio::io::{copy, repeat, sink, AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_util::codec::{BytesCodec, FramedRead};

    #[tokio::test]
    async fn limited_read() -> io::Result<()> {
        let limiter = <Limiter>::new(32768.0);

        let start_time = Instant::now();

        let reader = repeat(50).take(65536);
        let mut reader = limiter.limit(reader);

        let mut sink = sink();
        let total = copy(&mut reader, &mut sink).await?;
        sink.shutdown().await?;

        let elapsed = start_time.elapsed();

        assert!(
            Duration::from_millis(1900) <= elapsed && elapsed <= Duration::from_millis(2100),
            "elapsed = {:?}",
            elapsed
        );
        assert_eq!(total, 65536);

        Ok(())
    }

    #[tokio::test]
    async fn unlimited_read() -> io::Result<()> {
        let limiter = <Limiter>::new(std::f64::INFINITY);

        let start_time = Instant::now();

        let reader = repeat(50).take(65536);
        let mut reader = limiter.limit(reader);
        let mut sink = sink();

        let total = copy(&mut reader, &mut sink).await?;
        sink.shutdown().await?;

        let elapsed = start_time.elapsed();

        assert!(
            elapsed <= Duration::from_millis(100),
            "elapsed = {:?}",
            elapsed
        );
        assert_eq!(total, 65536);

        Ok(())
    }

    #[tokio::test]
    async fn limited_read_byte_stream() -> io::Result<()> {
        let limiter = <Limiter>::new(30000.0);

        let start_time = Instant::now();

        let reader = repeat(50).take(60000);
        let reader = limiter.limit(reader);

        let total = FramedRead::new(reader, BytesCodec::new())
            .try_fold(0, |i, j| {
                assert!(j.iter().all(|b| *b == 50_u8), "{} / {:?}", i, j);
                ok(i + j.len())
            })
            .await?;

        let elapsed = start_time.elapsed();

        assert!(
            Duration::from_millis(1900) <= elapsed && elapsed <= Duration::from_millis(2100),
            "elapsed = {:?}",
            elapsed
        );
        assert_eq!(total, 60000);

        Ok(())
    }
}
