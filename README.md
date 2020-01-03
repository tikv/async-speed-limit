async-speed-limit
=================

[![Build status](https://github.com/tikv/async-speed-limit/workflows/Rust/badge.svg)](https://github.com/tikv/async-speed-limit/actions?query=workflow%3ARust)
[![Latest Version](https://img.shields.io/crates/v/async-speed-limit.svg)](https://crates.io/crates/async-speed-limit)
[![Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/async-speed-limit)

Asynchronously speed-limiting multiple byte streams (`AsyncRead` and `AsyncWrite`).

## Usage

```rust
use async_speed_limit::Limiter;
use futures_util::{
    future::try_join,
    io::{self, AsyncRead, AsyncWrite},
};
use std::{marker::Unpin, pin::Pin};

async fn copy_both_slowly(
    r1: impl AsyncRead,
    w1: &mut (impl AsyncWrite + Unpin),
    r2: impl AsyncRead,
    w2: &mut (impl AsyncWrite + Unpin),
) -> io::Result<()> {
    // create a speed limiter of 1.0 KiB/s.
    let limiter = <Limiter>::new(1024.0);

    // limit both readers by the same queue
    // (so 1.0 KiB/s is the combined maximum speed)
    let r1 = limiter.clone().limit(r1);
    let r2 = limiter.limit(r2);

    // do the copy.
    try_join(io::copy(r1, w1), io::copy(r2, w2)).await?;
    Ok(())
}
```

## Algorithm

async-speed-limit imposes the maximum speed limit using the [token bucket]
algorithm with a single queue. Transmission is throttled by adding a delay
proportional to the consumed tokens *after* it is sent. Because of this, the
traffic flow will have high burstiness around when the token bucket is full.

The time needed to refill the bucket from empty to full is called the
*refill period*. Reducing the refill period also reduces burstiness, but there
would be more sleeps. The default value (0.1 s) should be good enough for most
situations.

[token bucket]: https://en.wikipedia.org/wiki/Token_bucket

## Tokio 0.1

async-speed-limit is designed for "Futures 0.3". This package does not directly
support Tokio 0.1, but you can use futures-util's [`compat` module] to bridge
the types.

[futures-timer]: https://crates.io/crates/futures-timer
[`compat` module]: https://docs.rs/futures-util/0.3/futures_util/compat/index.html

## Cargo features

| Name                         | Dependency      | Effect                                                                                    |
|------------------------------|-----------------|-------------------------------------------------------------------------------------------|
| **standard-clock** (default) | [futures-timer] | Enables the `clock::StandardClock` struct.                                                |
| **fused-future** (default)   | [futures-core]  | Implements `FusedFuture` on `limiter::Consume`.                                           |
| **futures-io** (default)     | [futures-io]    | Implements `AsyncRead` and `AsyncWrite` on `limiter::Resource`.                           |
| **read-initializer**         | [futures-io]    | Implements `AsyncRead::initializer`.<br>Unstable and requires nightly compiler to enable. |

[futures-core]: https://crates.io/crates/futures-core
[futures-io]: https://crates.io/crates/futures-io

## License

async-speed-limit is dual-licensed under

* Apache 2.0 license ([LICENSE-Apache](./LICENSE-Apache) or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](./LICENSE-MIT) or <https://opensource.org/licenses/MIT>)
