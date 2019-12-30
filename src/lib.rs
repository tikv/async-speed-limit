// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

#![warn(
    elided_lifetimes_in_paths,
    future_incompatible,
    missing_debug_implementations,
    missing_docs,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_qualifications,
    variant_size_differences,
    clippy::all,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::must_use_candidate)]
#![cfg_attr(feature = "read-initializer", feature(read_initializer))]
#![cfg_attr(feature = "doc", feature(external_doc))]
#![cfg_attr(feature = "doc", doc(include = "../README.md"))]
#![cfg_attr(
    not(feature = "doc"),
    doc = "
Asynchronously speed-limiting multiple byte streams (`AsyncRead` and `AsyncWrite`).
See README for details.
"
)]

pub mod clock;
#[cfg(feature = "futures-io")]
mod io;
pub mod limiter;

pub use limiter::{Limiter, Resource};
