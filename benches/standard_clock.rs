// Copyright 2019 TiKV Project Authors. Licensed under MIT or Apache-2.0.

use async_speed_limit::Limiter;
use criterion::{criterion_group, criterion_main, Criterion};

// Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
// infinity speed          time:   [32.527 ns 32.667 ns 32.818 ns]
//                         change: [-0.2189% +0.3899% +1.0013%] (p = 0.20 > 0.05)
fn criterion_benchmark_infinity_speed(c: &mut Criterion) {
    let limiter = <Limiter>::new(f64::INFINITY);

    c.bench_function("infinity speed", |b| {
        b.iter(|| futures_executor::block_on(limiter.consume(1)))
    });
}

criterion_group!(benches, criterion_benchmark_infinity_speed);
criterion_main!(benches);
