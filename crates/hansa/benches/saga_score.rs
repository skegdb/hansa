//! Criterion benchmarks: scoring a saga against a query.
//!
//! `score_saga` is the inner loop of the membrane's peer-selection
//! step. Two centroid counts (16 and 128) cover the lower and upper
//! ends of the schedule.
//!
//! Run with:
//!   cargo bench --bench saga_score

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use hansa::saga::{build_saga_from_tenant, score_saga};
use skeg_rigging::TenantId;

fn synth(n: u64, dim: u32) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..dim)
                .map(|d| {
                    let h = ((i as u32).wrapping_mul(2654435761) ^ d.wrapping_mul(40503)) as f32;
                    (h.sin() + 1.0) * 0.5
                })
                .collect()
        })
        .collect()
}

fn bench_saga_score(c: &mut Criterion) {
    let mut group = c.benchmark_group("saga_score");
    group.sample_size(100);
    for &(n, dim) in &[(1_000u64, 32u32), (100_000, 32), (100_000, 128)] {
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            dim,
            n,
            synth(n.min(2000), dim),
            Vec::<String>::new(),
            1,
            7,
        )
        .unwrap();
        let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.01).sin()).collect();
        let centroids = saga.centroids.len();
        group.bench_with_input(
            BenchmarkId::new("centroids", format!("n={n}_dim={dim}_k={centroids}")),
            &saga,
            |b, saga| {
                b.iter(|| {
                    let s = score_saga(black_box(saga), black_box(&query));
                    black_box(s);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_saga_score);
criterion_main!(benches);
