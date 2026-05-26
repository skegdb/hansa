//! Criterion benchmarks: saga construction throughput.
//!
//! Measures `build_saga_from_tenant` across record counts and embedding
//! dimensions. The k value follows hansa's `default_k_for` schedule,
//! so the picture matches production behaviour.
//!
//! Run with:
//!   cargo bench --bench saga_build

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use hansa::saga::build_saga_from_tenant;
use skeg_rigging::TenantId;

fn synthetic_vectors(n: u64, dim: u32) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..dim)
                .map(|d| {
                    // Cheap pseudo-random: hash-mix without an RNG dep on
                    // the hot path. Reproducible across runs.
                    let h = ((i as u32).wrapping_mul(2654435761) ^ d.wrapping_mul(40503)) as f32;
                    (h.sin() + 1.0) * 0.5
                })
                .collect()
        })
        .collect()
}

fn bench_saga_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("saga_build");
    group.sample_size(20);
    for &(n, dim) in &[(100u64, 8u32), (1_000, 32), (10_000, 32), (10_000, 128)] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(
            BenchmarkId::new("records", format!("{n}x{dim}d")),
            &(n, dim),
            |b, &(n, dim)| {
                let vectors = synthetic_vectors(n, dim);
                b.iter(|| {
                    let saga = build_saga_from_tenant(
                        TenantId::ZERO,
                        dim,
                        n,
                        vectors.clone(),
                        Vec::<String>::new(),
                        1,
                        7,
                    )
                    .unwrap();
                    black_box(saga);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_saga_build);
criterion_main!(benches);
