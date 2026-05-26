//! Criterion benchmarks: `ContextBuilder` throughput.
//!
//! Two dimensions: number of hits and length of each payload. Tests
//! both the byte-level dedup hot loop and the token-budget cut-off.
//!
//! Run with:
//!   cargo bench --bench context_assembly

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use hansa::{CharCountTokenizer, ContextBuilder, HitOrigin, MembraneHit};
use skeg_rigging::{RecordId, TenantId};
use std::sync::Arc;

fn synthetic_hits(count: usize, payload_chars: usize, dup_ratio: f32) -> Vec<MembraneHit> {
    let dup_every = if dup_ratio > 0.0 {
        ((1.0 / dup_ratio).round() as usize).max(1)
    } else {
        usize::MAX
    };
    let mut hits = Vec::with_capacity(count);
    for i in 0..count {
        let is_dup = i % dup_every == 0 && i > 0;
        let canonical_id = if is_dup { i - dup_every } else { i };
        let content: String = (0..payload_chars)
            .map(|c| ((canonical_id + c) as u8 % 26 + b'a') as char)
            .collect();
        let origin = if i % 3 == 0 {
            HitOrigin::Local
        } else {
            HitOrigin::Remote {
                tenant_id: TenantId::from_bytes([(i % 8) as u8 + 1; 16]),
            }
        };
        hits.push(MembraneHit {
            record_id: RecordId(i as u64),
            similarity: 1.0 - (i as f32) * 0.01,
            origin,
            payload: Bytes::from(content),
            embedding: None,
        });
    }
    hits
}

fn bench_context_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("context_assembly");
    let cases: &[(usize, usize, f32, &str)] = &[
        (50, 100, 0.0, "50_100ch_nodup"),
        (50, 100, 0.2, "50_100ch_20pct_dup"),
        (200, 200, 0.1, "200_200ch_10pct_dup"),
    ];
    for &(count, chars, dup, label) in cases {
        group.throughput(Throughput::Elements(count as u64));
        let hits = synthetic_hits(count, chars, dup);
        group.bench_with_input(BenchmarkId::new("build", label), &hits, |b, hits| {
            b.iter(|| {
                let bundle = ContextBuilder::from_hits(black_box(hits.clone()))
                    .min_similarity(0.2)
                    .token_budget(2048)
                    .dedup(true)
                    .tokenizer(Arc::new(CharCountTokenizer))
                    .build();
                black_box(bundle);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_context_assembly);
criterion_main!(benches);
