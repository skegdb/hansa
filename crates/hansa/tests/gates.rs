//! Performance and token-efficiency gates.
//!
//! Run with:
//!   cargo test --release --test gates
//!
//! Each gate is a `#[test]` that fails when a measured metric exceeds
//! (or falls below) a fixed threshold. Thresholds are chosen with
//! ~2-3x headroom over baseline on a 2024 Apple Silicon laptop. Bump
//! them deliberately when you make a feature that wins back budget;
//! never bump them silently to make a red CI green. See
//! `private/gates.md` for the policy.
//!
//! Gates run sequentially because they each construct their own data
//! and we want stable timings.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hansa::saga::{build_saga_from_tenant, score_saga};
use hansa::{CharCountTokenizer, ContextBuilder, HitOrigin, MembraneHit};
use skeg_rigging::{RecordId, TenantId};

/// Gates only make sense in release mode; debug code is roughly an
/// order of magnitude slower and would force absurd thresholds. Each
/// gate calls this first and bails out when invoked under plain
/// `cargo test`. CI runs `cargo test --release --test gates` so the
/// guard never fires in production.
fn skip_unless_release() -> bool {
    if cfg!(debug_assertions) {
        eprintln!(
            "[gates] skipping in debug mode; run `cargo test --release --test gates` to enforce"
        );
        true
    } else {
        false
    }
}

// ─────────────────────────────────────────────────────────────────────
// Thresholds. Treat these as part of the public commitment of v0.1.x.
// ─────────────────────────────────────────────────────────────────────

/// Time to build a saga from 1000 records at dim=32. Includes k-means
/// (k=32 per the schedule) and tag aggregation. Released M1 baseline
/// on a quiet M2 Pro is ~4.2 ms; we allow up to 12 ms before the gate
/// fires.
const GATE_SAGA_BUILD_1K_32D_MS: u128 = 12;

/// Time to score one saga (k=64 centroids, dim=128) against a query.
/// Inner loop of the membrane's peer-selection step. Baseline ~5-8 us;
/// gate at 30 us.
const GATE_SAGA_SCORE_K64_128D_US: u128 = 30;

/// Time to assemble a 200-hit context bundle with dedup, ranking,
/// and a 2048-token budget. Baseline a few hundred microseconds on
/// 200×200ch hits; gate at 5 ms.
const GATE_CONTEXT_ASSEMBLE_200_HITS_MS: u128 = 5;

/// **Dedup effectiveness.** Build a corpus with a known fraction of
/// exact duplicates; after `ContextBuilder` runs with dedup on, at
/// most this fraction of duplicates may survive. We aim for 0% but
/// allow 5% to absorb any future change in the normalisation rule.
const GATE_MAX_DUP_SURVIVAL_RATIO: f32 = 0.05;

/// **Budget honouring.** A 200-hit corpus crammed into a tight budget
/// must produce a bundle whose total tokens stay below the budget. No
/// fudge factor: budget is a hard ceiling.
const GATE_TOKEN_BUDGET_OVERFLOW: usize = 0;

/// **Yield under tight budget.** Even with a tight budget, the bundle
/// should not be empty when the corpus contains relevant hits. Catches
/// regressions where over-eager filtering eats everything.
const GATE_MIN_BUNDLE_ITEMS: usize = 1;

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

fn synth_vectors(n: u64, dim: u32) -> Vec<Vec<f32>> {
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

fn hit(origin: HitOrigin, id: u64, sim: f32, payload: &str) -> MembraneHit {
    MembraneHit {
        record_id: RecordId(id),
        similarity: sim,
        origin,
        payload: Bytes::from(payload.as_bytes().to_vec()),
        embedding: None,
    }
}

// ─────────────────────────────────────────────────────────────────────
// Gates
// ─────────────────────────────────────────────────────────────────────

#[test]
fn gate_saga_build_1k_32d_under_threshold() {
    if skip_unless_release() {
        return;
    }
    let n = 1_000u64;
    let dim = 32u32;
    let vectors = synth_vectors(n, dim);

    let start = Instant::now();
    let saga = build_saga_from_tenant(
        TenantId::ZERO,
        dim,
        n,
        vectors,
        Vec::<String>::new(),
        1,
        7,
    )
    .unwrap();
    let elapsed_ms = start.elapsed().as_millis();
    assert_eq!(saga.embedding_dim, dim);
    assert!(
        elapsed_ms <= GATE_SAGA_BUILD_1K_32D_MS,
        "saga_build(1000, dim=32) took {elapsed_ms} ms, gate {GATE_SAGA_BUILD_1K_32D_MS} ms - \
         a regression or build under contention. See private/gates.md."
    );
}

#[test]
fn gate_saga_score_k64_128d_under_threshold() {
    if skip_unless_release() {
        return;
    }
    let dim = 128u32;
    let n = 5_000u64;
    let saga = build_saga_from_tenant(
        TenantId::ZERO,
        dim,
        n,
        synth_vectors(n, dim),
        Vec::<String>::new(),
        1,
        7,
    )
    .unwrap();
    // Schedule gives k=64 for n in [10_000, 99_999]; n=5000 yields k=32,
    // so seed n high enough to land at k=64.
    let n2 = 50_000u64;
    let saga2 = build_saga_from_tenant(
        TenantId::ZERO,
        dim,
        n2,
        synth_vectors(2_000, dim),
        Vec::<String>::new(),
        1,
        7,
    )
    .unwrap();
    assert_eq!(saga2.centroids.len(), 64);
    let _ = saga; // first saga only used to warm up caches indirectly

    let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.01).sin()).collect();

    // Warm up + measure best-of-100 to dampen scheduler noise.
    for _ in 0..16 {
        let _ = score_saga(&saga2, &query);
    }
    let mut best_us = u128::MAX;
    for _ in 0..100 {
        let s = Instant::now();
        let _ = score_saga(&saga2, &query);
        best_us = best_us.min(s.elapsed().as_micros());
    }
    assert!(
        best_us <= GATE_SAGA_SCORE_K64_128D_US,
        "score_saga(k=64, dim=128) best-of-100 = {best_us} us, gate \
         {GATE_SAGA_SCORE_K64_128D_US} us. See private/gates.md."
    );
}

#[test]
fn gate_context_assemble_200_hits_under_threshold() {
    if skip_unless_release() {
        return;
    }
    let hits = synthetic_corpus(200, 200, 0.1);
    let start = Instant::now();
    let bundle = ContextBuilder::from_hits(hits)
        .min_similarity(0.2)
        .token_budget(2048)
        .dedup(true)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();
    let elapsed_ms = start.elapsed().as_millis();
    assert!(!bundle.is_empty());
    assert!(
        elapsed_ms <= GATE_CONTEXT_ASSEMBLE_200_HITS_MS,
        "context_assembly(200 hits) took {elapsed_ms} ms, gate \
         {GATE_CONTEXT_ASSEMBLE_200_HITS_MS} ms."
    );
}

#[test]
fn gate_dedup_drops_known_duplicates() {
    // 30 unique + 30 exact duplicates (different ids, identical text).
    let mut hits = Vec::with_capacity(60);
    for i in 0..30 {
        hits.push(hit(
            HitOrigin::Local,
            i as u64,
            0.9 - (i as f32) * 0.01,
            &format!("unique content number {i}"),
        ));
    }
    for i in 0..30 {
        hits.push(hit(
            HitOrigin::Remote {
                tenant_id: TenantId::from_bytes([2; 16]),
            },
            (100 + i) as u64,
            0.5 - (i as f32) * 0.01,
            &format!("unique content number {i}"), // exact duplicate
        ));
    }
    let bundle = ContextBuilder::from_hits(hits)
        .min_similarity(-1.0)
        .token_budget(usize::MAX)
        .dedup(true)
        .build();
    let dups_kept = bundle.items.len() as i64 - 30;
    let dup_survival = (dups_kept.max(0) as f32) / 30.0;
    assert!(
        dup_survival <= GATE_MAX_DUP_SURVIVAL_RATIO,
        "dedup let {dup_survival:.2} of duplicates survive (gate \
         {GATE_MAX_DUP_SURVIVAL_RATIO}). Kept {} items from 60.",
        bundle.items.len()
    );
}

#[test]
fn gate_token_budget_is_hard_ceiling() {
    let hits = synthetic_corpus(200, 200, 0.0);
    let bundle = ContextBuilder::from_hits(hits)
        .token_budget(500)
        .dedup(false)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();
    let overflow = bundle.total_tokens.saturating_sub(500);
    assert!(
        overflow == GATE_TOKEN_BUDGET_OVERFLOW,
        "bundle reported {} tokens but the budget was 500 (overflow {} \
         > gate {}).",
        bundle.total_tokens,
        overflow,
        GATE_TOKEN_BUDGET_OVERFLOW
    );
}

#[test]
fn gate_yield_is_not_zero_when_corpus_relevant() {
    let hits = synthetic_corpus(50, 80, 0.0);
    let bundle = ContextBuilder::from_hits(hits)
        .min_similarity(0.0)
        .token_budget(256)
        .dedup(true)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();
    assert!(
        bundle.items.len() >= GATE_MIN_BUNDLE_ITEMS,
        "bundle returned {} items, gate >= {}. ContextBuilder filters \
         too aggressively?",
        bundle.items.len(),
        GATE_MIN_BUNDLE_ITEMS
    );
}

// ─────────────────────────────────────────────────────────────────────
// Shared corpus generator
// ─────────────────────────────────────────────────────────────────────

fn synthetic_corpus(count: usize, chars: usize, dup_ratio: f32) -> Vec<MembraneHit> {
    let dup_every = if dup_ratio > 0.0 {
        ((1.0 / dup_ratio).round() as usize).max(1)
    } else {
        usize::MAX
    };
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let canonical = if i % dup_every == 0 && i > 0 {
            i - dup_every
        } else {
            i
        };
        let payload: String = (0..chars)
            .map(|c| ((canonical + c) as u8 % 26 + b'a') as char)
            .collect();
        let origin = if i % 3 == 0 {
            HitOrigin::Local
        } else {
            HitOrigin::Remote {
                tenant_id: TenantId::from_bytes([(i % 8) as u8 + 1; 16]),
            }
        };
        out.push(MembraneHit {
            record_id: RecordId(i as u64),
            similarity: 1.0 - (i as f32) * 0.005,
            origin,
            payload: Bytes::from(payload),
            embedding: None,
        });
    }
    out
}
