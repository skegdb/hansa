//! Bundle caching for similar queries (F.8).
//!
//! Streaming-prompt LLM patterns issue many near-identical queries
//! while a single user message is being typed. Re-running the full
//! membrane and context-assembly pipeline for each one burns latency
//! and produces almost-identical bundles. [`BundleCache`] short-
//! circuits the pipeline: if the incoming query's embedding is
//! cosine-similar enough to a cached query, return the cached
//! [`ContextBundle`] verbatim.
//!
//! ## Match semantics
//!
//! - Cosine of the incoming embedding to each cached entry's stored
//!   centre. The first entry past `hit_threshold` (default 0.98) wins.
//! - Below threshold → no hit (cache returns `None`).
//! - Mismatched embedding dim → no hit (defensive; mixing dims in one
//!   cache is a caller bug).
//!
//! v0.1 uses a linear scan over a small ring buffer (default capacity
//! 16). LSH-style signature bucketing (design-token-efficiency §7.2)
//! is a v0.2 optimisation worth paying for only at capacity > ~64.
//!
//! ## Invalidation
//!
//! - TTL: each entry has a wall-clock expiry; the default is 60 s
//!   ([`BundleCache::DEFAULT_TTL`]).
//! - Local writes: the cache cannot see them. Callers that
//!   [`crate::Hansa::insert`] (or otherwise mutate the local tenant)
//!   must call [`Self::invalidate_all`] afterwards. v0.1 keeps this
//!   explicit; F.13 events will let the cache subscribe on its own.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let mut cache = BundleCache::new(/* dim */ 384);
//! if let Some(bundle) = cache.get(&embedding) {
//!     return bundle;
//! }
//! let bundle = ContextBuilder::from_hits(hansa.query(&embedding)?.execute()?)
//!     .token_budget(2048)
//!     .build();
//! cache.insert(embedding.clone(), bundle.clone());
//! bundle
//! ```

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::context::ContextBundle;

/// LRU + TTL cache over query embeddings → [`ContextBundle`].
pub struct BundleCache {
    dim: usize,
    capacity: usize,
    ttl: Duration,
    hit_threshold: f32,
    entries: VecDeque<Entry>,
}

struct Entry {
    /// Centre vector: the query embedding that produced this bundle,
    /// pre-normalised to unit length so lookup can dot-product
    /// directly.
    centre_unit: Vec<f32>,
    bundle: ContextBundle,
    inserted_at: Instant,
}

impl BundleCache {
    /// Default TTL for cache entries (60 s).
    pub const DEFAULT_TTL: Duration = Duration::from_secs(60);
    /// Default cache capacity (16 entries).
    pub const DEFAULT_CAPACITY: usize = 16;
    /// Default cosine threshold above which a cached entry is reused.
    pub const DEFAULT_HIT_THRESHOLD: f32 = 0.98;

    /// Build a cache for `dim`-dimensional embeddings with defaults.
    pub fn new(dim: usize) -> Self {
        Self::with_config(
            dim,
            Self::DEFAULT_CAPACITY,
            Self::DEFAULT_TTL,
            Self::DEFAULT_HIT_THRESHOLD,
        )
    }

    /// Build a cache with explicit knobs. `capacity` is the number of
    /// distinct queries held; reaching it evicts the oldest entry on
    /// insert. `ttl` clamps each entry's lifetime regardless of LRU
    /// position. `hit_threshold` is the minimum cosine for a lookup to
    /// be considered a hit; values below ~0.95 risk surfacing the
    /// wrong bundle.
    pub fn with_config(
        dim: usize,
        capacity: usize,
        ttl: Duration,
        hit_threshold: f32,
    ) -> Self {
        Self {
            dim,
            capacity: capacity.max(1),
            ttl,
            hit_threshold,
            entries: VecDeque::with_capacity(capacity.max(1)),
        }
    }

    /// Current entry count after expiring stale rows. O(n) - only
    /// meaningful in tests / diagnostics.
    pub fn len(&mut self) -> usize {
        self.expire();
        self.entries.len()
    }

    /// True when the cache holds no live entry.
    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    /// Cosine-match the query against every live entry; return the
    /// first bundle past `hit_threshold`. Returns `None` on dim
    /// mismatch, on no live entry, or when nothing scores high enough.
    ///
    /// The matched entry is moved to the back of the ring buffer so a
    /// subsequent eviction takes a colder entry first (true LRU on
    /// successful reads).
    pub fn get(&mut self, embedding: &[f32]) -> Option<ContextBundle> {
        if embedding.len() != self.dim {
            return None;
        }
        let unit = unit_normalise(embedding)?;
        self.expire();
        let mut best: Option<(usize, f32)> = None;
        for (idx, entry) in self.entries.iter().enumerate() {
            let score = dot(&unit, &entry.centre_unit);
            if score >= self.hit_threshold
                && best.map(|(_, s)| score > s).unwrap_or(true)
            {
                best = Some((idx, score));
            }
        }
        let (idx, _) = best?;
        // Move to back so it counts as most-recently-used.
        let entry = self.entries.remove(idx)?;
        let bundle = entry.bundle.clone();
        self.entries.push_back(entry);
        Some(bundle)
    }

    /// Store a `bundle` keyed by the query `embedding`. The embedding
    /// is unit-normalised once at insert; lookups never re-normalise.
    /// Dim mismatch is a silent no-op so a caller wiring this up
    /// against a heterogeneous fleet does not crash; queries against
    /// the wrong-dim cache return cache miss instead.
    pub fn insert(&mut self, embedding: Vec<f32>, bundle: ContextBundle) {
        if embedding.len() != self.dim {
            return;
        }
        let Some(unit) = unit_normalise(&embedding) else {
            return;
        };
        self.expire();
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(Entry {
            centre_unit: unit,
            bundle,
            inserted_at: Instant::now(),
        });
    }

    /// Drop every cached entry. Use this after a local write that
    /// could change a future bundle's local component.
    pub fn invalidate_all(&mut self) {
        self.entries.clear();
    }

    fn expire(&mut self) {
        let now = Instant::now();
        while let Some(front) = self.entries.front() {
            if now.duration_since(front.inserted_at) > self.ttl {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }
}

fn unit_normalise(v: &[f32]) -> Option<Vec<f32>> {
    let mut norm = 0.0f32;
    for x in v {
        norm += x * x;
    }
    let norm = norm.sqrt();
    if !norm.is_finite() || norm == 0.0 {
        return None;
    }
    Some(v.iter().map(|x| x / norm).collect())
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextBundle;

    fn empty_bundle() -> ContextBundle {
        ContextBundle::default()
    }

    fn marked_bundle(total_tokens: usize) -> ContextBundle {
        ContextBundle {
            total_tokens,
            ..ContextBundle::default()
        }
    }

    #[test]
    fn hit_returns_cached_bundle() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(42));
        let got = cache.get(&[1.0, 0.0, 0.0]).expect("identical query hits");
        assert_eq!(got.total_tokens, 42);
    }

    #[test]
    fn near_miss_above_threshold_still_hits() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(7));
        // Cosine ≈ 0.9999 - well above default 0.98 threshold.
        let got = cache.get(&[1.0, 0.001, 0.0]).expect("near-miss should hit");
        assert_eq!(got.total_tokens, 7);
    }

    #[test]
    fn below_threshold_misses() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(9));
        // Orthogonal - cosine 0.0.
        assert!(cache.get(&[0.0, 1.0, 0.0]).is_none());
    }

    #[test]
    fn ttl_expiry_drops_stale_entries() {
        let mut cache = BundleCache::with_config(
            3,
            8,
            Duration::from_millis(10),
            BundleCache::DEFAULT_HIT_THRESHOLD,
        );
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(5));
        std::thread::sleep(Duration::from_millis(30));
        assert!(cache.get(&[1.0, 0.0, 0.0]).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn capacity_evicts_oldest() {
        let mut cache = BundleCache::with_config(
            3,
            2,
            BundleCache::DEFAULT_TTL,
            BundleCache::DEFAULT_HIT_THRESHOLD,
        );
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(1));
        cache.insert(vec![0.0, 1.0, 0.0], marked_bundle(2));
        cache.insert(vec![0.0, 0.0, 1.0], marked_bundle(3));
        assert_eq!(cache.len(), 2);
        // First entry was evicted.
        assert!(cache.get(&[1.0, 0.0, 0.0]).is_none());
        // Second and third still resolve.
        assert!(cache.get(&[0.0, 1.0, 0.0]).is_some());
        assert!(cache.get(&[0.0, 0.0, 1.0]).is_some());
    }

    #[test]
    fn dim_mismatch_returns_none_and_does_not_insert() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0], marked_bundle(99));
        assert!(cache.is_empty());
        assert!(cache.get(&[1.0, 0.0]).is_none());
    }

    #[test]
    fn invalidate_all_clears_cache() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(11));
        cache.insert(vec![0.0, 1.0, 0.0], marked_bundle(22));
        cache.invalidate_all();
        assert!(cache.is_empty());
        assert!(cache.get(&[1.0, 0.0, 0.0]).is_none());
    }

    #[test]
    fn zero_vector_query_misses() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], marked_bundle(1));
        assert!(cache.get(&[0.0, 0.0, 0.0]).is_none());
    }

    #[test]
    fn empty_bundle_round_trip() {
        let mut cache = BundleCache::new(3);
        cache.insert(vec![1.0, 0.0, 0.0], empty_bundle());
        let got = cache.get(&[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(got.total_tokens, 0);
        assert!(got.items.is_empty());
    }
}
