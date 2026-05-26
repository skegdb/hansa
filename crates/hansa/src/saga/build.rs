//! Saga construction: reservoir sample + k-means++ over a tenant's
//! vectors, plus top-N tag aggregation.

use std::collections::HashMap;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::Result;
use crate::saga::{Centroid, Saga, TagAggregate};

/// Default reservoir sample size: 10_000 vectors.
const RESERVOIR_SAMPLE: usize = 10_000;
/// k-means convergence threshold on relative inertia change.
const KMEANS_TOLERANCE: f64 = 1e-3;
/// k-means maximum iterations.
const KMEANS_MAX_ITER: usize = 20;
/// Maximum number of tag entries kept in the aggregate.
const TOP_TAGS: usize = 10;

/// Pick `k` for a tenant with `n` records, matching the schedule from
/// the hansa design doc §8.1.
pub fn default_k_for(n: u64) -> usize {
    match n {
        0..=999 => 16,
        1_000..=9_999 => 32,
        10_000..=99_999 => 64,
        _ => 128,
    }
}

/// Build a saga from an [`IterVectors`](skeg_rigging::IterVectors)-capable
/// tenant. The caller supplies a tag-iterator separately, since rigging
/// v0.1 has no `iter_records` trait (only iter_vectors). For real
/// integrations the adapter is expected to expose the tag stream
/// through its own concrete type.
///
/// Determinism: the reservoir sampler is seeded by `seed` so test cases
/// can reproduce centroid results.
pub fn build_saga_from_tenant<I, T>(
    tenant_id: skeg_rigging::TenantId,
    embedding_dim: u32,
    record_count: u64,
    vectors: I,
    tags: T,
    built_at: i64,
    seed: u64,
) -> Result<Saga>
where
    I: IntoIterator<Item = Vec<f32>>,
    T: IntoIterator<Item = String>,
{
    let mut rng = SmallRng::seed_from_u64(seed);
    let sample = reservoir_sample(vectors, RESERVOIR_SAMPLE, &mut rng);

    let k = default_k_for(record_count).min(sample.len().max(1));
    let centroids = if sample.is_empty() {
        Vec::new()
    } else {
        kmeans_lloyd(&sample, k, embedding_dim as usize, &mut rng)
    };

    let tags = aggregate_top_tags(tags);

    Ok(Saga {
        tenant_id,
        built_at,
        record_count,
        embedding_dim,
        centroids,
        tags,
    })
}

fn reservoir_sample<I: IntoIterator<Item = Vec<f32>>>(
    src: I,
    target: usize,
    rng: &mut SmallRng,
) -> Vec<Vec<f32>> {
    let mut out: Vec<Vec<f32>> = Vec::with_capacity(target);
    for (seen, v) in src.into_iter().enumerate() {
        if out.len() < target {
            out.push(v);
        } else {
            let j = rng.random_range(0..=seen);
            if j < target {
                out[j] = v;
            }
        }
    }
    out
}

fn aggregate_top_tags<T: IntoIterator<Item = String>>(tags: T) -> Vec<TagAggregate> {
    let mut counts: HashMap<String, u32> = HashMap::new();
    for t in tags {
        *counts.entry(t).or_default() += 1;
    }
    let mut entries: Vec<TagAggregate> = counts
        .into_iter()
        .map(|(tag, count)| TagAggregate { tag, count })
        .collect();
    entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.tag.cmp(&b.tag)));
    entries.truncate(TOP_TAGS);
    entries
}

fn kmeans_lloyd(
    sample: &[Vec<f32>],
    k: usize,
    dim: usize,
    rng: &mut SmallRng,
) -> Vec<Centroid> {
    let n = sample.len();
    debug_assert!(n > 0);
    let k = k.min(n);

    let mut centroids = kmeans_plus_plus_init(sample, k, rng);
    let mut assignments = vec![0usize; n];
    let mut prev_inertia = f64::INFINITY;

    for _iter in 0..KMEANS_MAX_ITER {
        // Assign step.
        let mut inertia = 0.0f64;
        for (i, point) in sample.iter().enumerate() {
            let (best_idx, best_dist) = nearest_centroid(point, &centroids);
            assignments[i] = best_idx;
            inertia += best_dist as f64;
        }

        // Convergence check.
        let rel_change = if prev_inertia.is_finite() && prev_inertia > 0.0 {
            (prev_inertia - inertia).abs() / prev_inertia
        } else {
            f64::INFINITY
        };
        if rel_change < KMEANS_TOLERANCE {
            break;
        }
        prev_inertia = inertia;

        // Update step.
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0u32; k];
        for (i, point) in sample.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                sums[c][d] += point[d];
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                // Empty cluster: leave centroid where it was.
                continue;
            }
            let denom = counts[c] as f32;
            for d in 0..dim {
                centroids[c][d] = sums[c][d] / denom;
            }
        }
    }

    // Final counts for cluster_size.
    let mut counts = vec![0u32; k];
    for &a in &assignments {
        counts[a] += 1;
    }

    centroids
        .into_iter()
        .zip(counts)
        .map(|(vector, cluster_size)| Centroid {
            cluster_size,
            vector,
        })
        .collect()
}

fn kmeans_plus_plus_init(
    sample: &[Vec<f32>],
    k: usize,
    rng: &mut SmallRng,
) -> Vec<Vec<f32>> {
    let n = sample.len();
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    let first = rng.random_range(0..n);
    centroids.push(sample[first].clone());

    let mut dists = vec![f32::INFINITY; n];
    while centroids.len() < k {
        let last = centroids.last().unwrap();
        for (i, p) in sample.iter().enumerate() {
            let d = squared_distance(p, last);
            if d < dists[i] {
                dists[i] = d;
            }
        }
        let total: f64 = dists.iter().map(|&d| d as f64).sum();
        if total == 0.0 {
            // All remaining points coincide with chosen centroids;
            // duplicate the last centroid and break out - k > n
            // duplicates is harmless because we count cluster sizes.
            centroids.push(last.clone());
            continue;
        }
        let mut threshold = rng.random_range(0.0..total);
        let mut chosen = 0usize;
        for (i, &d) in dists.iter().enumerate() {
            threshold -= d as f64;
            if threshold <= 0.0 {
                chosen = i;
                break;
            }
        }
        centroids.push(sample[chosen].clone());
    }
    centroids
}

fn nearest_centroid(point: &[f32], centroids: &[Vec<f32>]) -> (usize, f32) {
    let mut best_idx = 0;
    let mut best_dist = f32::INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let d = squared_distance(point, c);
        if d < best_dist {
            best_dist = d;
            best_idx = i;
        }
    }
    (best_idx, best_dist)
}

fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use skeg_rigging::TenantId;

    #[test]
    fn k_schedule_matches_design() {
        assert_eq!(default_k_for(0), 16);
        assert_eq!(default_k_for(500), 16);
        assert_eq!(default_k_for(999), 16);
        assert_eq!(default_k_for(1_000), 32);
        assert_eq!(default_k_for(10_000), 64);
        assert_eq!(default_k_for(99_999), 64);
        assert_eq!(default_k_for(100_000), 128);
        assert_eq!(default_k_for(10_000_000), 128);
    }

    #[test]
    fn kmeans_partitions_two_obvious_clusters() {
        // 200 points: 100 near (0,0), 100 near (10,10).
        let mut sample = Vec::new();
        for _ in 0..100 {
            sample.push(vec![0.1, 0.1]);
            sample.push(vec![10.0, 10.0]);
        }
        let mut rng = SmallRng::seed_from_u64(42);
        let centroids = kmeans_lloyd(&sample, 2, 2, &mut rng);
        assert_eq!(centroids.len(), 2);
        let near_origin = centroids.iter().any(|c| c.vector[0] < 5.0);
        let near_ten = centroids.iter().any(|c| c.vector[0] > 5.0);
        assert!(near_origin && near_ten);
        // Each cluster should have ~100 points.
        for c in &centroids {
            assert!(c.cluster_size > 50, "cluster size {}", c.cluster_size);
        }
    }

    #[test]
    fn build_saga_smoke() {
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|i| {
                let x = (i as f32) * 0.01;
                vec![x, 1.0 - x, 0.5]
            })
            .collect();
        let tags: Vec<String> = vec![
            "code", "code", "design", "code", "design", "docs",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let saga = build_saga_from_tenant(
            TenantId::from_bytes([1; 16]),
            3,
            50,
            vectors.clone(),
            tags,
            1_700_000_000,
            7,
        )
        .unwrap();

        assert_eq!(saga.record_count, 50);
        assert_eq!(saga.embedding_dim, 3);
        // 50 records → k = 16 by schedule, but min(k, sample) = min(16, 50) = 16.
        assert_eq!(saga.centroids.len(), 16);
        // Each centroid vector has correct dim.
        for c in &saga.centroids {
            assert_eq!(c.vector.len(), 3);
        }
        // Tag aggregate: top tag is "code" (3 occurrences).
        assert_eq!(saga.tags[0].tag, "code");
        assert_eq!(saga.tags[0].count, 3);
    }

    #[test]
    fn empty_tenant_produces_empty_saga_body() {
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            4,
            0,
            Vec::<Vec<f32>>::new(),
            Vec::<String>::new(),
            0,
            0,
        )
        .unwrap();
        assert!(saga.centroids.is_empty());
        assert!(saga.tags.is_empty());
    }

    #[test]
    fn cosine_score_of_built_saga_is_meaningful() {
        // Build a saga from records all near unit-x.
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|_| vec![1.0f32, 0.01, 0.01])
            .collect();
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            3,
            50,
            vectors,
            Vec::<String>::new(),
            0,
            7,
        )
        .unwrap();
        let score_match = crate::saga::score_saga(&saga, &[1.0, 0.0, 0.0]);
        let score_orthogonal = crate::saga::score_saga(&saga, &[0.0, 1.0, 0.0]);
        assert!(
            score_match > score_orthogonal,
            "match={score_match} ortho={score_orthogonal}"
        );
    }
}
