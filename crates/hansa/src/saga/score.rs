//! Saga scoring against a query embedding.

use crate::saga::Saga;

/// Score `saga` against `query`: the maximum cosine similarity between
/// the query and any of the saga's centroids. A vault with one tightly
/// relevant cluster scores higher than one with broadly medium-relevant
/// content - the question we ask is "does *any* cluster look close?".
///
/// Returns [`f32::NEG_INFINITY`] for an empty saga.
pub fn score_saga(saga: &Saga, query: &[f32]) -> f32 {
    if saga.centroids.is_empty() {
        return f32::NEG_INFINITY;
    }
    let mut best = f32::NEG_INFINITY;
    for c in &saga.centroids {
        let s = cosine_similarity(query, &c.vector);
        if s > best {
            best = s;
        }
    }
    best
}

/// Cosine similarity. Returns 0.0 if either side has zero norm.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_similarity: dim mismatch");
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::saga::{Centroid, Saga};
    use skeg_rigging::TenantId;

    #[test]
    fn cosine_of_unit_vectors_is_dot_product() {
        let s = cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]);
        assert!((s - 1.0).abs() < 1e-6);
        let s = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(s.abs() < 1e-6);
        let s = cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]);
        assert!((s + 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_with_zero_norm_is_zero() {
        let s = cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]);
        assert_eq!(s, 0.0);
    }

    #[test]
    fn score_picks_max_over_centroids() {
        let saga = Saga {
            tenant_id: TenantId::from_bytes([1; 16]),
            built_at: 0,
            record_count: 100,
            embedding_dim: 2,
            centroids: vec![
                Centroid {
                    cluster_size: 1,
                    vector: vec![1.0, 0.0],
                },
                Centroid {
                    cluster_size: 1,
                    vector: vec![0.0, 1.0],
                },
            ],
            tags: vec![],
        };
        // Query close to first centroid wins it.
        let s = score_saga(&saga, &[0.9, 0.1]);
        assert!(s > 0.9);
    }

    #[test]
    fn empty_saga_scores_neg_inf() {
        let saga = Saga::empty(TenantId::ZERO, 4);
        let s = score_saga(&saga, &[1.0, 0.0, 0.0, 0.0]);
        assert_eq!(s, f32::NEG_INFINITY);
    }
}
