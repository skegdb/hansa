//! Saga: a member's condensed memory digest.
//!
//! A saga is the cheap "is this peer worth querying?" summary that
//! drives the membrane. Building it walks every vector in a tenant once
//! (via [`skeg_rigging::IterVectors`]), runs k-means++ on a reservoir
//! sample of those vectors, and aggregates the top tags. Persistence
//! goes through `skeg-hull`'s SagaV1 format.
//!
//! Scoring a saga against a query embedding is fast: cosine similarity
//! against every centroid, taking the max. The membrane uses that score
//! to decide how to allocate its remote-record budget.

mod build;
mod score;

pub use build::{build_saga_from_tenant, default_k_for};
pub use score::{cosine_similarity, score_saga};

use std::path::Path;

use skeg_hull::saga::{Centroid as HullCentroid, Saga as HullSaga, TagEntry as HullTag};
use skeg_rigging::TenantId;

use crate::{HansaError, Result};

/// Cluster centroid produced by the saga's k-means.
#[derive(Debug, Clone, PartialEq)]
pub struct Centroid {
    /// Number of vectors that mapped to this centroid during k-means.
    pub cluster_size: u32,
    /// Centroid vector; length == [`Saga::embedding_dim`].
    pub vector: Vec<f32>,
}

/// Aggregated tag with its occurrence count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagAggregate {
    /// Times this tag appeared across the tenant.
    pub count: u32,
    /// Tag string.
    pub tag: String,
}

/// A member's saga in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct Saga {
    /// Owner tenant id.
    pub tenant_id: TenantId,
    /// Unix seconds when this saga was built.
    pub built_at: i64,
    /// Records present at build time.
    pub record_count: u64,
    /// Embedding dim. All centroid vectors share it.
    pub embedding_dim: u32,
    /// Cluster centroids.
    pub centroids: Vec<Centroid>,
    /// Top tags with counts, sorted descending by count.
    pub tags: Vec<TagAggregate>,
}

impl Saga {
    /// Empty saga for a freshly-joined tenant that hasn't been digested
    /// yet. Scoring it returns -inf against any query.
    pub fn empty(tenant_id: TenantId, embedding_dim: u32) -> Self {
        Self {
            tenant_id,
            built_at: 0,
            record_count: 0,
            embedding_dim,
            centroids: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Convert to the hull on-disk representation.
    pub fn to_hull(&self) -> HullSaga {
        HullSaga {
            tenant_id: self.tenant_id.0,
            built_at: self.built_at,
            record_count: self.record_count,
            embedding_dim: self.embedding_dim,
            centroids: self
                .centroids
                .iter()
                .map(|c| HullCentroid {
                    cluster_size: c.cluster_size,
                    vector: c.vector.clone(),
                })
                .collect(),
            tags: self
                .tags
                .iter()
                .map(|t| HullTag {
                    count: t.count,
                    tag: t.tag.clone(),
                })
                .collect(),
        }
    }

    /// Build from a hull on-disk representation.
    pub fn from_hull(hull: HullSaga) -> Self {
        Self {
            tenant_id: TenantId(hull.tenant_id),
            built_at: hull.built_at,
            record_count: hull.record_count,
            embedding_dim: hull.embedding_dim,
            centroids: hull
                .centroids
                .into_iter()
                .map(|c| Centroid {
                    cluster_size: c.cluster_size,
                    vector: c.vector,
                })
                .collect(),
            tags: hull
                .tags
                .into_iter()
                .map(|t| TagAggregate {
                    count: t.count,
                    tag: t.tag,
                })
                .collect(),
        }
    }

    /// Atomically write to `path` via skeg-hull's SagaV1 format.
    pub fn write_to_path(&self, path: &Path) -> Result<()> {
        self.to_hull().write_to_path(path).map_err(HansaError::from)
    }

    /// Read from `path`.
    pub fn read_from_path(path: &Path) -> Result<Self> {
        let hull = HullSaga::read_from_path(path)?;
        Ok(Self::from_hull(hull))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_saga_roundtrip_via_hull() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.saga");
        let saga = Saga::empty(TenantId::from_bytes([3; 16]), 4);
        saga.write_to_path(&path).unwrap();
        let back = Saga::read_from_path(&path).unwrap();
        assert_eq!(back, saga);
    }

    #[test]
    fn populated_saga_roundtrip_via_hull() {
        let saga = Saga {
            tenant_id: TenantId::from_bytes([9; 16]),
            built_at: 1_700_000_000,
            record_count: 100,
            embedding_dim: 3,
            centroids: vec![
                Centroid {
                    cluster_size: 60,
                    vector: vec![1.0, 0.0, 0.0],
                },
                Centroid {
                    cluster_size: 40,
                    vector: vec![0.0, 1.0, 0.0],
                },
            ],
            tags: vec![
                TagAggregate {
                    count: 50,
                    tag: "code".into(),
                },
                TagAggregate {
                    count: 10,
                    tag: "design".into(),
                },
            ],
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.saga");
        saga.write_to_path(&path).unwrap();
        let back = Saga::read_from_path(&path).unwrap();
        assert_eq!(back, saga);
    }
}
