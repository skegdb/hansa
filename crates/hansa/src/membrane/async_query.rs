//! Async variant of the membrane query path (F.6).
//!
//! Mirrors [`crate::membrane::MembraneQuery`] but fans out peer
//! queries via `tokio::spawn` instead of `std::thread`. Use this
//! when the caller already runs under a Tokio runtime - production
//! agent frameworks (axum, async-graphql, langchain-rs-style hosts)
//! all need it.
//!
//! ## Local vs remote
//!
//! The local query stays synchronous: it runs on the caller's
//! task. Local queries are typically file-mmap fast (~µs); paying
//! the await + blocking-pool dispatch for one local hop would
//! dominate the actual query work. Remote queries are where
//! concurrency wins, so those go through `tokio::spawn`.
//!
//! ## Deadline
//!
//! Same shape as the sync path: `deadline(Duration)` clamps remote
//! fan-out. Internally implemented via `tokio::time::timeout` on
//! the gather phase. Peers whose tasks are still pending at the
//! deadline are abandoned (Tokio joins them eventually, but their
//! results are not waited on); count surfaces as
//! [`crate::MembraneStats::dropped_for_deadline`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use skeg_rigging::asyncs::AsyncReadOnlyView;
use skeg_rigging::{
    Filter, IterVectors, OpenError, QueryFiltered, RecordMeta, TenantId,
};
use skeg_rigging_net::TenantLocation;

use crate::manifest::ManifestStore;
use crate::membrane::allocation::{DEFAULT_MAX_PEERS, proportional_allocation};
use crate::membrane::budget::TokenBudget;
use crate::membrane::{HitOrigin, MembraneHit, MembraneStats};
use crate::saga::score_saga;
use crate::{HansaError, MemberRecord, Result, Saga};

/// Boxed future returned by [`AsyncPeerOpener`]. `Send + 'static` so
/// the resulting task can be moved onto any tokio worker.
pub type PeerOpenFuture =
    Pin<Box<dyn Future<Output = std::result::Result<Box<dyn AsyncReadOnlyView>, OpenError>> + Send>>;

/// Async counterpart of [`crate::membrane::PeerOpener`]. Returns a
/// future that resolves to an [`AsyncReadOnlyView`].
///
/// Note: takes `TenantLocation` **by value**, not by reference, so
/// the future can move ownership across the await boundary without
/// borrowing the caller's stack. The sync `PeerOpener` takes a
/// reference because its closure returns immediately; the async
/// path needs `'static` captures.
pub type AsyncPeerOpener = Arc<dyn Fn(TenantId, TenantLocation) -> PeerOpenFuture + Send + Sync>;

/// Async one-shot membrane query.
///
/// Construction is via [`crate::Hansa::query_async`]. Builder
/// methods mirror the sync [`crate::membrane::MembraneQuery`].
pub struct MembraneQueryAsync<'a, T> {
    pub(crate) local_tenant: &'a T,
    pub(crate) local_tenant_id: TenantId,
    pub(crate) peer_opener: AsyncPeerOpener,
    pub(crate) embedding: Vec<f32>,
    pub(crate) members: Vec<MemberRecord>,
    pub(crate) sagas: Vec<(MemberRecord, Saga)>,
    pub(crate) top_k: u32,
    pub(crate) budget: TokenBudget,
    pub(crate) min_similarity: f32,
    pub(crate) local_only: bool,
    pub(crate) deadline: Option<Duration>,
    pub(crate) manifest_store: Arc<ManifestStore>,
}

impl<'a, T> MembraneQueryAsync<'a, T>
where
    T: IterVectors + QueryFiltered + Send + Sync,
{
    /// All members seen at construction time.
    pub fn members(&self) -> &[MemberRecord] {
        &self.members
    }

    /// Peer sagas the membrane will score.
    pub fn peer_sagas(&self) -> &[(MemberRecord, Saga)] {
        &self.sagas
    }

    /// Set the number of hits returned by the local query.
    pub fn top_k(mut self, k: u32) -> Self {
        self.top_k = k;
        self
    }

    /// Override the token budget.
    pub fn budget(mut self, b: TokenBudget) -> Self {
        self.budget = b;
        self
    }

    /// Drop peer sagas whose score falls below this threshold.
    pub fn min_similarity(mut self, threshold: f32) -> Self {
        self.min_similarity = threshold;
        self
    }

    /// Skip peer fan-out; query only the local tenant.
    pub fn local_only(mut self) -> Self {
        self.local_only = true;
        self
    }

    /// Cap wall-clock time for the remote fan-out.
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = Some(d);
        self
    }

    /// Run the query, discarding [`MembraneStats`].
    pub async fn execute(self) -> Result<Vec<MembraneHit>> {
        self.execute_with_stats().await.map(|(hits, _)| hits)
    }

    /// Run the query and return diagnostics alongside the hits.
    pub async fn execute_with_stats(self) -> Result<(Vec<MembraneHit>, MembraneStats)> {
        let mut stats = MembraneStats::default();

        // 1. Local query (sync; file-mmap fast).
        let local_hits: Vec<MembraneHit> = self
            .local_tenant
            .query_filtered(&self.embedding, self.top_k, &AcceptAll)?
            .into_iter()
            .map(|h| MembraneHit {
                record_id: h.record_id,
                similarity: h.similarity,
                origin: HitOrigin::Local,
                payload: h.payload,
                embedding: h.embedding,
            })
            .collect();

        if self.local_only {
            return Ok((
                truncate_to_budget(local_hits, self.budget.max_total_records),
                stats,
            ));
        }

        // 2. Score peer sagas + manifest bias.
        let now = unix_seconds();
        let manifests = self.manifest_store.clone();
        let mut scored: Vec<(MemberRecord, f32)> = self
            .sagas
            .into_iter()
            .filter(|(m, _)| m.tenant_id != self.local_tenant_id)
            .map(|(m, saga)| {
                let base = score_saga(&saga, &self.embedding);
                let factor = manifests.read(m.tenant_id).usefulness_factor(now);
                (m, base * factor)
            })
            .filter(|(_, s)| s.is_finite() && *s >= self.min_similarity)
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // 3. Allocate remote budget.
        let allocation = proportional_allocation(
            scored,
            self.budget.max_remote_records,
            1,
            DEFAULT_MAX_PEERS,
        );
        let peers_attempted = allocation.len();
        stats.peers_attempted = peers_attempted;

        // 4. Fan out: spawn one tokio task per peer.
        let opener = self.peer_opener.clone();
        let embedding = Arc::new(self.embedding);
        let mut handles = Vec::with_capacity(peers_attempted);
        for alloc in allocation {
            let opener = opener.clone();
            let embedding = embedding.clone();
            let member = alloc.member.clone();
            let budget = alloc.budget;
            handles.push(tokio::spawn(async move {
                match open_and_query_async(&opener, &embedding, &member, budget).await {
                    Ok(hits) => hits,
                    Err(e) => {
                        eprintln!("hansa(async): peer {} unavailable: {e}", member.tenant_id);
                        Vec::new()
                    }
                }
            }));
        }

        // 5. Collect with optional deadline.
        let mut remote_hits: Vec<MembraneHit> = Vec::new();
        let collect = async {
            for h in handles.drain(..) {
                match h.await {
                    Ok(hits) => {
                        stats.peers_completed += 1;
                        remote_hits.extend(hits);
                    }
                    Err(_) => {
                        // task panicked or was cancelled; treat as a
                        // failed peer (the spawned closure already
                        // swallows opener errors).
                    }
                }
            }
        };
        match self.deadline {
            Some(d) => {
                let _ = tokio::time::timeout(d, collect).await;
            }
            None => {
                collect.await;
            }
        }
        stats.dropped_for_deadline = peers_attempted - stats.peers_completed;

        // 6. Update manifest totals.
        let mut hit_count: HashMap<TenantId, u64> = HashMap::new();
        for h in &remote_hits {
            if let HitOrigin::Remote { tenant_id } = h.origin {
                *hit_count.entry(tenant_id).or_insert(0) += 1;
            }
        }
        for (peer, count) in hit_count {
            self.manifest_store.bump_total(peer, count);
        }

        // 7. Merge + truncate.
        let mut all: Vec<MembraneHit> = local_hits;
        all.extend(remote_hits);
        all.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        Ok((truncate_to_budget(all, self.budget.max_total_records), stats))
    }
}

async fn open_and_query_async(
    opener: &AsyncPeerOpener,
    embedding: &[f32],
    member: &MemberRecord,
    budget: u32,
) -> Result<Vec<MembraneHit>> {
    let view: Box<dyn AsyncReadOnlyView> = opener(member.tenant_id, member.tenant_location.clone())
        .await
        .map_err(HansaError::from)?;
    let filter: Arc<dyn Filter> = Arc::new(ShareableOnly);
    let hits = view
        .query_filtered_async(embedding, budget, filter)
        .await
        .map_err(HansaError::from)?;
    let tenant_id = view.tenant_id_async().await;
    let _ = view.close_async().await;
    Ok(hits
        .into_iter()
        .map(|h| MembraneHit {
            record_id: h.record_id,
            similarity: h.similarity,
            origin: HitOrigin::Remote { tenant_id },
            payload: h.payload,
            embedding: h.embedding,
        })
        .collect())
}

fn truncate_to_budget(mut hits: Vec<MembraneHit>, total_cap: u32) -> Vec<MembraneHit> {
    hits.truncate(total_cap as usize);
    hits
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

struct AcceptAll;
impl Filter for AcceptAll {
    fn accept(&self, _meta: &RecordMeta<'_>) -> bool {
        true
    }
}

struct ShareableOnly;
impl Filter for ShareableOnly {
    fn accept(&self, meta: &RecordMeta<'_>) -> bool {
        meta.shareable
    }
}
