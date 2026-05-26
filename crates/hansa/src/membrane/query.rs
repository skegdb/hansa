//! The membrane query pipeline.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use skeg_rigging::{
    Filter, IterVectors, OpenError, QueryFiltered, ReadOnlyView, RecordId, RecordMeta, TenantId,
};
use skeg_rigging_net::TenantLocation;

use crate::manifest::ManifestStore;
use crate::membrane::allocation::{DEFAULT_MAX_PEERS, proportional_allocation};
use crate::membrane::budget::TokenBudget;
use crate::saga::score_saga;
use crate::{HansaError, MemberRecord, Result, Saga};

/// Where a hit came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitOrigin {
    /// Found in the caller's own tenant.
    Local,
    /// Returned by a peer in the same hansa.
    Remote {
        /// Peer tenant id.
        tenant_id: TenantId,
    },
}

/// Diagnostics from a membrane query: counts that the caller may want
/// to surface in dashboards or use to decide whether to retry.
///
/// Returned alongside the hits by [`MembraneQuery::execute_with_stats`].
/// The plain [`MembraneQuery::execute`] discards them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembraneStats {
    /// Peers the fan-out actually launched a worker for.
    pub peers_attempted: usize,
    /// Peers whose worker returned (success or peer-side error) before
    /// the deadline expired.
    pub peers_completed: usize,
    /// Peers whose worker had not returned by the time the deadline
    /// hit; their would-be hits were not included in the result.
    /// Equal to `peers_attempted - peers_completed` and surfaced
    /// explicitly so callers don't have to subtract.
    pub dropped_for_deadline: usize,
}

/// A hit returned by a membrane query.
#[derive(Debug, Clone)]
pub struct MembraneHit {
    /// Hit record identifier.
    pub record_id: RecordId,
    /// Similarity score from the producing tenant.
    pub similarity: f32,
    /// Origin: local vs remote (with peer id).
    pub origin: HitOrigin,
    /// Record payload.
    pub payload: Bytes,
    /// Vector that produced this hit, propagated from
    /// [`skeg_rigging::Hit::embedding`] when the producing backend
    /// supplies it. Local hits and on-disk peers carry it; RESP3 /
    /// HTTP peers leave this `None`. Downstream `ContextBuilder`'s
    /// semantic dedup uses it when present and falls back to
    /// byte/sentence dedup when `None`.
    pub embedding: Option<Vec<f32>>,
}

/// Function that opens a peer tenant read-only, dispatching on the
/// peer's [`TenantLocation`]. Receives both the peer's `tenant_id`
/// (needed by RESP3 / HTTP transports to scope the connection) and
/// the location enum.
///
/// Typical wiring for a single-transport setup (filesystem only):
///
/// ```ignore
/// let opener: PeerOpener = Arc::new(|_tid, loc| match loc {
///     TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
///     _ => Err(OpenError::NotFound),
/// });
/// ```
///
/// Multi-transport setup:
///
/// ```ignore
/// let opener: PeerOpener = Arc::new(|tid, loc| match loc {
///     TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
///     TenantLocation::Resp3 { endpoint, auth } => {
///         let conn = Resp3Connection::connect(endpoint, auth.as_deref())?;
///         let t = Resp3Tenant::from_connection(conn, tid, "hansa")?;
///         Ok(Box::new(t))
///     }
///     TenantLocation::Http { .. } => Err(OpenError::NotFound),
/// });
/// ```
pub type PeerOpener = Arc<
    dyn Fn(TenantId, &TenantLocation) -> std::result::Result<Box<dyn ReadOnlyView>, OpenError>
        + Send
        + Sync,
>;

/// One-shot membrane query.
///
/// Construction is private to [`crate::Hansa::query`]. The builder is
/// configured with `top_k`, [`TokenBudget`], optional minimum
/// similarity threshold, and a `local_only` flag.
pub struct MembraneQuery<'a, T> {
    pub(crate) local_tenant: &'a T,
    pub(crate) local_tenant_id: TenantId,
    pub(crate) peer_opener: PeerOpener,
    pub(crate) embedding: Vec<f32>,
    pub(crate) members: Vec<MemberRecord>,
    pub(crate) sagas: Vec<(MemberRecord, Saga)>,
    pub(crate) top_k: u32,
    pub(crate) budget: TokenBudget,
    pub(crate) min_similarity: f32,
    pub(crate) local_only: bool,
    /// Wall-clock cap on remote fan-out, measured from the moment the
    /// peer workers are spawned. `None` waits for every peer.
    pub(crate) deadline: Option<Duration>,
    /// Peer manifests (F.5): biases per-peer saga scores by past
    /// usefulness. The membrane reads them when scoring and bumps
    /// `total_hits` after fan-out.
    pub(crate) manifest_store: Arc<ManifestStore>,
}

impl<'a, T> MembraneQuery<'a, T>
where
    T: IterVectors + QueryFiltered + Send + Sync,
{
    /// All members of this hansa as seen at construction time
    /// (including the local one). Useful for diagnostics: a caller
    /// can check `.members().len()` to know whether the membrane
    /// has anything to fan out to before paying for the query.
    pub fn members(&self) -> &[MemberRecord] {
        &self.members
    }

    /// Peer sagas the membrane will score (excludes the local tenant
    /// and any peer whose saga file was missing).
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

    /// Cap wall-clock time for the remote fan-out. The local query
    /// always runs to completion; the deadline only applies to the
    /// peer workers spawned in step 4 of the pipeline.
    ///
    /// When the deadline expires, in-flight peers are abandoned (their
    /// threads continue to run to completion in the background but
    /// their results are dropped) and the number of unfinished peers
    /// is surfaced as [`MembraneStats::dropped_for_deadline`] when the
    /// caller uses [`Self::execute_with_stats`].
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = Some(d);
        self
    }

    /// Run the query, discarding [`MembraneStats`].
    pub fn execute(self) -> Result<Vec<MembraneHit>> {
        self.execute_with_stats().map(|(hits, _)| hits)
    }

    /// Run the query and return the diagnostics alongside the hits.
    pub fn execute_with_stats(self) -> Result<(Vec<MembraneHit>, MembraneStats)> {
        let mut stats = MembraneStats::default();

        // 1. Local query. Local visibility is total: no shareable filter.
        let local_hits = self
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
            .collect::<Vec<_>>();

        if self.local_only {
            return Ok((
                truncate_to_budget(local_hits, self.budget.max_total_records),
                stats,
            ));
        }

        // 2. Score peer sagas, biased by per-peer manifest (F.5):
        //    `final = saga_score × manifest.usefulness_factor(now)`.
        //    Missing / never-useful manifests give factor 1.0 so this
        //    is a no-op on a cold federation.
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

        // 4. Fan out filtered queries on dedicated threads. mpsc carries
        //    the per-peer hit batches back. The deadline (if any) is
        //    enforced via `recv_timeout`; peers whose work is still
        //    pending at expiry are counted in `dropped_for_deadline`
        //    and their threads are left to run in the background.
        let opener = self.peer_opener.clone();
        let embedding = Arc::new(self.embedding);
        let (tx, rx) = mpsc::channel::<Vec<MembraneHit>>();
        let peers_attempted = allocation.len();
        stats.peers_attempted = peers_attempted;
        for alloc in allocation {
            let tx = tx.clone();
            let opener = opener.clone();
            let embedding = embedding.clone();
            let member = alloc.member.clone();
            let budget = alloc.budget;
            thread::spawn(move || {
                let hits = match open_and_query(&opener, &embedding, &member, budget) {
                    Ok(hits) => hits,
                    Err(e) => {
                        // Peer failure is not fatal: log + skip.
                        tracing_log(&member.tenant_id, &e);
                        Vec::new()
                    }
                };
                // Ignore SendError: the main thread may have already
                // moved on past the deadline and dropped the receiver.
                let _ = tx.send(hits);
            });
        }
        // Drop the local end so `recv` errors out once every worker
        // hand has been dropped.
        drop(tx);

        let mut remote_hits: Vec<MembraneHit> = Vec::new();
        let start = Instant::now();
        let deadline = self.deadline;
        for _ in 0..peers_attempted {
            let recv = match deadline {
                Some(d) => {
                    let elapsed = start.elapsed();
                    if elapsed >= d {
                        break;
                    }
                    rx.recv_timeout(d - elapsed)
                }
                None => rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };
            match recv {
                Ok(batch) => {
                    stats.peers_completed += 1;
                    remote_hits.extend(batch);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                // All workers finished and the channel hung up; we are done.
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        stats.dropped_for_deadline = peers_attempted - stats.peers_completed;

        // 5. Update manifests: every peer that returned hits in this
        //    fan-out gets its `total_hits` counter bumped by the
        //    number of hits it produced. Useful counters are bumped
        //    later via `Hansa::record_useful_hits` when the caller
        //    flags a hit as accepted.
        {
            use std::collections::HashMap;
            let mut hit_count: HashMap<TenantId, u64> = HashMap::new();
            for h in &remote_hits {
                if let HitOrigin::Remote { tenant_id } = h.origin {
                    *hit_count.entry(tenant_id).or_insert(0) += 1;
                }
            }
            for (peer, count) in hit_count {
                self.manifest_store.bump_total(peer, count);
            }
        }

        // 6. Merge + truncate.
        let mut all: Vec<MembraneHit> = local_hits;
        all.extend(remote_hits);
        all.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        Ok((truncate_to_budget(all, self.budget.max_total_records), stats))
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn open_and_query(
    opener: &PeerOpener,
    embedding: &[f32],
    member: &MemberRecord,
    budget: u32,
) -> Result<Vec<MembraneHit>> {
    let view = opener(member.tenant_id, &member.tenant_location).map_err(HansaError::from)?;
    let hits = view
        .query_filtered(embedding, budget, &ShareableOnly)
        .map_err(HansaError::from)?;
    let tenant_id = view.tenant_id();
    let _ = view.close();
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

// ============ filters ============

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

// Log a peer failure without taking a hard dep on `tracing` (kept
// minimal until v0.2 wires in proper structured logging).
fn tracing_log(peer: &TenantId, err: &HansaError) {
    eprintln!("hansa: peer {peer} unavailable: {err}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membrane::TokenBudget;
    use crate::membrane::allocation::PeerAllocation;
    use std::sync::Arc;

    /// Membrane query smoke tests live alongside the full Hansa
    /// integration tests in `tests/membrane_integration.rs` because they
    /// need a concrete vector backend.
    #[test]
    fn token_budget_default_is_balanced() {
        let b = TokenBudget::default();
        assert!(b.max_remote_records > 0);
        assert!(b.max_total_records >= b.max_remote_records);
    }

    #[test]
    fn peer_allocation_struct_is_constructible() {
        // Smoke construction so changes to the public surface trip a
        // compile failure.
        let a = PeerAllocation {
            member: MemberRecord {
                tenant_id: TenantId::ZERO,
                tenant_location: TenantLocation::Path {
                    path: std::path::PathBuf::from("/x"),
                },
                embedding_dim: 1,
                joined_at: 0,
            },
            budget: 5,
        };
        assert_eq!(a.budget, 5);
        let _ = Arc::new(a);
    }
}
