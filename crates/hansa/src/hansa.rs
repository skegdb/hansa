//! The [`Hansa`] handle: per-process entry point to a federation.
//!
//! v0.1 implements the lifecycle subset of the API:
//!
//! - [`Hansa::open`]: validate config, derive the id, build the
//!   saga directory.
//! - [`Hansa::join`] / [`Hansa::leave`]: registry events.
//! - [`Hansa::members`]: the current member list.
//! - [`Hansa::refresh_saga`]: rebuild this member's saga from its
//!   tenant.
//!
//! The membrane query path is the next slice of work and is intentionally
//! absent here; the handle is already useful for orchestrators that
//! want to spawn agents, discover peers, and serve their digests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use skeg_rigging::{CapabilityId, IterVectors, OpenError, TenantId, TenantInfo, TenantLifecycle};

use skeg_rigging::QueryFiltered;

use skeg_rigging_net::TenantLocation;

use crate::chain::{Body, replay};
use crate::manifest::ManifestStore;
use crate::membrane::{HitOrigin, MembraneHit, MembraneQuery, PeerOpener, TokenBudget};
use crate::saga::build_saga_from_tenant;
use crate::sign::Skipper;
use crate::{HansaError, HansaId, HansaKey, MemberRecord, Registry, Result, Saga};

/// Capability a tenant exposes once it has joined a hansa: an
/// orchestrator seeing this in [`TenantInfo::capabilities`] knows the
/// tenant is a hansa member. Namespace convention per
/// [`skeg_rigging::CapabilityId`].
pub const CAP_HANSA_MEMBER: CapabilityId = CapabilityId("hansa.member");
/// Capability a hansa member exposes when it can serve membrane
/// queries to peers (i.e. a peer opener is configured). Absent on a
/// local-only member.
pub const CAP_HANSA_MEMBRANE: CapabilityId = CapabilityId("hansa.membrane");

/// Discovery surface: a [`Hansa`] handle is itself a tenant from an
/// orchestrator's point of view. It delegates id / dim / record count
/// to the wrapped local tenant and extends the tenant's own
/// capabilities with [`CAP_HANSA_MEMBER`] (always) and
/// [`CAP_HANSA_MEMBRANE`] (when a peer opener is configured, so the
/// member can serve cross-tenant queries).
impl<T> TenantInfo for Hansa<T>
where
    T: TenantInfo + IterVectors + Send + Sync + 'static,
{
    fn tenant_id(&self) -> TenantId {
        self.local_tenant_id
    }

    fn embedding_dim(&self) -> u32 {
        TenantInfo::embedding_dim(&*self.local_tenant)
    }

    fn record_count(&self) -> u64 {
        TenantInfo::record_count(&*self.local_tenant)
    }

    fn capabilities(&self) -> Vec<CapabilityId> {
        let mut caps = self.local_tenant.capabilities();
        caps.push(CAP_HANSA_MEMBER);
        if self.peer_opener.is_some() {
            caps.push(CAP_HANSA_MEMBRANE);
        }
        caps
    }
}

/// Management surface for a hansa-wrapped tenant.
///
/// The scope is deliberately the *hansa layer*, not the wrapped
/// tenant's data:
///
/// - [`Self::snapshot`] delegates to the wrapped tenant's own snapshot
///   (its data) and copies the local saga digest alongside, so a
///   backup taken through this handle carries the membrane digest.
///   Membership lives in the shared registry — cluster state, not part
///   of a per-tenant snapshot.
/// - [`Self::destroy`] tears down hansa participation only (registry
///   entry + saga file). The wrapped tenant is sovereign: it owns its
///   data and its own [`TenantLifecycle`], and leaving a hansa must not
///   delete the agent's memory. Destroy the underlying tenant through
///   its own handle when that is actually intended.
impl<T> TenantLifecycle for Hansa<T>
where
    T: TenantLifecycle + IterVectors + Send + Sync + 'static,
{
    fn snapshot(&self, dest: &Path) -> std::result::Result<(), OpenError> {
        self.local_tenant.snapshot(dest)?;
        let saga = self.local_saga_path();
        if saga.exists() {
            if !dest.exists() {
                std::fs::create_dir_all(dest)?;
            }
            std::fs::copy(&saga, dest.join("hansa.saga"))?;
        }
        Ok(())
    }

    fn destroy(self: Box<Self>) -> std::result::Result<(), OpenError> {
        self.leave()
            .map_err(|e| OpenError::Io(std::io::Error::other(e.to_string())))
    }
}

/// Inputs needed to open a [`Hansa`] handle.
pub struct HansaConfig<T> {
    /// Trust group key. Scopes confidentiality / discovery; no longer
    /// derives the hansa id (that now commits to the skipper).
    pub key: HansaKey,
    /// The signing authority for this hansa.
    ///
    /// `Some` when this process founds the hansa or admits/revokes
    /// members (it holds the skipper secret). `None` for a read-only
    /// member that only queries and verifies — it cannot change
    /// membership.
    pub skipper: Option<Skipper>,
    /// The hansa id. Required for a read-only member (the id it was
    /// given out-of-band). For a founder it is optional and, if given,
    /// must equal `HansaId::from_skipper(skipper)`.
    pub hansa_id: Option<HansaId>,
    /// Registry used for member discovery (typically [`crate::FileRegistry`]).
    pub registry: Arc<dyn Registry>,
    /// Local tenant. Must implement [`IterVectors`] for saga refresh.
    pub local_tenant: Arc<T>,
    /// Tenant id (matches `local_tenant`).
    pub local_tenant_id: TenantId,
    /// Where this member's tenant is reachable from *other* members'
    /// processes. For an embedded single-machine setup this is
    /// `TenantLocation::Path { path: <tenant dir> }`; for cross-machine
    /// it's `TenantLocation::Resp3 { endpoint, auth }` or
    /// `TenantLocation::Http { base_url, bearer }`. Recorded in the
    /// registry so the [`PeerOpener`] can dispatch.
    pub local_tenant_location: TenantLocation,
    /// Directory where the *local member's* saga file is written.
    /// Typically `~/.hansa/<hansa-id>/sagas/`.
    pub saga_dir: PathBuf,
    /// Function that opens a peer tenant read-only. Supplied by the
    /// rigging adapter the user chose (e.g.
    /// `skeg_rigging_skeg::open_readonly` wrapped in a closure that
    /// matches on `TenantLocation`). `None` makes membrane queries
    /// fall back to local-only.
    pub peer_opener: Option<PeerOpener>,
    /// Default budget for [`Hansa::query`]. The query builder lets a
    /// caller override this.
    pub default_budget: TokenBudget,
    /// Async peer opener used by [`Hansa::query_async`]. Only present
    /// when the `tokio` feature is enabled. Set to `None` to leave
    /// async queries in local-only mode; set to `Some(opener)` to
    /// enable the Tokio fan-out path.
    #[cfg(feature = "tokio")]
    pub async_peer_opener: Option<crate::membrane::AsyncPeerOpener>,
}

/// Open handle to a hansa from one member's perspective.
pub struct Hansa<T> {
    id: HansaId,
    key: HansaKey,
    /// Signing authority, when this process holds it. Required to
    /// admit / revoke members; `None` for a read-only member.
    skipper: Option<Skipper>,
    registry: Arc<dyn Registry>,
    local_tenant: Arc<T>,
    local_tenant_id: TenantId,
    local_tenant_location: TenantLocation,
    saga_dir: PathBuf,
    peer_opener: Option<PeerOpener>,
    default_budget: TokenBudget,
    /// Per-peer manifest store. Bound to `<saga_dir>/../manifests`
    /// at `open` time. Reads/writes go through this; missing files
    /// are treated as neutral manifests.
    manifest_store: Arc<ManifestStore>,
    /// Async peer opener used by `query_async` (tokio feature).
    #[cfg(feature = "tokio")]
    async_peer_opener: Option<crate::membrane::AsyncPeerOpener>,
}

impl<T> Hansa<T>
where
    T: IterVectors + Send + Sync + 'static,
{
    /// Construct the handle. Creates the saga directory if missing.
    ///
    /// Founds the hansa (writes a signed genesis) when the log is empty
    /// and a skipper is held; otherwise verifies that the existing chain
    /// is well-formed and bound to this id. A read-only member (no
    /// skipper) cannot found and must point at an already-founded hansa.
    pub fn open(config: HansaConfig<T>) -> Result<Self> {
        std::fs::create_dir_all(&config.saga_dir)?;

        // Resolve the id: derived from the skipper for a founder, or the
        // out-of-band id for a read-only member.
        let id = match (&config.skipper, config.hansa_id) {
            (Some(sk), provided) => {
                let derived = HansaId::from_skipper(&sk.public());
                if let Some(p) = provided
                    && p != derived
                {
                    return Err(HansaError::IdMismatch);
                }
                derived
            }
            (None, Some(id)) => id,
            (None, None) => {
                return Err(HansaError::Invariant(
                    "hansa: need a skipper to found, or a hansa_id to join".into(),
                ));
            }
        };

        // Found (idempotent, locked) when we hold the skipper, else
        // require an already-founded chain. Then verify.
        match &config.skipper {
            Some(sk) => {
                let dim = config.local_tenant.embedding_dim();
                config
                    .registry
                    .found(id, sk, dim, current_unix_seconds())?;
            }
            None => {
                if config.registry.read_chain(id)?.is_empty() {
                    return Err(HansaError::Invariant(
                        "hansa not founded and no skipper to found it".into(),
                    ));
                }
            }
        }
        // Verifies signatures, hash chain, and the id<->skipper binding.
        replay(&config.registry.read_chain(id)?, Some(id))?;

        // Manifests live alongside the saga store under a sibling
        // `manifests/` dir. Derived rather than configured so the
        // typical caller doesn't have to plumb a second path.
        let manifest_dir = config
            .saga_dir
            .parent()
            .map(|p| p.join("manifests"))
            .unwrap_or_else(|| config.saga_dir.join("manifests"));
        let manifest_store = Arc::new(ManifestStore::new(manifest_dir));
        Ok(Self {
            id,
            key: config.key,
            skipper: config.skipper,
            registry: config.registry,
            local_tenant: config.local_tenant,
            local_tenant_id: config.local_tenant_id,
            local_tenant_location: config.local_tenant_location,
            saga_dir: config.saga_dir,
            peer_opener: config.peer_opener,
            default_budget: config.default_budget,
            manifest_store,
            #[cfg(feature = "tokio")]
            async_peer_opener: config.async_peer_opener,
        })
    }

    /// Borrow the peer manifest store for this hansa.
    pub fn manifest_store(&self) -> &Arc<ManifestStore> {
        &self.manifest_store
    }

    /// Mark every remote hit in `hits` as useful. Bumps the
    /// `useful_hits` counter (and refreshes `last_useful_at`) for the
    /// peer that produced each hit. Local hits are ignored - they
    /// don't carry peer attribution.
    ///
    /// Best effort: serialisation errors are logged but never
    /// propagated, so the caller can call this from a happy-path
    /// "user accepted the answer" callback without `?` plumbing.
    pub fn record_useful_hits(&self, hits: &[MembraneHit]) {
        use std::collections::HashMap;
        let mut by_peer: HashMap<TenantId, u64> = HashMap::new();
        for h in hits {
            if let HitOrigin::Remote { tenant_id } = h.origin {
                *by_peer.entry(tenant_id).or_insert(0) += 1;
            }
        }
        for (peer, count) in by_peer {
            self.manifest_store.bump_useful(peer, count);
        }
    }

    /// Directory where peer saga files are written.
    pub fn saga_dir(&self) -> &std::path::Path {
        &self.saga_dir
    }

    /// Clone the local tenant `Arc`. Crate-private helper used by the
    /// background refresh task.
    pub(crate) fn local_tenant_arc(&self) -> Arc<T> {
        self.local_tenant.clone()
    }

    /// Local tenant id. Crate-private helper used by the background
    /// refresh task.
    pub(crate) fn local_tenant_id(&self) -> TenantId {
        self.local_tenant_id
    }

    /// Public, non-secret id of this hansa.
    pub fn id(&self) -> HansaId {
        self.id
    }

    /// Reference to the held [`HansaKey`].
    pub fn key(&self) -> &HansaKey {
        &self.key
    }

    /// Path of this member's saga file inside `saga_dir`.
    pub fn local_saga_path(&self) -> PathBuf {
        self.saga_dir.join(format!("{}.saga", self.local_tenant_id))
    }

    /// Announce this member to the registry and ensure a saga file
    /// exists (an empty saga if the tenant is empty).
    ///
    /// `tags` is the iterator of tag strings drawn from the tenant's
    /// records. v0.1's rigging trait set does not include a record
    /// iterator with metadata; the caller wires this in from the
    /// concrete tenant type. An empty iterator is acceptable.
    pub fn join<I: IntoIterator<Item = String>>(&self, tags: I) -> Result<()> {
        // Ensure a saga file exists so peers don't fail to read.
        if !self.local_saga_path().exists() {
            self.refresh_saga(tags, current_unix_seconds(), 0)?;
        }
        let dim = self.local_tenant.embedding_dim();
        let record = MemberRecord {
            tenant_id: self.local_tenant_id,
            tenant_location: self.local_tenant_location.clone(),
            embedding_dim: dim,
            joined_at: current_unix_seconds(),
        };
        self.admit(record)
    }

    /// Admit a member: the skipper signs an `Admit` link and appends it
    /// to the chain. Skipper-only ([`HansaError::Unauthorized`] without
    /// one). The member's embedding dim must match the genesis.
    pub fn admit(&self, member: MemberRecord) -> Result<()> {
        let skipper = self.skipper.as_ref().ok_or(HansaError::Unauthorized)?;
        let head = replay(&self.registry.read_chain(self.id)?, Some(self.id))?;
        if member.embedding_dim != head.embedding_dim {
            return Err(HansaError::DimMismatch {
                existing: head.embedding_dim,
                joining: member.embedding_dim,
            });
        }
        self.registry.append_next(
            self.id,
            skipper,
            Body::Admit {
                member,
                member_pub: None,
            },
        )
    }

    /// Remove this member: revoke its own tenant and delete its local
    /// saga file (privacy default).
    pub fn leave(&self) -> Result<()> {
        self.revoke(self.local_tenant_id)?;
        let path = self.local_saga_path();
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Revoke a member by tenant id: the skipper signs a `Revoke` link.
    /// Skipper-only. The revoked tenant drops from the active set on the
    /// next replay, so honest peers stop serving it immediately.
    pub fn revoke(&self, tenant: TenantId) -> Result<()> {
        let skipper = self.skipper.as_ref().ok_or(HansaError::Unauthorized)?;
        self.registry.append_next(
            self.id,
            skipper,
            Body::Revoke {
                tenant_id: tenant,
                at: current_unix_seconds(),
                reason: None,
            },
        )
    }

    /// All currently-active members of this hansa.
    pub fn members(&self) -> Result<Vec<MemberRecord>> {
        self.registry.members(self.id)
    }

    /// Collapse the members log to a signed checkpoint, bounding its
    /// growth. Skipper-only. `append_next` also does this automatically
    /// once the log grows large.
    pub fn compact(&self) -> Result<()> {
        let skipper = self.skipper.as_ref().ok_or(HansaError::Unauthorized)?;
        self.registry.compact(self.id, skipper)
    }

    /// Rebuild the local saga from the tenant and persist it atomically
    /// via skeg-hull. `tags` is the iterator of tag strings (one per
    /// tenant record, with repetitions). `seed` controls the reservoir
    /// sampler for deterministic test outcomes.
    pub fn refresh_saga<I: IntoIterator<Item = String>>(
        &self,
        tags: I,
        built_at: i64,
        seed: u64,
    ) -> Result<()> {
        let dim = self.local_tenant.embedding_dim();
        let count = self.local_tenant.record_count();
        let vectors = self
            .local_tenant
            .iter_vectors()
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        let saga = build_saga_from_tenant(
            self.local_tenant_id,
            dim,
            count,
            vectors,
            tags,
            built_at,
            seed,
        )?;
        saga.write_to_path(&self.local_saga_path())?;
        Ok(())
    }

    /// Load any peer's saga file from `saga_dir`. Returns `None` if the
    /// saga is not present yet (peer just joined, hasn't refreshed).
    pub fn load_peer_saga(&self, peer_tenant: TenantId) -> Result<Option<Saga>> {
        let path = self.saga_dir.join(format!("{peer_tenant}.saga"));
        if !path.exists() {
            return Ok(None);
        }
        let saga = Saga::read_from_path(&path)?;
        Ok(Some(saga))
    }
}

impl<T> Hansa<T>
where
    T: IterVectors + QueryFiltered + Send + Sync + 'static,
{
    /// Build a membrane query for `embedding`. Configure it with the
    /// builder methods on [`MembraneQuery`], then call `.execute()`.
    ///
    /// If no [`PeerOpener`] was provided at config time, the query
    /// silently falls back to local-only.
    pub fn query<'a>(&'a self, embedding: &[f32]) -> Result<MembraneQuery<'a, T>> {
        // Snapshot members + peer sagas.
        let members = self.registry.members(self.id)?;
        let mut sagas: Vec<(MemberRecord, Saga)> = Vec::with_capacity(members.len());
        for m in &members {
            if m.tenant_id == self.local_tenant_id {
                continue;
            }
            if let Some(saga) = self.load_peer_saga(m.tenant_id)? {
                sagas.push((m.clone(), saga));
            }
        }

        let (peer_opener, local_only) = match &self.peer_opener {
            Some(o) => (o.clone(), false),
            None => (placeholder_opener(), true),
        };

        Ok(MembraneQuery {
            local_tenant: self.local_tenant.as_ref(),
            local_tenant_id: self.local_tenant_id,
            peer_opener,
            embedding: embedding.to_vec(),
            members,
            sagas,
            top_k: 10,
            budget: self.default_budget,
            min_similarity: f32::NEG_INFINITY,
            local_only,
            deadline: None,
            manifest_store: self.manifest_store.clone(),
        })
    }
}

#[cfg(feature = "tokio")]
impl<T> Hansa<T>
where
    T: IterVectors + QueryFiltered + Send + Sync + 'static,
{
    /// Async counterpart of [`Self::query`]. Returns a builder that
    /// fans out peer queries via `tokio::spawn` instead of
    /// `std::thread`.
    ///
    /// Requires the `tokio` feature. The caller must already be
    /// running under a Tokio runtime; building the query is
    /// synchronous, only `execute()` is async.
    ///
    /// If no [`crate::membrane::AsyncPeerOpener`] was provided via
    /// [`crate::HansaConfig::async_peer_opener`], the query falls back
    /// to local-only.
    pub fn query_async<'a>(
        &'a self,
        embedding: &[f32],
    ) -> Result<crate::membrane::MembraneQueryAsync<'a, T>> {
        let members = self.registry.members(self.id)?;
        let mut sagas: Vec<(MemberRecord, Saga)> = Vec::with_capacity(members.len());
        for m in &members {
            if m.tenant_id == self.local_tenant_id {
                continue;
            }
            if let Some(saga) = self.load_peer_saga(m.tenant_id)? {
                sagas.push((m.clone(), saga));
            }
        }

        let (peer_opener, local_only) = match &self.async_peer_opener {
            Some(o) => (o.clone(), false),
            None => (async_placeholder_opener(), true),
        };

        Ok(crate::membrane::MembraneQueryAsync {
            local_tenant: self.local_tenant.as_ref(),
            local_tenant_id: self.local_tenant_id,
            peer_opener,
            embedding: embedding.to_vec(),
            members,
            sagas,
            top_k: 10,
            budget: self.default_budget,
            min_similarity: f32::NEG_INFINITY,
            local_only,
            deadline: None,
            manifest_store: self.manifest_store.clone(),
        })
    }
}

#[cfg(feature = "tokio")]
fn async_placeholder_opener() -> crate::membrane::AsyncPeerOpener {
    Arc::new(|_tid, _loc| {
        Box::pin(async { Err(skeg_rigging::OpenError::NotFound) })
    })
}

fn placeholder_opener() -> PeerOpener {
    Arc::new(|_tid, _loc| Err(skeg_rigging::OpenError::NotFound))
}

fn current_unix_seconds() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileRegistry;
    use skeg_rigging::RecordId;
    use std::path::Path;

    /// Minimal in-test tenant: just IterVectors, no QueryFiltered.
    struct StubTenant {
        id: TenantId,
        dim: u32,
        records: Vec<(RecordId, Vec<f32>)>,
    }

    impl IterVectors for StubTenant {
        fn iter_vectors(&self) -> Box<dyn Iterator<Item = (RecordId, Vec<f32>)> + '_> {
            Box::new(self.records.iter().map(|(id, v)| (*id, v.clone())))
        }
        fn record_count(&self) -> u64 {
            self.records.len() as u64
        }
        fn embedding_dim(&self) -> u32 {
            self.dim
        }
    }

    impl TenantInfo for StubTenant {
        fn tenant_id(&self) -> TenantId {
            self.id
        }
        fn embedding_dim(&self) -> u32 {
            self.dim
        }
        fn record_count(&self) -> u64 {
            self.records.len() as u64
        }
        fn capabilities(&self) -> Vec<CapabilityId> {
            vec![skeg_rigging::CAP_VECTOR_KV]
        }
    }

    impl TenantLifecycle for StubTenant {
        fn snapshot(&self, dest: &Path) -> std::result::Result<(), OpenError> {
            std::fs::create_dir_all(dest)?;
            std::fs::write(dest.join("tenant.stub"), b"stub")?;
            Ok(())
        }
        fn destroy(self: Box<Self>) -> std::result::Result<(), OpenError> {
            Ok(())
        }
    }

    fn open_handle(tmpdir: &Path, tenant_seed: u8) -> (Hansa<StubTenant>, HansaId) {
        let key = HansaKey::from_bytes([7; 32]);
        // Deterministic skipper so reopening the same tempdir resolves to
        // the same hansa id (and the same founded chain).
        let skipper = Skipper::from_seed([7u8; 32]);
        let id = HansaId::from_skipper(&skipper.public());
        let saga_dir = tmpdir.join(id.as_hex()).join("sagas");
        let registry = Arc::new(FileRegistry::new(tmpdir));
        let tenant = Arc::new(StubTenant {
            id: TenantId::from_bytes([tenant_seed; 16]),
            dim: 3,
            records: (0..20)
                .map(|i| {
                    let x = (i as f32) * 0.05;
                    (RecordId(i as u64), vec![x, 1.0 - x, 0.5])
                })
                .collect(),
        });
        let handle = Hansa::open(HansaConfig {
            key: key.clone(),
            skipper: Some(skipper),
            hansa_id: Some(id),
            registry,
            local_tenant: tenant.clone(),
            local_tenant_id: tenant.id,
            local_tenant_location: TenantLocation::Path {
                path: tmpdir.join(format!("tenant-{tenant_seed}")),
            },
            saga_dir,
            peer_opener: None,
            default_budget: crate::membrane::TokenBudget::default(),
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
        })
        .unwrap();
        (handle, id)
    }

    #[test]
    fn join_writes_saga_and_registers() {
        let dir = tempfile::tempdir().unwrap();
        let (h, hid) = open_handle(dir.path(), 1);
        h.join(Vec::<String>::new()).unwrap();
        // Member is in registry.
        let members = h.members().unwrap();
        assert_eq!(members.len(), 1);
        // Saga file present.
        let saga_path = h.local_saga_path();
        assert!(saga_path.exists(), "saga not at {saga_path:?}");
        // Hansa id matches what the registry observed.
        assert_eq!(hid, h.id());
    }

    #[test]
    fn refresh_saga_overwrites_previous() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 2);
        h.join(Vec::<String>::new()).unwrap();
        let first = Saga::read_from_path(&h.local_saga_path()).unwrap();
        h.refresh_saga(vec!["code".to_string()], first.built_at + 100, 99)
            .unwrap();
        let second = Saga::read_from_path(&h.local_saga_path()).unwrap();
        assert_eq!(second.built_at, first.built_at + 100);
        // Tags changed.
        assert_eq!(second.tags.len(), 1);
        assert_eq!(second.tags[0].tag, "code");
    }

    #[test]
    fn leave_removes_saga_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 3);
        h.join(Vec::<String>::new()).unwrap();
        assert_eq!(h.members().unwrap().len(), 1);
        h.leave().unwrap();
        assert_eq!(h.members().unwrap().len(), 0);
        assert!(!h.local_saga_path().exists());
    }

    #[test]
    fn tenant_info_delegates_and_adds_hansa_member_cap() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 5);
        // Delegation to the wrapped tenant.
        assert_eq!(TenantInfo::tenant_id(&h), TenantId::from_bytes([5; 16]));
        assert_eq!(TenantInfo::embedding_dim(&h), 3);
        assert_eq!(TenantInfo::record_count(&h), 20);
        // Base cap preserved, hansa.member added. peer_opener is None
        // in open_handle, so hansa.membrane must be absent.
        let caps = TenantInfo::capabilities(&h);
        assert!(caps.contains(&skeg_rigging::CAP_VECTOR_KV));
        assert!(caps.contains(&CAP_HANSA_MEMBER));
        assert!(!caps.contains(&CAP_HANSA_MEMBRANE));
    }

    #[test]
    fn snapshot_captures_tenant_data_and_saga() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 6);
        h.join(Vec::<String>::new()).unwrap();
        let dest = dir.path().join("backup");
        TenantLifecycle::snapshot(&h, &dest).unwrap();
        // Wrapped tenant's own snapshot ran.
        assert!(dest.join("tenant.stub").exists());
        // Hansa-layer saga digest copied alongside.
        assert!(dest.join("hansa.saga").exists());
    }

    #[test]
    fn destroy_leaves_hansa_without_touching_tenant() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 7);
        h.join(Vec::<String>::new()).unwrap();
        assert_eq!(h.members().unwrap().len(), 1);
        let saga = h.local_saga_path();
        assert!(saga.exists());
        Box::new(h).destroy().unwrap();
        // Hansa participation gone: saga removed. Re-open a fresh handle
        // against the same registry to confirm membership dropped.
        let (h2, _) = open_handle(dir.path(), 7);
        assert_eq!(h2.members().unwrap().len(), 0);
        assert!(!saga.exists());
    }

    #[test]
    fn load_peer_saga_returns_none_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let (h, _) = open_handle(dir.path(), 4);
        let other = TenantId::from_bytes([99; 16]);
        let r = h.load_peer_saga(other).unwrap();
        assert!(r.is_none());
    }
}
