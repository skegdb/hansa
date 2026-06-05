//! `HybridRegistry`: composes a local `FileRegistry` with N remote
//! HTTP peers so a hansa can span machines.
//!
//! ## Model
//!
//! - **Writes** (`join`, `leave`) go to the local `FileRegistry` only.
//!   Each agent owns its own registry; remote peers see new members
//!   when they re-poll.
//! - **Reads** (`members`) merge the local registry with each remote
//!   peer's `members.snap` fetched via HTTP. Tenants are deduplicated
//!   by `tenant_id`; a remote entry never overrides a local one.
//! - **Compaction** is the local `FileRegistry`'s responsibility;
//!   remotes are pulled fresh on every read (cache layer is a future
//!   addition).
//!
//! ## Failure handling
//!
//! A remote peer that returns an error (unreachable, malformed JSON)
//! is logged to stderr and skipped - the same "best effort" policy
//! the membrane already uses for query fan-out. The local members
//! always survive.

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use skeg_rigging::TenantId;
use skeg_rigging_net_http::{SagaClient, fetch_to_path};

use crate::chain::{Body, Link};
use crate::sign::Skipper;
use crate::{FileRegistry, HansaError, HansaId, MemberRecord, Registry, Result};

/// Local-plus-remote member registry.
pub struct HybridRegistry {
    local: FileRegistry,
    remotes: RwLock<Vec<RemotePeer>>,
}

struct RemotePeer {
    /// Base URL of the peer's `SagaServer` with members enabled.
    base_url: String,
    /// Cached `SagaClient`. Cheap to construct, cheaper to keep.
    client: Arc<SagaClient>,
}

impl HybridRegistry {
    /// Construct from a local `FileRegistry`. No remotes yet - add
    /// them with `add_remote`.
    pub fn new(local: FileRegistry) -> Self {
        Self {
            local,
            remotes: RwLock::new(Vec::new()),
        }
    }

    /// Add a remote peer base URL (e.g. `"http://peer-b:9000"`).
    /// Idempotent: adding the same URL twice has no effect.
    pub fn add_remote(&self, base_url: impl Into<String>) {
        let url = base_url.into();
        let mut peers = self.remotes.write();
        if peers.iter().any(|p| p.base_url == url) {
            return;
        }
        peers.push(RemotePeer {
            base_url: url.clone(),
            client: Arc::new(SagaClient::new(url)),
        });
    }

    /// Drop a remote peer.
    pub fn remove_remote(&self, base_url: &str) {
        self.remotes.write().retain(|p| p.base_url != base_url);
    }

    /// Number of remotes currently configured.
    pub fn remote_count(&self) -> usize {
        self.remotes.read().len()
    }

    /// Borrow the local registry - useful if the caller needs to call
    /// `compact` directly.
    pub fn local(&self) -> &FileRegistry {
        &self.local
    }

    /// Pull members from one named remote. Exposed for tests and for
    /// debug tooling that wants to inspect a single peer.
    pub fn fetch_remote_members(
        &self,
        base_url: &str,
        hansa: HansaId,
    ) -> Result<Vec<MemberRecord>> {
        let peers = self.remotes.read();
        let peer = peers
            .iter()
            .find(|p| p.base_url == base_url)
            .ok_or_else(|| {
                HansaError::Invariant(format!("unknown remote: {base_url}"))
            })?;
        fetch_one(peer, hansa).map_err(|e| HansaError::Invariant(format!("{e}")))
    }

    /// Fetch every saga listed by every remote peer into `saga_dir`,
    /// skipping files whose local mtime already matches or exceeds
    /// the remote's `last_modified`. Returns the number of sagas
    /// newly downloaded.
    ///
    /// Call this before [`crate::Hansa::query`] so the membrane's
    /// peer-scoring step finds the latest sagas on disk. Failures
    /// against an individual peer log to stderr and skip; the method
    /// still returns successfully for the peers that worked.
    pub fn pull_sagas_into(&self, saga_dir: &Path) -> Result<usize> {
        std::fs::create_dir_all(saga_dir)?;
        let mut downloaded = 0usize;
        let snapshot: Vec<(String, Arc<SagaClient>)> = self
            .remotes
            .read()
            .iter()
            .map(|p| (p.base_url.clone(), p.client.clone()))
            .collect();
        for (url, client) in snapshot {
            let entries = match client.list() {
                Ok(es) => es,
                Err(e) => {
                    eprintln!("hansa: list sagas at {url} failed: {e}");
                    continue;
                }
            };
            for entry in entries {
                let dest = saga_dir.join(format!("{}.saga", entry.tenant_id_hex));
                if !should_fetch(&dest, entry.last_modified) {
                    continue;
                }
                let Some(tenant_id) = parse_tenant_hex(&entry.tenant_id_hex) else {
                    eprintln!("hansa: invalid tenant hex {} from {url}", entry.tenant_id_hex);
                    continue;
                };
                match fetch_to_path(&client, tenant_id, &dest) {
                    Ok(_) => downloaded += 1,
                    Err(e) => eprintln!("hansa: fetch saga {} from {url} failed: {e}", entry.tenant_id_hex),
                }
            }
        }
        Ok(downloaded)
    }
}

fn should_fetch(dest: &Path, remote_mtime: i64) -> bool {
    match std::fs::metadata(dest) {
        Ok(m) => {
            let local = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            local < remote_mtime
        }
        Err(_) => true,
    }
}

fn parse_tenant_hex(hex: &str) -> Option<TenantId> {
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(TenantId::from_bytes(bytes))
}

impl Registry for HybridRegistry {
    fn append_link(&self, hansa: HansaId, link: &Link) -> Result<()> {
        self.local.append_link(hansa, link)
    }

    fn read_chain(&self, hansa: HansaId) -> Result<Vec<Link>> {
        self.local.read_chain(hansa)
    }

    fn found(
        &self,
        hansa: HansaId,
        skipper: &Skipper,
        embedding_dim: u32,
        created_at: i64,
    ) -> Result<()> {
        self.local.found(hansa, skipper, embedding_dim, created_at)
    }

    fn append_next(&self, hansa: HansaId, skipper: &Skipper, body: Body) -> Result<()> {
        self.local.append_next(hansa, skipper, body)
    }

    fn compact(&self, hansa: HansaId, skipper: &Skipper) -> Result<()> {
        self.local.compact(hansa, skipper)
    }

    fn members(&self, hansa: HansaId) -> Result<Vec<MemberRecord>> {
        // Local members come from the verified chain; remotes are merged
        // best-effort for cross-machine discovery (not chain-verified
        // here — that hardening is future work).
        let mut all = self.local.members(hansa)?;
        let remotes_snapshot: Vec<_> = self
            .remotes
            .read()
            .iter()
            .map(|p| (p.base_url.clone(), p.client.clone()))
            .collect();

        for (url, client) in remotes_snapshot {
            match fetch_with_client(&client, hansa) {
                Ok(remote_members) => {
                    for m in remote_members {
                        if !all.iter().any(|x| x.tenant_id == m.tenant_id) {
                            all.push(m);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("hansa: remote peer {url} unavailable: {e}");
                }
            }
        }
        all.sort_by(|a, b| a.tenant_id.0.cmp(&b.tenant_id.0));
        Ok(all)
    }
}

fn fetch_one(peer: &RemotePeer, hansa: HansaId) -> Result<Vec<MemberRecord>> {
    fetch_with_client(&peer.client, hansa).map_err(|e| HansaError::Invariant(e.to_string()))
}

fn fetch_with_client(
    client: &SagaClient,
    hansa: HansaId,
) -> std::result::Result<Vec<MemberRecord>, String> {
    let bytes = client
        .fetch_members_raw(&hansa.as_hex())
        .map_err(|e| format!("fetch: {e}"))?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice::<Vec<MemberRecord>>(&bytes)
        .map_err(|e| format!("decode: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HansaKey;

    #[test]
    fn write_only_local_when_no_remotes() {
        let dir = tempfile::tempdir().unwrap();
        let reg = HybridRegistry::new(FileRegistry::new(dir.path()));
        let key = HansaKey::from_bytes([1; 32]);
        let id = key.hansa_id();
        // Empty result with no peers + no joins.
        let m = reg.members(id).unwrap();
        assert!(m.is_empty());
        assert_eq!(reg.remote_count(), 0);
    }

    #[test]
    fn add_remote_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let reg = HybridRegistry::new(FileRegistry::new(dir.path()));
        reg.add_remote("http://x:9000");
        reg.add_remote("http://x:9000");
        reg.add_remote("http://y:9001");
        assert_eq!(reg.remote_count(), 2);
        reg.remove_remote("http://x:9000");
        assert_eq!(reg.remote_count(), 1);
    }
}
