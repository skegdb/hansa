//! `MembraneQuery::deadline` cuts off remote fan-out when peers exceed
//! the wall-clock budget. Uses a `SlowView` wrapper that delays
//! `query_filtered` so the test runs deterministically without real
//! network slowness.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use hansa::prelude::*;
use skeg_rigging::{
    Filter, Hit, IterVectors, OpenError, QueryError, QueryFiltered, ReadOnlyView, RecordId,
    TenantId,
};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

const DIM: u32 = 4;

fn unit(at: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM as usize];
    v[at] = 1.0;
    v
}

/// Wraps a real `Tenant`, sleeping a fixed duration before forwarding
/// `query_filtered` to it. Used to simulate a slow peer.
struct SlowView {
    inner: Tenant,
    delay: Duration,
}

impl IterVectors for SlowView {
    fn iter_vectors(&self) -> Box<dyn Iterator<Item = (RecordId, Vec<f32>)> + '_> {
        self.inner.iter_vectors()
    }
    fn record_count(&self) -> u64 {
        <Tenant as IterVectors>::record_count(&self.inner)
    }
    fn embedding_dim(&self) -> u32 {
        <Tenant as IterVectors>::embedding_dim(&self.inner)
    }
}

impl QueryFiltered for SlowView {
    fn query_filtered(
        &self,
        embedding: &[f32],
        top_k: u32,
        filter: &dyn Filter,
    ) -> std::result::Result<Vec<Hit>, QueryError> {
        thread::sleep(self.delay);
        self.inner.query_filtered(embedding, top_k, filter)
    }
}

impl ReadOnlyView for SlowView {
    fn tenant_id(&self) -> TenantId {
        ReadOnlyView::tenant_id(&self.inner)
    }
    fn close(self: Box<Self>) -> std::result::Result<(), OpenError> {
        Ok(())
    }
}

/// Opener that re-opens the peer as a `SlowView` with the configured
/// delay. Captured by `Arc` so the closure is `Send + Sync`.
fn slow_opener(delay: Duration) -> PeerOpener {
    Arc::new(
        move |_tid, loc: &TenantLocation| -> std::result::Result<Box<dyn ReadOnlyView>, OpenError> {
            match loc {
                TenantLocation::Path { path } => {
                    let inner = Tenant::open_readonly_at(path).map_err(OpenError::from)?;
                    Ok(Box::new(SlowView { inner, delay }))
                }
                _ => Err(OpenError::NotFound),
            }
        },
    )
}

fn spawn_with_opener(
    root: &std::path::Path,
    label: u8,
    opener: PeerOpener,
) -> Hansa<Tenant> {
    let tenant_id = TenantId::from_bytes([label; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, DIM).unwrap());
    for i in 0..10u64 {
        tenant
            .insert(
                RecordId(label as u64 * 1000 + i),
                unit(((i % 3) + 1) as usize % DIM as usize),
                true,
                vec!["topic".into()],
                format!("p-{label}-{i}").into_bytes(),
            )
            .unwrap();
    }
    tenant.flush().unwrap();

    let key = HansaKey::from_bytes([42; 32]);
    let skipper = Skipper::from_seed([42; 32]);
    let hid = HansaId::from_skipper(&skipper.public());
    let registry = Arc::new(FileRegistry::new(root));
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    Hansa::open(HansaConfig {
        key,
        skipper: Some(skipper),
        hansa_id: Some(hid),
        registry,
        local_tenant: tenant,
        local_tenant_id: tenant_id,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(opener),
        default_budget: TokenBudget::split(20, 30),
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .unwrap()
}

#[test]
fn deadline_drops_slow_peers() {
    let dir = tempfile::tempdir().unwrap();
    // A is the caller, B + C are peers with a 200 ms artificial delay.
    let opener = slow_opener(Duration::from_millis(200));
    let a = spawn_with_opener(dir.path(), 1, opener.clone());
    let b = spawn_with_opener(dir.path(), 2, opener.clone());
    let c = spawn_with_opener(dir.path(), 3, opener.clone());
    for h in [&a, &b, &c] {
        h.join(vec!["topic".into()]).unwrap();
        h.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
    // Re-open A to pick up B + C in its registry view.
    let a = spawn_with_opener(dir.path(), 1, opener);

    let (hits, stats) = a
        .query(&unit(1))
        .unwrap()
        .top_k(10)
        .deadline(Duration::from_millis(20))
        .execute_with_stats()
        .unwrap();

    // Local hits always make it. Remotes are slow - none should land.
    let remote = hits
        .iter()
        .filter(|h| matches!(h.origin, HitOrigin::Remote { .. }))
        .count();
    assert_eq!(remote, 0, "remote hits leaked past deadline: {remote}");
    assert!(stats.peers_attempted >= 2, "expected B + C as peers");
    assert_eq!(stats.peers_completed, 0);
    assert_eq!(stats.dropped_for_deadline, stats.peers_attempted);
}

#[test]
fn no_deadline_waits_for_all_peers() {
    let dir = tempfile::tempdir().unwrap();
    // Small delay so the test is fast but still exercises the threaded
    // fan-out path.
    let opener = slow_opener(Duration::from_millis(5));
    let a = spawn_with_opener(dir.path(), 1, opener.clone());
    let b = spawn_with_opener(dir.path(), 2, opener.clone());
    for h in [&a, &b] {
        h.join(vec!["topic".into()]).unwrap();
        h.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
    let a = spawn_with_opener(dir.path(), 1, opener);

    let (hits, stats) = a
        .query(&unit(1))
        .unwrap()
        .top_k(10)
        .execute_with_stats()
        .unwrap();
    assert!(!hits.is_empty());
    assert_eq!(stats.dropped_for_deadline, 0);
    assert_eq!(stats.peers_completed, stats.peers_attempted);
}
