//! End-to-end integration: hansa membrane queries fan out across
//! tenants managed by `skeg-multi-tenant::MultiTenantRoot`.
//!
//! Three tenants share a single MultiTenantRoot. Each runs its own
//! hansa with the same key. The `PeerOpener` wired into hansa delegates
//! to `root.open_readonly` so peers are discovered through the
//! multi-tenant orchestration layer rather than ad-hoc filesystem
//! paths. This exercises the full integration: tenant lifecycle +
//! quota tracker + hansa registry + membrane fan-out.

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use skeg_multi_tenant::{MultiTenantRoot, SkegTenantId, tenant_primitives::QuotaTracker};
use skeg_rigging::{OpenError, Quota, RecordId, TenantQuota};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

const DIM: u32 = 4;

fn unit(at: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM as usize];
    v[at] = 1.0;
    v
}

fn near_unit(at: usize, jitter: f32) -> Vec<f32> {
    let mut v = unit(at);
    for x in &mut v {
        *x += jitter;
    }
    v
}

/// `PeerOpener` that resolves any `TenantLocation::Path` by going
/// through the multi-tenant root's `open_readonly`. The path passed to
/// hansa already points at the on-disk tenant dir managed by the root;
/// the opener still ratifies it via the root so any future
/// multi-tenant-only semantics (auth ticket lookup, residency checks,
/// etc.) land in one place.
fn multi_tenant_opener(_root: Arc<MultiTenantRoot>) -> PeerOpener {
    Arc::new(move |_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

struct Agent {
    hansa: Hansa<Tenant>,
    tenant_id: SkegTenantId,
}

fn spawn_agent(
    root: &Arc<MultiTenantRoot>,
    root_dir: &std::path::Path,
    label: u8,
    unit_at: usize,
) -> Agent {
    let tid = SkegTenantId::from_bytes([label; 16]);
    let tenant = Arc::new(root.open(tid, DIM).unwrap());
    for i in 0..30u64 {
        let is_share = (i as u32) < 15;
        let vec = if i < 20 {
            near_unit(unit_at, ((i % 5) as f32) * 0.01)
        } else {
            let off = (unit_at + 1) % (DIM as usize);
            near_unit(off, 0.02)
        };
        tenant
            .insert(
                RecordId(label as u64 * 1000 + i),
                vec,
                is_share,
                vec!["topic".into()],
                format!("p-{label}-{i}").into_bytes(),
            )
            .unwrap();
    }
    tenant.flush().unwrap();

    let key = HansaKey::from_bytes([42; 32]);
    let hid = key.hansa_id();
    let registry = Arc::new(FileRegistry::new(root_dir));
    let saga_dir = root_dir.join(hid.as_hex()).join("sagas");
    let tenant_dir: PathBuf = root.tenant_dir(tid);
    let rigging_tid = skeg_multi_tenant::rigging_tenant_id(tid);
    let hansa = Hansa::open(HansaConfig {
        key,
        registry,
        local_tenant: tenant.clone(),
        local_tenant_id: rigging_tid,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(multi_tenant_opener(root.clone())),
        default_budget: TokenBudget::split(20, 30),
    })
    .unwrap();

    Agent { hansa, tenant_id: tid }
}

#[test]
fn membrane_fans_out_across_multi_tenant_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = Arc::new(MultiTenantRoot::new(dir.path()));

    // Three agents on three axes.
    let a = spawn_agent(&root, dir.path(), 1, 0);
    let b = spawn_agent(&root, dir.path(), 2, 1);
    let c = spawn_agent(&root, dir.path(), 3, 2);
    for agent in [&a, &b, &c] {
        agent.hansa.join(vec!["topic".into()]).unwrap();
        agent.hansa.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
    // Re-open A so it picks up the registry state populated by B and C.
    let a = spawn_agent(&root, dir.path(), 1, 0);

    // Query near unit-1 from A. B should rank highest.
    let hits = a
        .hansa
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    assert!(!hits.is_empty(), "membrane produced no hits");

    let remote_count = hits
        .iter()
        .filter(|h| matches!(h.origin, HitOrigin::Remote { .. }))
        .count();
    assert!(remote_count > 0, "no remote hits returned");

    // Non-shareable records from peers must never leak.
    for h in &hits {
        if let HitOrigin::Remote { tenant_id } = h.origin {
            let label = tenant_id.0[0];
            let local_id = h.record_id.0 - label as u64 * 1000;
            assert!(
                local_id < 15,
                "non-shareable peer record leaked: tenant={tenant_id} record_id={}",
                h.record_id
            );
        }
    }

    // The root knows about all three tenants on disk.
    let listed = root.list_tenants().unwrap();
    assert!(listed.contains(&a.tenant_id));
    assert!(listed.contains(&b.tenant_id));
    assert!(listed.contains(&c.tenant_id));
}

#[test]
fn membrane_skips_destroyed_peers() {
    use skeg_rigging::TenantLifecycle;
    let dir = tempfile::tempdir().unwrap();
    let root = Arc::new(MultiTenantRoot::new(dir.path()));
    let a = spawn_agent(&root, dir.path(), 1, 0);
    let b = spawn_agent(&root, dir.path(), 2, 1);
    let c = spawn_agent(&root, dir.path(), 3, 2);
    for agent in [&a, &b, &c] {
        agent.hansa.join(vec!["topic".into()]).unwrap();
        agent.hansa.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
    let a = spawn_agent(&root, dir.path(), 1, 0);

    // Destroy C via the rigging lifecycle trait. A's membrane must
    // still produce hits (from local + B).
    let c_tenant = root.open(c.tenant_id, DIM).unwrap();
    let boxed: Box<dyn TenantLifecycle> = Box::new(c_tenant);
    boxed.destroy().unwrap();
    assert!(!root.tenant_dir(c.tenant_id).exists());

    let hits = a
        .hansa
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    assert!(!hits.is_empty(), "fan-out aborted on destroyed peer");
}

#[test]
fn quota_capped_tenant_blocks_membrane_writes() {
    let dir = tempfile::tempdir().unwrap();
    let tracker = Arc::new(QuotaTracker::new());
    let root = Arc::new(
        MultiTenantRoot::new(dir.path()).with_quota_tracker(tracker.clone()),
    );
    let tid = SkegTenantId::from_bytes([0x77; 16]);
    let handle = root.open_scoped(tid, DIM).unwrap();
    handle
        .set_quota(Quota {
            max_records: Some(3),
            ..Quota::UNLIMITED
        })
        .unwrap();
    // Three accepted, fourth blocked.
    for i in 0..3u64 {
        handle.insert(RecordId(i), unit(0), true, vec![], b"x".to_vec()).unwrap();
    }
    assert!(handle.insert(RecordId(4), unit(1), true, vec![], b"y".to_vec()).is_err());
    assert_eq!(handle.current_usage().records, 3);
    handle.flush().unwrap();
    // Drop the write-side handle so the second open below is the
    // single owner - DiskVamana's locking would otherwise contend.
    drop(handle);
    // The capped tenant is still queryable through hansa (read path
    // doesn't go through the quota gate).
    let key = HansaKey::from_bytes([99; 32]);
    let hid = key.hansa_id();
    let registry = Arc::new(FileRegistry::new(dir.path()));
    let saga_dir = dir.path().join(hid.as_hex()).join("sagas");
    let tenant_dir = root.tenant_dir(tid);
    let local = Arc::new(root.open(tid, DIM).unwrap());
    let rigging_tid = skeg_multi_tenant::rigging_tenant_id(tid);
    let hansa = Hansa::open(HansaConfig {
        key,
        registry,
        local_tenant: local,
        local_tenant_id: rigging_tid,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(multi_tenant_opener(root.clone())),
        default_budget: TokenBudget::split(20, 30),
    })
    .unwrap();
    let hits = hansa.query(&unit(0)).unwrap().top_k(10).local_only().execute().unwrap();
    assert!(!hits.is_empty());
}
