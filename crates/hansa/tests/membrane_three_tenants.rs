//! End-to-end membrane test.
//!
//! Three in-process tenants A/B/C share a HansaKey. A inserts records
//! near unit-x, B near unit-y, C near unit-z. Some records are
//! `shareable`, some are not. Agent A then runs a membrane query for a
//! point near unit-y; the expected outcome is:
//!
//! - Hits come back with provenance markers.
//! - Non-shareable records from B and C never appear in remote results.
//! - The total respects the token budget.
//! - Killing C mid-flight (here: deleting its sidecar) still produces a
//!   non-empty result containing A and B hits.

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use skeg_rigging::{OpenError, RecordId, TenantId};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

const DIM: u32 = 4;

/// Filesystem-only `PeerOpener`: dispatches `TenantLocation::Path`
/// to the in-process adapter and rejects other variants.
fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

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

struct Agent {
    hansa: Hansa<Tenant>,
    tags: Vec<String>,
}

fn spawn_agent(root: &std::path::Path, label: u8, unit_at: usize, shareable_count: u32) -> Agent {
    let tenant_id = TenantId::from_bytes([label; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, DIM).unwrap());

    // 30 records: shareable_count near the chosen axis (high relevance),
    // the rest off-axis filler. The first `shareable_count` carry the
    // shareable=true flag.
    for i in 0..30u64 {
        let is_share = (i as u32) < shareable_count;
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
    let registry = Arc::new(FileRegistry::new(root));
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    let hansa = Hansa::open(HansaConfig {
        key,
        registry,
        local_tenant: tenant.clone(),
        local_tenant_id: tenant_id,
        local_tenant_location: skeg_rigging_net::TenantLocation::Path {
            path: tenant_dir,
        },
        saga_dir,
        peer_opener: Some(path_only_opener()),
        default_budget: TokenBudget::split(20, 30),
    })
    .unwrap();

    Agent {
        hansa,
        tags: vec!["topic".into(); 30],
    }
}

fn join_all(agents: &[Agent]) {
    for a in agents {
        a.hansa.join(a.tags.clone()).unwrap();
        // Force a saga refresh so the cluster centroids cover the real
        // distribution; join writes an initial empty saga only.
        a.hansa.refresh_saga(a.tags.clone(), 1, 7).unwrap();
    }
}

#[test]
fn membrane_returns_local_and_remote_with_provenance() {
    let dir = tempfile::tempdir().unwrap();

    // A near unit-x, B near unit-y, C near unit-z. Each shares 15
    // records out of 30.
    let a = spawn_agent(dir.path(), 1, 0, 15);
    let b = spawn_agent(dir.path(), 2, 1, 15);
    let c = spawn_agent(dir.path(), 3, 2, 15);
    join_all(&[a, b, c]);
    // Re-open A to pick up the registry state populated by B and C.
    let a = spawn_agent(dir.path(), 1, 0, 15);

    // Query near unit-y from agent A. B's saga should score highest.
    let hits = a
        .hansa
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();

    assert!(!hits.is_empty(), "membrane produced no hits");

    let local = hits.iter().filter(|h| matches!(h.origin, HitOrigin::Local)).count();
    let remote = hits
        .iter()
        .filter(|h| matches!(h.origin, HitOrigin::Remote { .. }))
        .count();
    assert!(local > 0, "no local hits");
    assert!(remote > 0, "no remote hits");
    // No non-shareable record from remote tenants leaked. Shareable ids
    // are label*1000..label*1000+15.
    for h in &hits {
        if let HitOrigin::Remote { tenant_id } = h.origin {
            let label = tenant_id.0[0];
            let local_id = h.record_id.0 - label as u64 * 1000;
            assert!(
                local_id < 15,
                "non-shareable record leaked: tenant={tenant_id} record_id={}",
                h.record_id
            );
        }
    }
}

#[test]
fn membrane_skips_offline_peers() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0, 15);
    let b = spawn_agent(dir.path(), 2, 1, 15);
    let c = spawn_agent(dir.path(), 3, 2, 15);
    join_all(&[a, b, c]);
    let a = spawn_agent(dir.path(), 1, 0, 15);

    // Knock out C by removing its sidecar so open_readonly fails.
    let c_dir = dir.path().join("tenant-3");
    std::fs::remove_file(Tenant::meta_path(&c_dir)).unwrap();

    let hits = a
        .hansa
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    assert!(!hits.is_empty(), "fan-out aborted on offline peer");
}

#[test]
fn local_only_skips_peers_entirely() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0, 15);
    let b = spawn_agent(dir.path(), 2, 1, 15);
    join_all(&[a, b]);
    let a = spawn_agent(dir.path(), 1, 0, 15);

    let hits = a
        .hansa
        .query(&near_unit(0, 0.0))
        .unwrap()
        .top_k(10)
        .local_only()
        .execute()
        .unwrap();
    for h in &hits {
        assert!(
            matches!(h.origin, HitOrigin::Local),
            "remote hit leaked under local_only"
        );
    }
}

#[test]
fn budget_caps_total() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0, 15);
    let b = spawn_agent(dir.path(), 2, 1, 15);
    join_all(&[a, b]);
    let a = spawn_agent(dir.path(), 1, 0, 15);

    let hits = a
        .hansa
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(50)
        .budget(TokenBudget::split(20, 5))
        .execute()
        .unwrap();
    assert!(hits.len() <= 5, "budget exceeded: {}", hits.len());
}
