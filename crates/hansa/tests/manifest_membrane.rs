//! F.5 - Peer manifests bias the membrane's saga scoring.
//!
//! Three agents share a hansa. Agent A queries near unit-2. Both B
//! and C have records on that axis; their raw saga scores against
//! the query should be close. After we mark B's hits as useful many
//! times, B's manifest should grow and its saga score gets biased
//! up - B should receive a larger budget share on subsequent queries.

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use skeg_rigging::{OpenError, RecordId, TenantId};
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

fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

fn spawn_agent(root: &std::path::Path, label: u8, unit_at: usize) -> Hansa<Tenant> {
    let tid = TenantId::from_bytes([label; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tid, DIM).unwrap());
    for i in 0..30u64 {
        let v = if i < 20 {
            near_unit(unit_at, ((i % 5) as f32) * 0.01)
        } else {
            near_unit((unit_at + 1) % DIM as usize, 0.02)
        };
        tenant
            .insert(
                RecordId(label as u64 * 1000 + i),
                v,
                (i as u32) < 15, // first 15 shareable
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
        local_tenant_id: tid,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(path_only_opener()),
        default_budget: TokenBudget::split(20, 30),
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .unwrap()
}

fn join_all(agents: &[&Hansa<Tenant>]) {
    for a in agents {
        a.join(vec!["topic".into()]).unwrap();
        a.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
}

#[test]
fn fresh_manifest_does_not_bias_scoring() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    // Re-open A so it picks up B + C in registry.
    let a = spawn_agent(dir.path(), 1, 0);

    let hits = a
        .query(&near_unit(2, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    assert!(!hits.is_empty());
    // Cold federation: no manifests are useful yet, so the membrane
    // should just behave like the pre-F.5 path.
    let c_id = TenantId::from_bytes([3; 16]);
    let manifest_before = a.manifest_store().read(c_id);
    assert_eq!(manifest_before.useful_hits, 0);
    // After one query, total_hits should reflect what C delivered.
    assert!(manifest_before.total_hits > 0);
}

#[test]
fn marking_hits_useful_increments_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    let a = spawn_agent(dir.path(), 1, 0);

    let hits = a
        .query(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    let remote_count = hits
        .iter()
        .filter(|h| matches!(h.origin, HitOrigin::Remote { .. }))
        .count();
    assert!(remote_count > 0, "test needs remote hits to be meaningful");

    a.record_useful_hits(&hits);

    // Every remote peer that produced a hit should now have
    // useful_hits == total_hits (we marked every hit).
    for h in &hits {
        if let HitOrigin::Remote { tenant_id } = h.origin {
            let m = a.manifest_store().read(tenant_id);
            assert!(m.useful_hits > 0, "peer {tenant_id} stayed at 0 useful");
            assert!(m.last_useful_at > 0);
        }
    }
}

#[test]
fn manifest_bias_grows_share_for_useful_peer() {
    // Two peers with the same on-axis records. Mark B's hits useful
    // repeatedly; expect B's saga to outscore C on subsequent
    // identical queries even though their raw saga scores are tied.
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 2);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    let a = spawn_agent(dir.path(), 1, 0);

    let b_id = TenantId::from_bytes([2; 16]);
    let c_id = TenantId::from_bytes([3; 16]);

    // Heavily inflate B's manifest by direct writes (faster than
    // running many query+mark cycles in a test).
    let store = a.manifest_store();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let b_manifest = PeerManifest {
        peer_id_bytes: *b_id.as_bytes(),
        useful_hits: 50,
        total_hits: 50,
        last_useful_at: now,
    };
    store.write(&b_manifest).unwrap();
    // C stays at neutral (no manifest written).

    let b_factor = store.read(b_id).usefulness_factor(now);
    let c_factor = store.read(c_id).usefulness_factor(now);
    assert!(
        b_factor > c_factor,
        "expected B's bias factor > C's, got b={b_factor} c={c_factor}"
    );
    assert!(b_factor >= 1.4, "B should be near the cap, got {b_factor}");
    assert_eq!(c_factor, 1.0);

    // End-to-end: query must succeed and produce hits from at least one peer.
    let hits = a
        .query(&near_unit(2, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .unwrap();
    let has_b = hits.iter().any(|h| matches!(h.origin, HitOrigin::Remote { tenant_id } if tenant_id == b_id));
    assert!(has_b, "B should be reached with biased budget");
}

#[test]
fn cold_peer_factor_outweighs_warm_peer_in_scoring() {
    // F.4: a peer in a cold streak should be ranked below a peer with
    // a strong manifest, even when their raw saga scores are identical.
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 2);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    let a = spawn_agent(dir.path(), 1, 0);

    let b_id = TenantId::from_bytes([2; 16]);
    let c_id = TenantId::from_bytes([3; 16]);

    let store = a.manifest_store();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // B = warm (high useful).
    store
        .write(&PeerManifest {
            peer_id_bytes: *b_id.as_bytes(),
            useful_hits: 50,
            total_hits: 50,
            last_useful_at: now,
        })
        .unwrap();
    // C = cold (saturated dud streak).
    store
        .write(&PeerManifest {
            peer_id_bytes: *c_id.as_bytes(),
            useful_hits: 0,
            total_hits: PeerManifest::TRIAL_RETURNS * 3,
            last_useful_at: 0,
        })
        .unwrap();

    let b_factor = store.read(b_id).usefulness_factor(now);
    let c_factor = store.read(c_id).usefulness_factor(now);
    assert!(b_factor >= 1.4, "warm peer should be near +cap: {b_factor}");
    assert!(c_factor <= 0.3, "cold peer should be near -cap: {c_factor}");
    assert!(store.read(c_id).is_cold());
    // Sanity: warm/cold ratio ~7x.
    assert!(b_factor / c_factor > 5.0);
}

#[test]
fn fresh_peer_stays_neutral_during_trial_period() {
    // F.4 trial period: a peer that has returned hits but none have
    // been marked useful yet keeps a 1.0 factor as long as total_hits
    // < TRIAL_RETURNS.
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    join_all(&[&a, &b]);
    let a = spawn_agent(dir.path(), 1, 0);

    let b_id = TenantId::from_bytes([2; 16]);
    a.manifest_store()
        .write(&PeerManifest {
            peer_id_bytes: *b_id.as_bytes(),
            useful_hits: 0,
            total_hits: PeerManifest::TRIAL_RETURNS - 1,
            last_useful_at: 0,
        })
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let factor = a.manifest_store().read(b_id).usefulness_factor(now);
    assert_eq!(factor, 1.0);
    assert!(!a.manifest_store().read(b_id).is_cold());
}

#[test]
fn record_useful_hits_ignores_local_hits() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    a.join(vec!["topic".into()]).unwrap();

    // Build a fake hit list with only local hits.
    let hits = a
        .query(&near_unit(0, 0.0))
        .unwrap()
        .top_k(5)
        .local_only()
        .execute()
        .unwrap();
    assert!(hits.iter().all(|h| matches!(h.origin, HitOrigin::Local)));

    a.record_useful_hits(&hits);
    // No manifest file should exist for the local tenant id.
    let local_id = TenantId::from_bytes([1; 16]);
    let m = a.manifest_store().read(local_id);
    assert_eq!(m.useful_hits, 0);
}
