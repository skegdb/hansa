//! F.6 - `Hansa::query_async` end-to-end under a Tokio runtime.
//!
//! Three agents share a hansa; A queries the membrane via the async
//! path. Peers are bridged through `SyncToAsync<Tenant>` because the
//! on-disk adapter is sync. The fan-out runs on `tokio::spawn`
//! instead of `std::thread`, so the test exercises the actual
//! production async wiring.

#![cfg(feature = "tokio")]

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use hansa::membrane::AsyncPeerOpener;
use skeg_rigging::asyncs::{AsyncReadOnlyView, SyncToAsync};
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

fn async_opener() -> AsyncPeerOpener {
    Arc::new(|_tid, loc: TenantLocation| {
        Box::pin(async move {
            match loc {
                TenantLocation::Path { path } => {
                    let inner = Tenant::open_readonly_at(&path)
                        .map_err(skeg_rigging::OpenError::from)?;
                    let bridged: Box<dyn AsyncReadOnlyView> = Box::new(SyncToAsync::new(inner));
                    Ok(bridged)
                }
                _ => Err(OpenError::NotFound),
            }
        })
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
                (i as u32) < 15,
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
    Hansa::open(HansaConfig {
        key,
        registry,
        local_tenant: tenant,
        local_tenant_id: tid,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: None,
        default_budget: TokenBudget::split(20, 30),
        async_peer_opener: Some(async_opener()),
    })
    .unwrap()
}

fn join_all(agents: &[&Hansa<Tenant>]) {
    for a in agents {
        a.join(vec!["topic".into()]).unwrap();
        a.refresh_saga(vec!["topic".into()], 1, 7).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_async_returns_remote_hits() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    // Re-open A to pick up B + C in registry.
    let a = spawn_agent(dir.path(), 1, 0);

    let hits = a
        .query_async(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute()
        .await
        .unwrap();
    assert!(!hits.is_empty(), "async membrane produced no hits");

    let remote = hits
        .iter()
        .filter(|h| matches!(h.origin, HitOrigin::Remote { .. }))
        .count();
    assert!(remote > 0, "no remote hits from async fan-out");

    // Shareable filter must still apply: peer records with id offset
    // >= 15 are non-shareable.
    for h in &hits {
        if let HitOrigin::Remote { tenant_id } = h.origin {
            let label = tenant_id.0[0];
            let local_id = h.record_id.0 - label as u64 * 1000;
            assert!(
                local_id < 15,
                "non-shareable record leaked: {tenant_id} id={}",
                h.record_id
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_async_local_only_skips_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    join_all(&[&a, &b]);
    let a = spawn_agent(dir.path(), 1, 0);

    let hits = a
        .query_async(&near_unit(0, 0.0))
        .unwrap()
        .top_k(10)
        .local_only()
        .execute()
        .await
        .unwrap();
    for h in &hits {
        assert!(matches!(h.origin, HitOrigin::Local));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_async_reports_stats() {
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    let c = spawn_agent(dir.path(), 3, 2);
    join_all(&[&a, &b, &c]);
    let a = spawn_agent(dir.path(), 1, 0);

    let (_hits, stats) = a
        .query_async(&near_unit(1, 0.0))
        .unwrap()
        .top_k(10)
        .execute_with_stats()
        .await
        .unwrap();
    assert!(stats.peers_attempted >= 2, "should have attempted B + C");
    assert_eq!(stats.peers_completed, stats.peers_attempted);
    assert_eq!(stats.dropped_for_deadline, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_async_concurrent_queries_share_handle() {
    // Spin 4 concurrent async queries against the same Hansa handle.
    // Each tokio::spawn task drives its own membrane fan-out; results
    // must be independent and consistent.
    let dir = tempfile::tempdir().unwrap();
    let a = spawn_agent(dir.path(), 1, 0);
    let b = spawn_agent(dir.path(), 2, 1);
    join_all(&[&a, &b]);
    let a = Arc::new(spawn_agent(dir.path(), 1, 0));

    let mut handles = vec![];
    for _ in 0..4 {
        let h = a.clone();
        handles.push(tokio::spawn(async move {
            h.query_async(&near_unit(1, 0.0))
                .unwrap()
                .top_k(5)
                .execute()
                .await
                .unwrap()
        }));
    }
    for h in handles {
        let hits = h.await.unwrap();
        assert!(!hits.is_empty(), "concurrent async query returned empty");
    }
}
