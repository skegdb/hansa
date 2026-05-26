//! Background saga refresh test.
//!
//! The test starts a refresh task with a short interval, populates the
//! tenant in waves, and verifies that the on-disk saga's
//! `record_count` advances without manual calls to `refresh_saga`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hansa::prelude::*;
use skeg_rigging::{RecordId, TenantId};
use skeg_rigging_skeg::Tenant;

const DIM: u32 = 4;

fn unit(at: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM as usize];
    v[at] = 1.0;
    v
}

fn read_saga_count(handle: &Hansa<Tenant>) -> u64 {
    Saga::read_from_path(&handle.local_saga_path())
        .map(|s| s.record_count)
        .unwrap_or(0)
}

#[test]
fn background_task_rebuilds_saga_after_growth() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let tenant_id = TenantId::from_bytes([1; 16]);
    let tenant_dir = root.join("tenant-1");
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, DIM).unwrap());

    // Seed the tenant before opening hansa so the initial saga has a
    // baseline > 0 (otherwise threshold_ratio * 0 == 0 and any insert
    // would trigger a refresh, which is fine but harder to assert on).
    for i in 0..20u64 {
        tenant
            .insert(
                RecordId(i),
                unit((i % 4) as usize),
                true,
                vec!["topic".into()],
                format!("p-{i}").into_bytes(),
            )
            .unwrap();
    }
    tenant.flush().unwrap();

    let key = HansaKey::from_bytes([7; 32]);
    let hid = key.hansa_id();
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    let handle = Hansa::open(HansaConfig {
        key,
        registry: Arc::new(FileRegistry::new(root)),
        local_tenant: tenant.clone(),
        local_tenant_id: tenant_id,
        local_tenant_location: skeg_rigging_net::TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: None,
        default_budget: TokenBudget::default(),
    })
    .unwrap();
    handle.join(vec!["topic".into(); 20]).expect("join");
    handle.refresh_saga(vec!["topic".into(); 20], 1, 7).unwrap();

    assert_eq!(read_saga_count(&handle), 20);

    // Start the refresh task: poll every 50 ms, rebuild whenever the
    // tenant has grown by 10% (so >= 2 new records beyond the baseline).
    let refresh = handle.start_background_refresh(
        BackgroundRefreshConfig {
            interval: Duration::from_millis(50),
            threshold_ratio: 0.10,
            min_growth: 1,
            seed_for_kmeans: 7,
        },
        || vec!["topic".into()],
    );

    // Wave 1: add 10 records, expect the saga to bump to 30.
    for i in 20..30u64 {
        tenant
            .insert(
                RecordId(i),
                unit((i % 4) as usize),
                true,
                vec!["topic".into()],
                format!("p-{i}").into_bytes(),
            )
            .unwrap();
    }
    wait_until(Duration::from_secs(2), || read_saga_count(&handle) >= 30);
    assert!(read_saga_count(&handle) >= 30, "saga did not rebuild after wave 1");

    // Wave 2: stop the task, add more records, verify saga does NOT
    // advance.
    refresh.stop();
    let saga_at_stop = read_saga_count(&handle);
    for i in 30..50u64 {
        tenant
            .insert(
                RecordId(i),
                unit((i % 4) as usize),
                true,
                vec!["topic".into()],
                format!("p-{i}").into_bytes(),
            )
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        read_saga_count(&handle),
        saga_at_stop,
        "saga rebuilt after handle stopped"
    );
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
