//! End-to-end HybridRegistry: local FileRegistry + one remote SagaServer.
//!
//! Sets up two filesystem roots simulating two machines. Machine A
//! hosts a `SagaServer` exposing `members.snap`; agent B runs a
//! HybridRegistry that adds A as a remote. B's `members()` returns
//! its own local member + A's remote member, deduplicated.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use hansa::prelude::*;
use hansa::HybridRegistry;
use skeg_rigging::TenantId;
use skeg_rigging_net::TenantLocation;
use skeg_rigging_net_http::SagaServer;

fn mk_member(seed: u8, dim: u32) -> MemberRecord {
    MemberRecord {
        tenant_id: TenantId::from_bytes([seed; 16]),
        tenant_location: TenantLocation::Path {
            path: PathBuf::from(format!("/tmp/tenant-{seed}")),
        },
        embedding_dim: dim,
        joined_at: 1_700_000_000 + seed as i64,
    }
}

fn spawn_server(
    saga_dir: PathBuf,
    members_root: PathBuf,
) -> (u16, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let server = SagaServer::bind("127.0.0.1:0", saga_dir)
        .unwrap()
        .with_members_root(members_root);
    let port = server.local_addr().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let h = std::thread::spawn(move || server.serve_until(stop_clone));
    std::thread::sleep(Duration::from_millis(50));
    (port, stop, h)
}

#[test]
fn members_merges_local_and_remote() {
    // Machine A: hosts SagaServer + has one member in its FileRegistry.
    let root_a = tempfile::tempdir().unwrap();
    let reg_a = FileRegistry::new(root_a.path());
    let key = HansaKey::from_bytes([7; 32]);
    let hid = key.hansa_id();
    reg_a.join(hid, mk_member(0x11, 4)).unwrap();
    reg_a.compact(hid).unwrap(); // produce members.snap so the HTTP endpoint has data

    let sagas_a = root_a.path().join(hid.as_hex()).join("sagas");
    std::fs::create_dir_all(&sagas_a).unwrap();
    let (port, stop, h) = spawn_server(sagas_a, root_a.path().to_path_buf());

    // Machine B: HybridRegistry pointing at A.
    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote(format!("http://127.0.0.1:{port}"));
    reg_b.join(hid, mk_member(0x22, 4)).unwrap();

    let merged = reg_b.members(hid).unwrap();
    // Expected: B's own member (0x22) + A's member (0x11).
    let ids: Vec<u8> = merged.iter().map(|m| m.tenant_id.0[0]).collect();
    assert_eq!(ids, vec![0x11, 0x22], "got {ids:?}");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = h.join();
}

#[test]
fn remote_unreachable_does_not_break_local_view() {
    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote("http://127.0.0.1:1".to_string()); // closed port
    let key = HansaKey::from_bytes([3; 32]);
    let hid = key.hansa_id();
    reg_b.join(hid, mk_member(0x42, 4)).unwrap();

    let merged = reg_b.members(hid).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].tenant_id.0[0], 0x42);
}

#[test]
fn dedup_prefers_local_over_remote_for_same_tenant() {
    // Both sides register tenant 0x55; HybridRegistry's local entry
    // must survive even if the remote also reports 0x55.
    let root_a = tempfile::tempdir().unwrap();
    let reg_a = FileRegistry::new(root_a.path());
    let key = HansaKey::from_bytes([11; 32]);
    let hid = key.hansa_id();
    let remote_55 = mk_member(0x55, 4);
    reg_a.join(hid, remote_55.clone()).unwrap();
    reg_a.compact(hid).unwrap();
    let sagas_a = root_a.path().join(hid.as_hex()).join("sagas");
    std::fs::create_dir_all(&sagas_a).unwrap();
    let (port, stop, h) = spawn_server(sagas_a, root_a.path().to_path_buf());

    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote(format!("http://127.0.0.1:{port}"));
    // Same tenant id, different metadata.
    let mut local_55 = remote_55.clone();
    local_55.tenant_location = TenantLocation::Path {
        path: PathBuf::from("/different/path"),
    };
    reg_b.join(hid, local_55.clone()).unwrap();

    let merged = reg_b.members(hid).unwrap();
    assert_eq!(merged.len(), 1, "expected dedup by tenant_id");
    assert_eq!(merged[0].tenant_location, local_55.tenant_location);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = h.join();
}

#[test]
fn pull_sagas_into_downloads_missing_files() {
    let root_a = tempfile::tempdir().unwrap();
    let key = HansaKey::from_bytes([5; 32]);
    let hid = key.hansa_id();
    let sagas_a = root_a.path().join(hid.as_hex()).join("sagas");
    std::fs::create_dir_all(&sagas_a).unwrap();
    // Write a fake saga blob; the test only verifies bytes round-trip,
    // not that it's a valid SagaV1 file.
    let tenant_a = TenantId::from_bytes([0x11; 16]);
    let mut tenant_a_hex = String::new();
    for b in tenant_a.0 {
        tenant_a_hex.push_str(&format!("{b:02x}"));
    }
    let payload_a = b"agent-A saga bytes here";
    std::fs::write(sagas_a.join(format!("{tenant_a_hex}.saga")), payload_a).unwrap();

    let (port, stop, h) = spawn_server(sagas_a, root_a.path().to_path_buf());

    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote(format!("http://127.0.0.1:{port}"));

    let dest_dir = root_b.path().join("sagas-cache");
    let count = reg_b.pull_sagas_into(&dest_dir).unwrap();
    assert_eq!(count, 1);
    let pulled = std::fs::read(dest_dir.join(format!("{tenant_a_hex}.saga"))).unwrap();
    assert_eq!(pulled, payload_a);

    // Second pull should not re-download (mtime check).
    let count2 = reg_b.pull_sagas_into(&dest_dir).unwrap();
    assert_eq!(count2, 0);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = h.join();
}

#[test]
fn empty_remote_snap_returns_no_extra_members() {
    // Spin up A with no snap on disk; remote endpoint should return
    // empty array, B sees only its local members.
    let root_a = tempfile::tempdir().unwrap();
    let sagas_a = root_a.path().join("sagas");
    std::fs::create_dir_all(&sagas_a).unwrap();
    let (port, stop, h) = spawn_server(sagas_a, root_a.path().to_path_buf());

    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote(format!("http://127.0.0.1:{port}"));
    let key = HansaKey::from_bytes([4; 32]);
    let hid = key.hansa_id();
    reg_b.join(hid, mk_member(0x77, 4)).unwrap();

    let merged = reg_b.members(hid).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].tenant_id.0[0], 0x77);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = h.join();
}
