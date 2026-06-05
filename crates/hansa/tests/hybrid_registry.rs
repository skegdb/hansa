//! HybridRegistry: local signed chain + a remote saga server.
//!
//! Local membership now comes from the verified members chain. Merging
//! *remote* members over HTTP relied on the old compacted `members.snap`
//! and is not chain-verified; that path is deferred to the network
//! registry work, so the tests here cover the local view holding up when
//! a remote is unreachable or empty, plus saga-blob pulling.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use hansa::chain::{Body, Link};
use hansa::prelude::*;
use hansa::{Genesis, HybridRegistry};
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

/// Found a hansa and admit one member through the registry transport.
fn found_admit(reg: &HybridRegistry, skipper: &Skipper, member: MemberRecord, dim: u32) -> HansaId {
    let id = HansaId::from_skipper(&skipper.public());
    let (g, sig) = Genesis::found(skipper, dim, 1, false);
    reg.append_link(id, &Link::genesis(g, sig)).unwrap();
    let last = reg.read_chain(id).unwrap().last().unwrap().clone();
    reg.append_link(
        id,
        &Link::signed(
            skipper,
            last.seq + 1,
            last.hash(),
            Body::Admit {
                member,
                member_pub: None,
            },
        ),
    )
    .unwrap();
    id
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
fn remote_unreachable_does_not_break_local_view() {
    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote("http://127.0.0.1:1".to_string()); // closed port
    let hid = found_admit(&reg_b, &Skipper::from_seed([3; 32]), mk_member(0x42, 4), 4);

    let merged = reg_b.members(hid).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].tenant_id.0[0], 0x42);
}

#[test]
fn empty_remote_returns_no_extra_members() {
    let root_a = tempfile::tempdir().unwrap();
    let sagas_a = root_a.path().join("sagas");
    std::fs::create_dir_all(&sagas_a).unwrap();
    let (port, stop, h) = spawn_server(sagas_a, root_a.path().to_path_buf());

    let root_b = tempfile::tempdir().unwrap();
    let reg_b = HybridRegistry::new(FileRegistry::new(root_b.path()));
    reg_b.add_remote(format!("http://127.0.0.1:{port}"));
    let hid = found_admit(&reg_b, &Skipper::from_seed([4; 32]), mk_member(0x77, 4), 4);

    let merged = reg_b.members(hid).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].tenant_id.0[0], 0x77);

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
