//! FileRegistry round-trip and edge cases.

use hansa::prelude::*;
use skeg_rigging::TenantId;
use skeg_rigging_net::TenantLocation;
use std::path::PathBuf;

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

#[test]
fn join_leave_members_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let key = HansaKey::from_bytes([1; 32]);
    let id = key.hansa_id();

    assert!(reg.members(id).unwrap().is_empty());

    reg.join(id, mk_member(1, 4)).unwrap();
    reg.join(id, mk_member(2, 4)).unwrap();
    let listed = reg.members(id).unwrap();
    assert_eq!(listed.len(), 2);

    reg.leave(id, TenantId::from_bytes([1; 16])).unwrap();
    let listed = reg.members(id).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].tenant_id, TenantId::from_bytes([2; 16]));
}

#[test]
fn rejects_dim_mismatch_on_join() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let key = HansaKey::from_bytes([2; 32]);
    let id = key.hansa_id();
    reg.join(id, mk_member(1, 768)).unwrap();
    let err = reg.join(id, mk_member(2, 384)).unwrap_err();
    assert!(matches!(
        err,
        HansaError::DimMismatch {
            existing: 768,
            joining: 384,
        }
    ));
}

#[test]
fn rejoin_is_idempotent_via_log() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let key = HansaKey::from_bytes([3; 32]);
    let id = key.hansa_id();
    let m = mk_member(1, 4);
    reg.join(id, m.clone()).unwrap();
    reg.join(id, m.clone()).unwrap();
    let listed = reg.members(id).unwrap();
    assert_eq!(listed.len(), 1);
}

#[test]
fn compact_truncates_log() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let key = HansaKey::from_bytes([4; 32]);
    let id = key.hansa_id();
    for seed in 1..=5u8 {
        reg.join(id, mk_member(seed, 4)).unwrap();
    }
    for seed in 1..=3u8 {
        reg.leave(id, TenantId::from_bytes([seed; 16])).unwrap();
    }
    reg.compact(id).unwrap();
    let log = dir.path().join(id.as_hex()).join("members.log");
    assert!(log.exists());
    assert_eq!(std::fs::metadata(&log).unwrap().len(), 0);
    let listed = reg.members(id).unwrap();
    assert_eq!(listed.len(), 2);
}
