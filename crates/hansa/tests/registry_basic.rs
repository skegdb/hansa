//! FileRegistry as a transport for the signed members chain.

use hansa::chain::{Body, Link};
use hansa::prelude::*;
use hansa::Genesis;
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

fn found(reg: &FileRegistry, skipper: &Skipper, dim: u32) -> HansaId {
    let id = HansaId::from_skipper(&skipper.public());
    let (g, sig) = Genesis::found(skipper, dim, 1, false);
    reg.append_link(id, &Link::genesis(g, sig)).unwrap();
    id
}

fn append_body(reg: &FileRegistry, id: HansaId, skipper: &Skipper, body: Body) {
    let chain = reg.read_chain(id).unwrap();
    let last = chain.last().unwrap();
    let link = Link::signed(skipper, last.seq + 1, last.hash(), body);
    reg.append_link(id, &link).unwrap();
}

#[test]
fn unfounded_hansa_has_no_members() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let id = HansaId::from_skipper(&Skipper::generate().public());
    assert!(reg.members(id).unwrap().is_empty());
}

#[test]
fn admit_then_revoke_round_trips_through_disk() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let skipper = Skipper::from_seed([1; 32]);
    let id = found(&reg, &skipper, 4);

    append_body(
        &reg,
        id,
        &skipper,
        Body::Admit {
            member: mk_member(1, 4),
            member_pub: None,
        },
    );
    append_body(
        &reg,
        id,
        &skipper,
        Body::Admit {
            member: mk_member(2, 4),
            member_pub: None,
        },
    );
    assert_eq!(reg.members(id).unwrap().len(), 2);

    append_body(
        &reg,
        id,
        &skipper,
        Body::Revoke {
            tenant_id: TenantId::from_bytes([1; 16]),
            at: 2,
            reason: None,
        },
    );
    let listed = reg.members(id).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].tenant_id, TenantId::from_bytes([2; 16]));
}

#[test]
fn tampered_log_line_is_rejected_on_replay() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let skipper = Skipper::from_seed([2; 32]);
    let id = found(&reg, &skipper, 4);
    append_body(
        &reg,
        id,
        &skipper,
        Body::Admit {
            member: mk_member(1, 4),
            member_pub: None,
        },
    );

    // Rewrite the admit line on disk: flip a dim digit. The signature no
    // longer covers it, so replaying the chain must fail rather than
    // silently trust the edited record.
    let log = dir.path().join(id.as_hex()).join("members.log");
    let text = std::fs::read_to_string(&log).unwrap();
    let tampered = text.replace("\"embedding_dim\":4", "\"embedding_dim\":9");
    assert_ne!(text, tampered, "expected to find the dim field to tamper");
    std::fs::write(&log, tampered).unwrap();

    assert!(reg.members(id).is_err());
}

#[test]
fn compact_collapses_log_and_preserves_members() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let skipper = Skipper::from_seed([7; 32]);
    let id = found(&reg, &skipper, 4);
    for s in 1..=3u8 {
        append_body(
            &reg,
            id,
            &skipper,
            Body::Admit {
                member: mk_member(s, 4),
                member_pub: None,
            },
        );
    }
    append_body(
        &reg,
        id,
        &skipper,
        Body::Revoke {
            tenant_id: TenantId::from_bytes([2; 16]),
            at: 1,
            reason: None,
        },
    );
    assert!(reg.read_chain(id).unwrap().len() > 1);
    let active = |reg: &FileRegistry| -> Vec<u8> {
        reg.members(id).unwrap().iter().map(|m| m.tenant_id.0[0]).collect()
    };
    assert_eq!(active(&reg), vec![1, 3]);

    reg.compact(id, &skipper).unwrap();
    // Log is now a single signed checkpoint, members preserved.
    assert_eq!(reg.read_chain(id).unwrap().len(), 1);
    assert_eq!(active(&reg), vec![1, 3]);

    // The chain keeps going on top of the checkpoint.
    append_body(
        &reg,
        id,
        &skipper,
        Body::Admit {
            member: mk_member(9, 4),
            member_pub: None,
        },
    );
    assert_eq!(active(&reg), vec![1, 3, 9]);
}

#[test]
fn tampered_checkpoint_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let skipper = Skipper::from_seed([8; 32]);
    let id = found(&reg, &skipper, 4);
    append_body(
        &reg,
        id,
        &skipper,
        Body::Admit {
            member: mk_member(1, 4),
            member_pub: None,
        },
    );
    reg.compact(id, &skipper).unwrap();

    // Inject a phantom member into the checkpoint on disk: the signature
    // no longer covers the set, so replay must reject it.
    let log = dir.path().join(id.as_hex()).join("members.log");
    let text = std::fs::read_to_string(&log).unwrap();
    let tampered = text.replace("\"tenant_id\":\"01010101", "\"tenant_id\":\"ff010101");
    assert_ne!(text, tampered);
    std::fs::write(&log, tampered).unwrap();
    assert!(reg.members(id).is_err());
}

#[test]
fn impostor_link_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let reg = FileRegistry::new(dir.path());
    let skipper = Skipper::from_seed([3; 32]);
    let id = found(&reg, &skipper, 4);

    // An impostor with the right id but a different key appends a
    // correctly-shaped admit. Replay must reject it: not the skipper.
    let impostor = Skipper::generate();
    append_body(
        &reg,
        id,
        &impostor,
        Body::Admit {
            member: mk_member(9, 4),
            member_pub: None,
        },
    );
    assert!(reg.members(id).is_err());
}
