//! Cross-process integration test.
//!
//! Spawns three separate OS processes (one per agent) that each populate
//! their own tenant and join a shared hansa. Then spawns a fourth
//! process that opens the hansa from agent A's perspective and issues a
//! membrane query. The test parses the querier's JSON output and
//! asserts on its shape.
//!
//! This exercises:
//! - Filesystem-local registry consistency across pids (append-only
//!   `members.log` plus snapshot).
//! - Saga files written in one process being read in another.
//! - Read-only opens (`skeg_rigging_skeg::open_readonly`) across pid
//!   boundaries via the JSON sidecar.

use std::path::{Path, PathBuf};
use std::process::Command;

fn key_hex() -> String {
    // Deterministic key so the test is reproducible.
    "2a".repeat(32)
}

/// Locate the `hansa-agent` binary. `assert_cmd::cargo_bin` only works
/// when the binary lives in the same package as the test, so we walk up
/// from `current_exe` (the test binary) to find the workspace
/// `target/<profile>/hansa-agent`.
fn agent_bin() -> PathBuf {
    // Test binary lives at target/<profile>/deps/<test>-<hash>.
    let exe = std::env::current_exe().expect("current_exe");
    let mut target_profile_dir = exe.clone();
    target_profile_dir.pop(); // remove test-<hash>
    target_profile_dir.pop(); // remove deps/
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let path = target_profile_dir.join(format!("hansa-agent{suffix}"));
    if !path.exists() {
        panic!(
            "hansa-agent binary not found at {path:?}; build it first via\n\
             `cargo build -p hansa-agent` (cargo test --workspace handles this automatically)"
        );
    }
    path
}

fn run_populate(root: &Path, label: u8, axis: usize) {
    let bin = agent_bin();
    let output = Command::new(bin)
        .env("HANSA_ROOT", root)
        .env("HANSA_KEY_HEX", key_hex())
        .env("HANSA_LABEL", label.to_string())
        .env("HANSA_AXIS", axis.to_string())
        .env("HANSA_ACTION", "populate")
        .output()
        .expect("spawn populate");
    if !output.status.success() {
        panic!(
            "populate label={label} failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn spawn_populate(root: &Path, label: u8, axis: usize) -> std::process::Child {
    let bin = agent_bin();
    Command::new(bin)
        .env("HANSA_ROOT", root)
        .env("HANSA_KEY_HEX", key_hex())
        .env("HANSA_LABEL", label.to_string())
        .env("HANSA_AXIS", axis.to_string())
        .env("HANSA_ACTION", "populate")
        .spawn()
        .expect("spawn populate")
}

fn run_query(root: &Path, label: u8, axis: usize, query_axis: usize) -> serde_json::Value {
    let bin = agent_bin();
    let output = Command::new(bin)
        .env("HANSA_ROOT", root)
        .env("HANSA_KEY_HEX", key_hex())
        .env("HANSA_LABEL", label.to_string())
        .env("HANSA_AXIS", axis.to_string())
        .env("HANSA_QUERY_AXIS", query_axis.to_string())
        .env("HANSA_ACTION", "query")
        .output()
        .expect("spawn query");
    if !output.status.success() {
        panic!(
            "query label={label} failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    serde_json::from_str(stdout.trim()).expect("valid JSON")
}

#[test]
fn three_agents_in_separate_processes_federate() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    // Spawn three populators sequentially. Concurrent spawning would
    // exercise the file-lock path too, but sequential is enough to
    // prove cross-process state survives.
    run_populate(root, 1, 0); // A on axis 0
    run_populate(root, 2, 1); // B on axis 1
    run_populate(root, 3, 2); // C on axis 2

    // Each populator should have left its tenant + saga files behind.
    for label in [1u8, 2, 3] {
        let meta = root.join(format!("tenant-{label}")).join("meta.json");
        assert!(meta.exists(), "missing {meta:?}");
    }

    // Now A queries near axis 1 - B's territory.
    let report = run_query(root, 1, 0, 1);
    let hits = report["hits"].as_array().expect("hits array");
    let member_count = report["member_count"].as_u64().unwrap();
    assert_eq!(member_count, 3, "wrong member count: {member_count}");

    let local: Vec<_> = hits
        .iter()
        .filter(|h| h["origin"].as_str() == Some("local"))
        .collect();
    let remote: Vec<_> = hits
        .iter()
        .filter(|h| h["origin"].as_str() == Some("remote"))
        .collect();
    assert!(!remote.is_empty(), "no remote hits: {hits:?}");

    // No remote hit may carry a non-shareable record id. In the agent
    // binary, ids `label*1000 .. label*1000+10` are shareable; the rest
    // are not.
    for h in &remote {
        let id = h["record_id"].as_u64().unwrap();
        let byte = h["tenant_byte"].as_u64().unwrap() as u8;
        let local_id = id - byte as u64 * 1000;
        assert!(
            local_id < 10,
            "non-shareable leaked from peer {byte}: id={id}"
        );
    }

    // Top hit should come from B (axis 1) since that's where the query lives.
    let top = &hits[0];
    assert_eq!(top["origin"].as_str(), Some("remote"));
    assert_eq!(
        top["tenant_byte"].as_u64(),
        Some(2),
        "top hit was not from B: {top:?}"
    );
    let _ = local;
}

#[test]
fn concurrent_populate_does_not_corrupt_registry() {
    // Three populators race to append to members.log + write their own
    // saga files. Hansa relies on `OpenOptions::append` (atomic for
    // <PIPE_BUF sized writes on POSIX) and on per-tenant write paths.
    // This test exercises both.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    let mut children: Vec<_> = (1u8..=3)
        .map(|label| spawn_populate(root, label, (label - 1) as usize))
        .collect();

    for child in &mut children {
        let status = child.wait().expect("wait child");
        assert!(status.success(), "child failed: {status:?}");
    }

    // After all three exit, agent A should see all three members.
    let report = run_query(root, 1, 0, 1);
    let members = report["member_count"].as_u64().unwrap();
    assert_eq!(members, 3, "concurrent populate lost members: {members}");
}
