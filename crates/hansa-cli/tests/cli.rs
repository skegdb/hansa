//! End-to-end tests of the `hansa` binary.
//!
//! Every test runs against a throwaway `HOME` (so it touches no real
//! `~/.hansa`) and uses `--embed-url stub`, the deterministic in-process
//! embedder — so these need neither Ollama nor a skeg server and run in CI.

use std::path::Path;
use std::process::Command;

use assert_cmd::prelude::*;
use tempfile::TempDir;

/// A `hansa` command bound to an isolated HOME.
fn hansa(home: &Path) -> Command {
    let mut c = Command::cargo_bin("hansa").unwrap();
    c.env("HOME", home);
    c
}

/// Fresh HOME + an initialised agent using the stub embedder.
fn init_agent(tenant: &str) -> TempDir {
    let home = TempDir::new().unwrap();
    hansa(home.path())
        .args(["init", "--name", "t", "--tenant", tenant, "--passphrase", "p", "--embed-url", "stub"])
        .assert()
        .success();
    home
}

#[test]
fn init_reports_identity_and_status() {
    let home = init_agent("me");
    hansa(home.path())
        .arg("status")
        .assert()
        .success()
        .stdout(predicates::str::contains("agent    me"))
        .stdout(predicates::str::contains("memories 0"))
        .stdout(predicates::str::contains("dim 64"));
}

#[test]
fn remember_then_query_finds_it() {
    let home = init_agent("me");
    hansa(home.path())
        .args(["remember", "alpha caching fact", "--share"])
        .assert()
        .success();
    hansa(home.path())
        .args(["query", "caching", "-k", "5"])
        .assert()
        .success()
        .stdout(predicates::str::contains("alpha caching fact"));
}

#[test]
fn private_memory_is_stored() {
    let home = init_agent("me");
    hansa(home.path())
        .args(["remember", "secret beta note"])
        .assert()
        .success()
        .stdout(predicates::str::contains("private"));
    hansa(home.path())
        .arg("status")
        .assert()
        .success()
        .stdout(predicates::str::contains("memories 1"));
}

#[test]
fn ingest_directory_then_query() {
    let home = init_agent("me");
    let docs = home.path().join("docs");
    std::fs::create_dir_all(&docs).unwrap();
    std::fs::write(docs.join("a.md"), "gamma deployment runbook lives in ops.\n").unwrap();
    std::fs::write(docs.join("b.md"), "delta unrelated content.\n").unwrap();

    hansa(home.path())
        .args(["ingest", docs.to_str().unwrap(), "--ext", "md"])
        .assert()
        .success()
        .stdout(predicates::str::contains("2 file(s)"));

    hansa(home.path())
        .args(["query", "deployment runbook", "-k", "5"])
        .assert()
        .success()
        .stdout(predicates::str::contains("gamma deployment runbook"));
}

#[test]
fn forget_removes_a_memory() {
    let home = init_agent("me");
    hansa(home.path()).args(["remember", "to be forgotten"]).assert().success();
    hansa(home.path())
        .args(["forget", "0"])
        .assert()
        .success()
        .stdout(predicates::str::contains("forgot #0"));
}

#[test]
fn agents_add_switch_and_leave() {
    let home = init_agent("me");
    // Add a second agent in the same hansa (same passphrase).
    hansa(home.path())
        .args(["init", "--name", "t", "--tenant", "bot", "--passphrase", "p", "--embed-url", "stub"])
        .assert()
        .success();
    hansa(home.path())
        .arg("agents")
        .assert()
        .success()
        .stdout(predicates::str::contains("me"))
        .stdout(predicates::str::contains("bot"));
    hansa(home.path())
        .args(["use", "me"])
        .assert()
        .success()
        .stdout(predicates::str::contains("now using 'me'"));
    hansa(home.path())
        .args(["leave", "--tenant", "bot"])
        .assert()
        .success()
        .stdout(predicates::str::contains("left hansa"));
}

#[test]
fn federation_shares_only_shared_records() {
    // Two agents, same hansa, same HOME (shared registry).
    let home = init_agent("me");
    hansa(home.path()).args(["remember", "my private grocery list"]).assert().success();
    hansa(home.path())
        .args(["init", "--name", "t", "--tenant", "bob", "--passphrase", "p", "--embed-url", "stub"])
        .assert()
        .success();
    // bob shares one record, then refreshes its saga.
    hansa(home.path())
        .args(["remember", "epsilon deploy token in vault", "--share"])
        .assert()
        .success();
    hansa(home.path()).arg("saga").assert().success();

    // me refreshes its saga and queries bob's shared knowledge.
    hansa(home.path()).args(["use", "me"]).assert().success();
    hansa(home.path()).arg("saga").assert().success();
    hansa(home.path())
        .args(["query", "epsilon deploy token", "-k", "5"])
        .assert()
        .success()
        .stdout(predicates::str::contains("epsilon deploy token"))
        .stdout(predicates::str::contains("peer"));
}
