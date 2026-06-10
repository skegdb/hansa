# hansa

Federation primitive for [skeg](https://github.com/skegdb/skeg). Lets local AI
agents form trust groups and query across each other's memory without dumping
context.

A **hansa** is a trust group of agents that share one secret key. Each agent
keeps its own memory private by default and opts a record into sharing with a
`shareable` flag. A query made through one member's handle can fan out, under an
explicit token budget, to the peers whose digest suggests they hold relevant
records. Only records marked shareable ever leave their owner.

The mental model is the Hanseatic League: autonomous entities forming alliances
of mutual commerce, each sovereign in its own port, sharing routes when it
serves them. It is not a distributed database, not a synchronization protocol,
and not a hive mind. It is a discovery and access mechanism with explicit cost
accounting.

## What it is

hansa is a Rust library. You embed it in an agent process, point it at a local
skeg vault, and join a hansa. Membership is a signed, hash-chained roster on the
filesystem; queries read peers' vaults read-only and respect each record's
shareable flag at the source.

Version 0.2 targets a single user running several agent processes on one
machine, with filesystem-local discovery. Cross-machine federation is not in
this release.

A command-line front end, [`hansa-cli`](crates/hansa-cli), wraps the library so
the same trust groups can be created, fed, and queried without writing any Rust.

## Install

The library:

```sh
cargo add hansa
```

The command-line tool:

```sh
cargo install hansa-cli
```

Both require a Rust toolchain (MSRV 1.88).

## Command-line quickstart

The secret that binds a hansa is a passphrase. Anyone who runs `hansa init` with
the same hansa name and passphrase joins the same group. Storing and querying
embed text through a local [Ollama](https://ollama.com)-compatible endpoint, so
have one running with an embedding model pulled.

```sh
hansa init --name team --tenant me --passphrase "correct horse battery"
hansa remember "the deploy runbook is in ops/deploy.md" --share
hansa ingest ./docs --ext md          # embed a whole directory
hansa query "where is the deploy runbook?"
```

See the [CLI guide](crates/hansa-cli/README.md) for the full command set
(agents, ingest, watch, members, revoke).

## Library API tour

```rust
use std::sync::Arc;
use hansa::prelude::*;
use skeg_rigging::TenantId;
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

let key = HansaKey::generate();
let skipper = Skipper::generate(); // the signing authority that founds the hansa
let tenant = Arc::new(Tenant::open("/path/to/vault", TenantId::ZERO, 768)?);

let h = Hansa::open(HansaConfig {
    key,
    skipper: Some(skipper), // hold it to found and admit/revoke; None is a read-only member
    hansa_id: None,         // derived from the skipper; a joiner passes Some(id) instead
    registry: Arc::new(FileRegistry::default_root()),
    local_tenant: tenant.clone(),
    local_tenant_id: TenantId::ZERO,
    local_tenant_location: TenantLocation::Path { path: "/path/to/vault".into() },
    saga_dir: "/home/me/.hansa/<id>/sagas".into(),
    peer_opener: Some(Arc::new(|_id, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(skeg_rigging::OpenError::NotFound),
    })),
    default_budget: TokenBudget::default(),
    head_cache_dir: None,   // Some(dir) enables anti-rollback
})?;

h.join(Vec::<String>::new())?; // the skipper admits the local tenant

let hits = h
    .query(&query_embedding)?
    .top_k(10)
    .budget(TokenBudget::split(20, 30))
    .execute()?;
```

The `three-agents` example is a full in-process walkthrough: three agents
populate distinct domains, mark some records private, join one hansa, and run
queries that show exactly what crossed the membrane and what the shareable flag
held back.

```sh
cargo run -p three-agents
```

## Trust model

Membership is a skipper-signed, hash-chained log. A hansa is founded with an
ed25519 keypair (the skipper); the hansa id commits to the skipper key, so
knowing the id pins it. Every admit and revoke is a signed link, and a member is
trusted only if a signature vouches for it, verified on replay rather than taken
on faith from the filesystem.

hansa protects against:

- accidental cross-hansa or cross-tenant leakage; records are scoped by key, and
  `shareable: false` never crosses.
- outsiders without the key; the hansa id alone grants nothing.
- forged or evicted membership; only the skipper can admit or revoke, and a
  rewritten or reordered log fails replay.
- rollback against a returning member, via the opt-in head cache.

hansa does not protect against:

- a compromised skipper key; recovery is founding a new hansa.
- key-holders reading what was marked shareable.
- network attackers; version 0.2 is filesystem-local.
- filesystem tampering as denial of service; integrity is detected, but
  availability is an OS-permissions and backup concern.

The reasoning behind each line is in
[docs/threat-model.md](docs/threat-model.md).

## Storage layout

```text
~/.hansa/<hansa-id-hex>/
  members.log          # signed, hash-chained membership events
  sagas/<tenant>.saga  # binary memory digest per member
  lock                 # advisory lock for compaction
```

## Status

Working in 0.2:

- Passphrase-derived key, ed25519 skipper, three keystore backends (env, file,
  in-memory).
- Signed, hash-chained `members.log` with checkpoint compaction and replay
  verification; selective revocation; opt-in anti-rollback head cache.
- Memory digest (saga) per member, persisted in skeg's on-disk format.
- Membrane query path: local query plus saga-scored peer fan-out under a token
  budget, parallelized, with the shareable filter applied at the source.
- A peer that is offline, locked, or corrupt is logged and skipped; it never
  aborts the query.

Not in 0.2:

- Cross-machine federation, distributed consistency, per-record provenance
  signing, in-band skipper rotation, and confidentiality against key-holders who
  read shared records.

## Documentation

Guides live in this repository:

- [docs/threat-model.md](docs/threat-model.md): what one shared key does and does
  not buy you.
- [docs/plugin-guide.md](docs/plugin-guide.md): the four traits hansa lets you
  swap (registry, keystore, tokenizer, ranker) and the peer-opener seam.
- [docs/deployment.md](docs/deployment.md): layouts that work in 0.2, saga
  freshness, the query budget knobs, and sync versus async fan-out.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
cargo run -p three-agents      # the walkthrough demo
```

## License

[Apache-2.0](LICENSE).
