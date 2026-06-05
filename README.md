# hansa

> *Federation primitive for skeg. Lets local AI agents form trust
> groups and query across each other's memory without dumping context.*

A **hansa** is a trust group of agents that hold the same
[`HansaKey`]. Each agent keeps its own memory private by default and
opts in per record via a `shareable` flag. A query made through one
member's [`Hansa`] handle can fan out, under an explicit token budget,
to peers whose [`Saga`] (digest) suggests they hold relevant records.

The mental model is the Hanseatic League: autonomous entities forming
alliances of mutual commerce, each sovereign in its own port, sharing
routes when it serves them. **Not** a distributed database, **not** a
synchronization protocol, **not** a hive mind. A discovery + access
mechanism with explicit cost accounting.

## v0.1 - M1 Foundation

A single user, several agent processes on one machine, filesystem-local
discovery. The trust model is shared secret: anyone with the key is a
trusted equal.

- [`HansaKey`] (32-byte secret, zeroized on drop, BLAKE3-KDF from passphrase)
- [`HansaId`] (`blake3(key || "hansa-id-v1")`, public)
- Three [`Keystore`] impls: [`EnvKeystore`], [`FileKeystore`], [`MemoryKeystore`]
- [`FileRegistry`]: append-only `members.log` + advisory-lock compaction
- [`Saga`]: condensed memory digest (k-means++ over a reservoir sample, top-N tag aggregate)
  persisted via [`skeg-hull`](https://github.com/skegdb/skeg-hull) SagaV1
- Membrane query path: local query + saga-scored peer fan-out under
  [`TokenBudget`] cap, parallel via `rayon`, `shareable` filter at the source
- Hit set carries [`HitOrigin`] (Local vs `Remote { tenant_id }`)
- Peer failure (offline, locked, corrupt) → log + skip, never aborts the query

Out of scope for v0.1: distributed consistency, cross-machine
federation, selective revocation, provenance signing, confidentiality
against malicious key-holders. See [private/hansa.md][hansa-design]
§1.2 for the full list; [private/roadmap.md][roadmap] for what M2-M6
add.

## Walkthrough - three agents

```sh
cargo run -p three-agents
```

Three in-process agents (work / research / code) populate distinct
domains, mark some records private, join one hansa. The program then
runs three queries and prints the hit set with provenance and the
records that *would* have matched but were blocked by the `shareable`
flag - so you can see exactly what crossed the membrane and what did
not.

## Cross-process

```sh
cargo test -p hansa --test cross_process
```

Spawns three real OS processes that share one filesystem root, joins
the hansa from each, then queries from a fourth process. Validates
that `members.log`, `.saga` files, and the JSON sidecar all survive
across pid boundaries. A `concurrent_populate` variant runs the three
populators in parallel to exercise the registry's append path under
race conditions.

## Quick API tour

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
    skipper: Some(skipper), // hold it to found and admit/revoke; None = read-only member
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

h.join(/* tags */ Vec::<String>::new())?; // the skipper admits the local tenant

let hits = h
    .query(&query_embedding)?
    .top_k(10)
    .budget(TokenBudget::split(20, 30))
    .execute()?;
```

## Storage layout

```text
~/.hansa/<hansa-id-hex>/
  members.log          # newline-delimited JSON events
  members.snap         # periodic snapshot
  sagas/<tenant>.saga  # SagaV1 binary digest per member
  lock                 # advisory lock for compaction
```

## Trust model (v0.2)

Membership is a **skipper-signed, hash-chained log**. A hansa is founded
with an ed25519 keypair (the skipper); the id commits to the skipper key,
so knowing the id pins it. Every admit and revoke is a signed link, and a
member is trusted only if a signature vouches for it — verified on replay,
not taken on faith from the filesystem.

Hansa protects against:

- accidental cross-hansa / cross-tenant leakage (records scoped by key;
  `shareable: false` never crosses)
- outsiders without the key (HansaId alone grants nothing)
- forged or evicted membership (only the skipper can admit/revoke; a
  rewritten or reordered log fails replay)
- rollback against a returning member (opt-in head cache)

Hansa does **not** protect against:

- a compromised **skipper** key (recovery is founding a new hansa)
- malicious key-holders reading what was marked `shareable`
- network attackers (v0.2 is filesystem-local)
- filesystem tampering as a denial-of-service (integrity is detected,
  availability is an OS-permissions / backup concern)

The full version, with the reasoning and the milestone that lifts each
limit, is in [docs/threat-model.md](docs/threat-model.md).

## Guides

- [docs/plugin-guide.md](docs/plugin-guide.md) - the four traits hansa
  lets you swap (`Registry`, `Keystore`, `Tokenizer`, `Ranker`) plus
  the `peer_opener` seam.
- [docs/deployment.md](docs/deployment.md) - layouts that work in v0.1,
  saga freshness, the query budget knobs, sync vs async fan-out.
- [docs/threat-model.md](docs/threat-model.md) - what one shared key
  does and does not buy you.

## Roadmap

See [private/roadmap.md][roadmap]:

| Milestone       | Status   | What it adds                                                                       |
| --------------- | -------- | ---------------------------------------------------------------------------------- |
| M1 Foundation   | **done** | join/leave/query end-to-end                                                        |
| M2 Hardening    | **done** | background saga refresh, threat-model/plugin/deployment docs                       |
| M3 Lifecycle    | **done** | `TenantLifecycle`/`TenantInfo` on Hansa, skipper-signed roster, revocation         |
| M4 Events       | next     | `TenantEvents` push notifications, pheromone trail                                 |
| M5 Accounting   | future   | quotas, stats, searchable encryption                                               |
| M6 Engine-ready | future   | network registry, spawn/seed/sign                                                  |

## Planned features

- [private/features.md][features] - master feature index across the
  five repos (hansa, skeg-rigging, skeg-hull, skeg-rigging-net,
  skeg-rigging-skeg-tenant). Each entry carries status, milestone, and
  dependencies.
- [private/design-token-efficiency.md][token-eff] - design for the
  token-saving features: semantic dedup, density ranking, negative
  caching, provenance-collapsed rendering, bundle caching, binary
  wire format.
- [private/design-operational-efficiency.md][op-eff] - design for the
  performance work: RESP3 connection pooling, HTTP saga distribution,
  async query path, pheromone trail (peer reputation), incremental
  saga refresh, membrane latency budgets, RESP3 push notifications,
  quantised saga centroids.

## Building, testing, benching

```sh
# Build the lot
cargo build --workspace

# All correctness tests (unit + integration + cross-process)
cargo test --workspace

# Performance + token-efficiency gates (CI gating, see private/gates.md)
cargo test --release --test gates

# Informational benchmark snapshot (pretty output, no gate enforcement)
cargo run --release -p bench-report

# Criterion detailed benches (writes HTML to target/criterion/)
cargo bench --bench saga_build
cargo bench --bench saga_score
cargo bench --bench context_assembly

# Walkthrough demo (three agents sharing knowledge)
cargo run -p three-agents
```

Gate thresholds live in [`private/gates.md`][gates] and are enforced
by the `gates` test. Touching one requires an explicit reason in the
commit message; see §3 of that doc.

## License

Apache-2.0.

[hansa-design]: ./private/hansa.md
[features]: ./private/features.md
[token-eff]: ./private/design-token-efficiency.md
[op-eff]: ./private/design-operational-efficiency.md
[roadmap]: ./private/roadmap.md
[gates]: ./private/gates.md
