# Changelog

All notable changes to hansa are recorded here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
semantic-ish minor versions tied to roadmap milestones.

## [0.2.0] - 2026-06-03

The trust model becomes asymmetric: a hansa now has a signing authority
(the *skipper*) and a signed, tamper-evident membership log. Reading and
controlling-membership are separate powers.

### Added

- **Skipper (ed25519).** `Skipper` / `SkipperPub` / `Sig` signing
  primitives (`verify_strict`, weak-key rejection, canonical
  domain-separated encoder). `Skipper::from_hansa_key` derives the
  authority from the shared key for the single-user case.
- **Signed genesis + identity.** Founding mints a skipper and writes a
  signed genesis; `HansaId` v2 commits to the skipper key
  (`HansaId::from_skipper`), so knowing the id pins it.
- **Signed members log.** `members.log` is a hash-chained log of
  skipper-signed links (`Genesis` / `Admit` / `Revoke` / `Checkpoint`);
  replay verifies seq, prev-hash, signer, and signature on every link.
  Chain extension is serialized under a per-hansa lock.
- **Selective revocation.** `Hansa::revoke` (and `leave`) emit signed
  revokes, enforced at the membrane: a revoked peer stops crossing on
  the next replay.
- **Signed checkpoint compaction.** `Hansa::compact` (and automatic
  compaction past a threshold) collapses the log to one signed
  checkpoint, keeping it bounded without becoming an unsigned rewrite.
- **Anti-rollback head cache.** Opt-in via `HansaConfig::head_cache_dir`:
  refuses a chain whose head regressed below the last verified one.
- **`migrate_v1`.** Migrate a v1 unsigned hansa to a fresh v2 signed one,
  re-admitting the active members.
- **Lifecycle traits.** `TenantInfo` and `TenantLifecycle` on `Hansa`,
  exposing `hansa.member` / `hansa.membrane` capabilities.
- Exact token counting (`tiktoken` feature) and an async query path
  (`tokio` feature, `Hansa::query_async`).

### Changed

- `HansaConfig` gains `skipper`, `hansa_id`, and `head_cache_dir`. The id
  is derived from the skipper, not the symmetric key.
- The `Registry` trait is now a transport for the signed chain
  (`append_link` / `read_chain` / `found` / `append_next` / `compact`);
  `members()` replays and verifies.
- README, threat model, deployment, and plugin docs updated to v0.2.

### Deferred / known limits

- `OsKeystore` (platform secret store via `keyring`) — optional, not yet
  shipped.
- In-band skipper rotation; per-record provenance signing; cross-machine
  member federation (the `hybrid-demo` example is parked until the
  network registry lands).
- First-join rollback needs an external witness (network registry).

## [0.1.x] - earlier

Foundation: `HansaKey`/`HansaId`, keystores, filesystem registry, saga
digests, the membrane query path with `shareable` filtering and
provenance, background saga refresh, and the docs set. Trust model was a
symmetric shared secret (trusted equals).
