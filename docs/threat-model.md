# Threat model

This is the long form of the trust section in the README. It exists
because v0.2 has real limits and shipping without naming them invites
misuse. Read it before you put anything you would not hand to every
key-holder behind a `shareable` flag.

## The one-sentence version

A hansa has two secrets with two jobs. The **`HansaKey`** (symmetric)
scopes confidentiality and discovery: holding it lets you read what was
shared. The **skipper** (ed25519 keypair) holds authority: only it can
admit or revoke members, and every membership change is a signed link in
a hash-chained log that each peer verifies on replay. So reading and
controlling-membership are now separate powers — a key-holder is no
longer automatically able to change who is in the group.

## What hansa protects against

- **Cross-hansa leakage.** Records in hansa A are invisible to members
  of hansa B, even when one process holds both keys. The membrane is
  scoped by `HansaKey`. There is no shared namespace to leak across.
- **Cross-tenant leakage inside a hansa.** A record is private to its
  owning tenant until that tenant marks it `shareable`. Sharing is
  opt-in per record, enforced at the source tenant during the query,
  not filtered after the fact.
- **Outsiders.** Without a valid `HansaKey` you cannot enumerate
  members, score sagas, or query. The `HansaId` is public and grants
  nothing on its own; it is a directory name, not a credential.
- **Forged or evicted membership.** Only the skipper can admit or
  revoke. A member (or anyone who can write the registry directory)
  cannot forge a peer, evict another member, or rewrite/reorder the log:
  every link is signed and hash-chained, so tampering fails replay. A
  signed revoke drops a member from every honest membrane on the next
  replay — no key rotation needed.
- **Rollback against a returning member.** With the opt-in head cache, a
  member refuses a chain whose head regressed below what it last
  verified (e.g. a truncated log dropping a later revoke).

## What hansa does not protect against (v0.2)

- **A compromised skipper key.** The skipper is the root of trust; if
  its secret leaks, the attacker can admit and revoke at will. There is
  no in-band rotation yet — recovery is founding a new hansa (new
  skipper -> new id) and re-admitting trusted members. Keep the skipper
  secret in an OS keystore, not a dotfile.
- **Malicious members reading shared data.** A key-holder still reads
  every `shareable` record from every member. The flag is a switch the
  owner controls, not an ACL the group enforces: if you would not let a
  peer read a record, do not mark it `shareable`.
- **Cross-tenant provenance.** When member A reads from B's tenant, the
  membership is signed but the *records* B serves are not. Per-record
  provenance signing is M6.
- **First-join rollback.** The head cache protects returning members; a
  first-time joiner has no prior head to compare, so an attacker
  controlling the filesystem at join time could present a truncated
  chain. Closing this needs an external witness (network registry, M6).
- **Network attackers.** v0.2 is filesystem-local. A networked registry
  (`HttpRegistry`, `RedisRegistry`) is M6 and changes this section.
- **Filesystem tampering as denial-of-service.** Integrity is now
  *detected* (signatures + chain), but anyone who can write
  `~/.hansa/<id>/` can still delete it. That is an availability /
  backup / OS-permissions concern, not an integrity one.

## Why the membrane filters at the source

A natural mistake is to fetch a peer's records and filter `shareable`
locally. Hansa does not: the `shareable` predicate runs inside the peer
query, so a non-shareable record never crosses the process boundary,
never lands in the requester's memory, and never appears in a crash
dump on the requesting side. The flag is checked where the data lives.

## Mapping limits to milestones

| Limit | Status |
| --- | --- |
| Per-member revocation | **done** (v0.2: signed `Revoke` link) |
| Authenticated membership | **done** (v0.2: ed25519-signed chain) |
| Bounded, tamper-evident log | **done** (v0.2: signed checkpoint compaction) |
| Skipper key in OS keystore | pending (`OsKeystore`, optional) |
| In-band skipper rotation | future (TUF-style, signed by old key) |
| Cross-tenant provenance | M6 (provenance signing) |
| First-join rollback / witness | M6 (network registry) |
| Filesystem-local only | M6 (network registry) |

For now: treat one hansa as one trust boundary, keep the **skipper**
secret in an OS keystore rather than a dotfile, lock down the registry
directory with normal file permissions, and point
`HansaConfig::head_cache_dir` at a path the registry writer does not
control to get the rollback guarantee.
