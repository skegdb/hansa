# Threat model

This is the long form of the trust section in the README. It exists
because v0.1 has real limits and shipping without naming them invites
misuse. Read it before you put anything you would not hand to every
key-holder behind a `shareable` flag.

## The one-sentence version

A hansa is a group of agents that trust each other as equals because
they share one secret. Anyone holding the [`HansaKey`] is inside; the
membrane keeps records private until a holder opts one in. That is the
whole guarantee, and it is symmetric: there is no inside-the-group
privilege boundary in v0.1.

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

## What hansa does not protect against (v0.1)

- **Malicious members.** A key-holder reads every `shareable` record
  from every member and can write arbitrary records into its own
  tenant. The model is trusted equals. If you would not let a peer read
  a record, do not mark it `shareable` and do not assume the flag is a
  permission system — it is a switch the owner controls, not an ACL the
  group enforces.
- **Compromised keys.** A leaked `HansaKey` compromises the whole
  hansa. There is no per-member revocation in v0.1. Recovery is manual:
  generate a new key, hand it to the members you still trust, abandon
  the old hansa directory. Selective, signed revocation arrives in M3
  with the skipper keypair.
- **Forged remote identities.** When member A reads from member B's
  tenant, A trusts that B is B. Nothing is signed in v0.1, so a member
  that can write to the registry directory can also impersonate a
  tenant id. Provenance signing is M6.
- **Network attackers.** There is no network surface in v0.1. The
  registry, sagas, and tenants are all on one filesystem. A networked
  registry (`HttpRegistry`, `RedisRegistry`) is M6 and changes this
  section.
- **Filesystem tampering.** Anyone who can write `~/.hansa/<id>/` can
  inject phantom members or swap a saga. The defense is OS file
  permissions on that directory; hansa does not re-implement access
  control on top of the filesystem.

## Why the membrane filters at the source

A natural mistake is to fetch a peer's records and filter `shareable`
locally. Hansa does not: the `shareable` predicate runs inside the peer
query, so a non-shareable record never crosses the process boundary,
never lands in the requester's memory, and never appears in a crash
dump on the requesting side. The flag is checked where the data lives.

## Mapping limits to milestones

| Limit | Lifted in |
| --- | --- |
| No per-member revocation | M3 (skipper keypair, signed `revoke`) |
| Members are unauthenticated | M3 (ed25519-signed `members.log`) |
| No cross-tenant provenance | M6 (provenance signing) |
| Filesystem-local only | M6 (network registry) |

Until then: treat one hansa as one trust boundary, keep the key in an
OS keystore rather than a dotfile, and lock down the registry directory
with normal file permissions.
