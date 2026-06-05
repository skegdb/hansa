# Deployment patterns

v0.2 targets one machine, one user, several agent processes. Within
that envelope these are the layouts that work and the knobs that
matter.

## The baseline: co-located agents

Several agent processes on one host, one human behind all of them, one
shared `~/.hansa/<id>/`. Each agent owns a tenant under `~/.skeg/` and
joins the hansa at startup. This is what the trust model assumes (see
[threat-model.md](threat-model.md)) and what `cargo run -p three-agents`
demonstrates.

```text
~/.hansa/<hansa-id>/
  members.log          # who is in the group
  members.snap         # compaction snapshot
  sagas/<tenant>.saga  # one digest per member
  lock                 # advisory lock for compaction
~/.skeg/tenants/<tenant>/   # each agent's own memory
```

Key handling: put the `HansaKey` in the environment (`EnvKeystore`,
`HANSA_KEY_<SLOT>`) for ephemeral processes, or `FileKeystore` for
long-lived ones. Either way, lock the registry directory down with
normal file permissions — that directory is the trust boundary.

## Saga freshness

A query scores peers by their saga, so a stale saga means a member gets
skipped for records it actually holds. Two ways to keep them current:

- **Manual.** Call `refresh_saga` after a batch of writes. Simple,
  predictable, fine for agents that write in bursts.
- **Background.** `start_background_refresh` runs a refresh task on an
  interval (`BackgroundRefreshConfig`), returning a `RefreshHandle` you
  `stop()` at shutdown. Use this for agents that write continuously and
  should not block on digesting their own memory. This is the M2
  replacement for hand-rolled refresh loops.

Refreshing is not free — it samples the tenant and rebuilds centroids —
so match the interval to write volume rather than running it tight by
default.

## Budgets and the query knobs

Fan-out is bounded, not best-effort. The levers, all on the query
builder:

- `budget(TokenBudget)` — caps how much remote content crosses the
  membrane, counted by the active [`Tokenizer`]. Splits across the
  top-scoring peers.
- `top_k(n)` — hard ceiling on peers contacted, regardless of budget.
- `min_similarity(t)` — drops peers whose saga scores below `t` before
  any budget is spent.
- `deadline(d)` — wall-clock cap; peers that miss it are dropped, not
  waited on. A slow or hung peer cannot stall the query.
- `local_only()` — skip fan-out entirely; query only this agent's
  tenant.

A peer that is offline, lock-contended, or holding a corrupt saga is
logged and skipped. A query never aborts because a peer failed.

## Sync vs async fan-out

The default fan-out parallelises peer queries across a `rayon` pool —
right for a CPU-bound, thread-per-peer workload on one box. If your
agent already runs on Tokio (for example it also serves a network API),
enable the `tokio` feature and use `Hansa::query_async`, which spawns
peer queries as Tokio tasks instead and integrates with your existing
runtime rather than standing up a second thread pool beside it.

## What v0.2 is not for

- **Multiple humans sharing read access.** A key-holder still reads
  everything marked `shareable`. Membership is now skipper-controlled,
  but the `HansaKey` is not a per-user read boundary.
- **Cross-machine.** The registry and sagas are filesystem paths. A
  networked registry is M6.
- **An untrusted skipper.** The skipper is the root of trust; a
  compromised skipper key compromises the hansa (no in-band rotation
  yet).

If your deployment needs any of those, the shape changes — track the
relevant milestone in [the roadmap](../private/roadmap.md) rather than
working around the trust model.
