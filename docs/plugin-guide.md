# Plugin guide

Hansa wires together four pieces it does not hard-code. Each is a
trait you can implement to swap in your own backend without touching
the query path. The defaults cover the local single-user case; replace
one when your deployment outgrows it.

| Trait | Default | Replace when |
| --- | --- | --- |
| [`Registry`] | `FileRegistry` (`~/.hansa/`) | members live somewhere other than one filesystem |
| [`Keystore`] | `EnvKeystore` / `FileKeystore` | the key belongs in an OS secret store |
| [`Tokenizer`] | `CharCountTokenizer` (chars/4) | the budget is a real model token cap |
| [`Ranker`] | `SimilarityRanker` | similarity alone is the wrong sort order |

Plus one function pointer, `peer_opener`, that turns a member's tenant
path into something queryable. That is the seam between hansa and the
storage backend.

## Registry

Member discovery. Three methods, all idempotent:

```rust
pub trait Registry: Send + Sync {
    fn join(&self, hansa: HansaId, member: MemberRecord) -> Result<()>;
    fn leave(&self, hansa: HansaId, tenant: TenantId) -> Result<()>;
    fn members(&self, hansa: HansaId) -> Result<Vec<MemberRecord>>;
}
```

`FileRegistry` is an append-only `members.log` plus a periodic snapshot,
compacted under an advisory lock. Implement `Registry` yourself to back
discovery with a database or a network service; the query path only
ever calls `members`, so a read-mostly implementation is fine.

## Keystore

Where the `HansaKey` lives. `load`/`store`/`remove` over a named slot:

```rust
pub trait Keystore: Send + Sync {
    fn load(&self, slot: &str) -> Result<HansaKey>;
    fn store(&self, slot: &str, key: &HansaKey) -> Result<()>;
    fn remove(&self, slot: &str) -> Result<()>;
}
```

Shipped: `EnvKeystore` (reads `HANSA_KEY_<SLOT>`, write/remove are
no-ops), `FileKeystore`, `MemoryKeystore` (tests). An OS-native
keystore backed by the platform secret store — macOS Keychain, Linux
Secret Service, Windows Credential Manager — lands in M3 behind a
feature flag.

## Tokenizer

The budget counts tokens; this trait decides what a token is.

```rust
pub trait Tokenizer: Send + Sync {
    fn count(&self, text: &str) -> usize;
}
```

`CharCountTokenizer` (the default) is `chars / 4`, ceiling-rounded so
non-empty content always costs at least one. It drifts up to ~30% from
a real BPE count, which is fine when the budget is a soft cap and wrong
when it is an OpenAI prompt limit you must not exceed. For that, enable
the `tiktoken` feature and use `TiktokenTokenizer`, which runs the exact
OpenAI BPE. It costs ~6 MB of embedded tables, so it is opt-in.

```toml
hansa = { version = "0.1", features = ["tiktoken"] }
```

## Ranker

How a context bundle is ordered after retrieval.

```rust
pub trait Ranker: Send + Sync {
    fn score(&self, item: &ContextItem) -> f32;  // higher = earlier
}
```

`SimilarityRanker` (default) sorts by raw similarity. `TokenDensityRanker`
sorts by `similarity / log2(1 + tokens)`, so a tight 50-token fact can
outrank a 500-token ramble that scores marginally higher — useful when
the bundle is going into a tokenizer-billed prompt and density matters
more than peak similarity.

## peer_opener

`HansaConfig::peer_opener` is `Option<Arc<dyn Fn(path) -> queryable>>`.
It is how hansa opens a peer's tenant for a read-only filtered query
without knowing the storage format. The reference implementation is
`skeg_rigging_skeg::open_readonly`. Set it to `None` to run local-only
(no fan-out); set it to your own opener to query a different backend.

With the `tokio` feature there is a second seam,
`HansaConfig::async_peer_opener`, used by `Hansa::query_async`. It plays
the same role for the async fan-out path, which spawns peer queries on
the Tokio runtime instead of `std::thread`.

## Putting it together

Everything is passed through `HansaConfig` at `Hansa::open`. Nothing is
global; you can run two hansas with different registries and keystores
in one process. See the README's API tour for a full config.
