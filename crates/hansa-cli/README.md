# hansa

A command-line front end for [hansa](../hansa): create a trust group of
agent memories, feed it, and query it, without writing any Rust.

hansa lets several agents keep their own private memories and selectively
share some of them. A query made by one agent fans out, under a token
budget, to peers that look like they hold relevant records. This CLI wraps
that: each agent is a local skeg vault; the group is bound by a shared
**passphrase**.

## Requirements

- An [Ollama](https://ollama.com)-compatible embedding endpoint running
  locally (default `http://localhost:11434`) with an embedding model
  pulled (default `mxbai-embed-large`). hansa stores vectors, not text, so
  text is embedded caller-side before it is stored or queried.

## Install

```sh
cargo install --path crates/hansa-cli   # from the hansa checkout
# or, once published:
# cargo install hansa-cli
```

## The secret model

What binds a hansa together is a **passphrase**, not a key file. Anyone who
runs `hansa init` with the same hansa *name* and *passphrase* derives the
same trust-group key and joins the same group. The passphrase is the only
thing you share with another person or machine.

```sh
hansa init --name team --tenant me --passphrase "correct horse battery"
```

## Agents on one machine

One machine can run several agents (members). They share `~/.hansa`.

```sh
hansa init --name team --tenant me   --passphrase ...   # add an agent
hansa init --name team --tenant bot  --passphrase ...   # add another
hansa agents            # list local agents; ● marks the active one
hansa use bot           # switch the active agent
hansa leave --tenant bot   # remove an agent (vault files are kept)
```

> Cross-**machine** federation is not available yet (it needs the network
> registry, hansa milestone M6). Today, agents that federate must share the
> same `~/.hansa` registry on one machine.

## Storing memories

```sh
hansa remember "the prod token lives in 1password, vault Ops"          # private
hansa remember "skeg uses an S3-FIFO cache" --share --tag cache        # shared with peers
```

Memories are **private by default**; `--share` makes a record visible to
federation peers.

## Ingesting files

`ingest` walks a file or directory, chunks each file, embeds every chunk,
and stores it. Unlike `remember`, ingested records are **shared by
default** (the usual intent is to load a corpus the group can query); pass
`--private` to keep them local.

```sh
hansa ingest ./docs --ext md --tag handbook        # one-shot
hansa ingest ./notes/meeting.md                     # a single file
hansa watch  ./docs --ext md                         # live: ingest on change
```

Each record stores the chunk text as its payload and a `src:<relpath>:<lines>`
tag for provenance.

### `watch` quirks (read before relying on it)

`watch` is a v1 and has two user-facing behaviours worth knowing:

1. **Re-edits append.** A re-edited file appends fresh records rather than
   replacing its previous chunks; record ids are not yet stable per chunk.
   Editing the same file repeatedly grows the vault. (A v2 will use a
   deterministic per-chunk id and delete-then-insert to make re-ingest
   idempotent.)
2. **The saga is not refreshed while watching.** The saga is the digest
   peers use to decide whether to ask this agent during a fan-out.
   Refreshing it mid-watch would open a second writer on the same vault, so
   `watch` deliberately skips it. After a watch session, or any time you
   want peers to see freshly ingested records immediately, run:

   ```sh
   hansa saga
   ```

   A normal `remember` or one-shot `ingest` already refreshes the saga, so
   this only matters for `watch`.

## Querying

```sh
hansa query "where is the prod token?" -k 5
```

Answers fan out to peers under a token budget (`--budget`). Each hit shows
its similarity, where it came from (`you` or `peer <id>`), the record id,
and the text.

## Other commands

```sh
hansa members     # members of the active agent's hansa
hansa status      # active agent: id, vault, memory count, embedder
hansa forget <id> # delete one local memory
hansa revoke <hex># evict a member (skipper only)
hansa key         # show the hansa id and how peers join
```

## Where data lives

```
~/.hansa/
  cli.toml                       # local agents + the active one
  keys/                          # FileKeystore: derived trust-group keys
  <hansa-id>/
    tenant-<name>/               # this agent's skeg vault (DiskVamana)
    sagas/                       # saga digests (own + peers')
  members.log                    # signed, hash-chained roster
```

## Known limitations

- Single-machine federation only (M6 network registry pending).
- In the deterministic passphrase-to-key model, anyone with the passphrase is
  skipper-capable; `revoke` suits a trusted team, not an adversarial setting.
- `watch` re-edits append and do not refresh the saga (see above).
