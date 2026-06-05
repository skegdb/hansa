//! Hansa registry: how members discover each other.
//!
//! The registry is a *transport* for the signed members chain, not a
//! trusted store. It persists and returns [`Link`]s; authority comes
//! from the skipper signatures inside them, verified on replay (see
//! [`crate::chain`]). A process that can write the log therefore still
//! cannot forge or evict membership.
//!
//! [`FileRegistry`] is the filesystem-local implementation: one
//! append-only newline-delimited JSON `members.log` per hansa, suitable
//! for one user with several agent processes on one machine.
//!
//! Compaction (a signed checkpoint that truncates the log) is a later
//! phase; for now the log is purely append-only.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use fs2::FileExt;

use crate::chain::{Body, Link, replay};
use crate::genesis::Genesis;
use crate::member::MemberRecord;
use crate::sign::Skipper;
use crate::{HansaError, HansaId, Result};

/// Transport for a hansa's signed members chain.
pub trait Registry: Send + Sync {
    /// Append one already-signed link verbatim. Low-level: the caller is
    /// responsible for `seq`/`prev`. Prefer [`Self::append_next`], which
    /// computes them under a lock.
    fn append_link(&self, hansa: HansaId, link: &Link) -> Result<()>;

    /// Read the full chain for `hansa`, oldest link first. Returns an
    /// empty vec when the hansa has no log yet (not founded).
    fn read_chain(&self, hansa: HansaId) -> Result<Vec<Link>>;

    /// Found the hansa: write a signed genesis as seq 0. Idempotent and
    /// serialized — concurrent founders do not fork the chain; the
    /// second is a no-op.
    fn found(
        &self,
        hansa: HansaId,
        skipper: &Skipper,
        embedding_dim: u32,
        created_at: i64,
    ) -> Result<()>;

    /// Append an admit/revoke `body`, computing `seq`/`prev` from the
    /// current head under a lock so concurrent writers cannot fork the
    /// chain. The chain is replayed (and verified) before extension.
    fn append_next(&self, hansa: HansaId, skipper: &Skipper, body: Body) -> Result<()>;

    /// Collapse the chain into a single signed checkpoint carrying the
    /// active set, truncating the log. Skipper-only and locked. A
    /// replayer trusts the compacted state by the checkpoint signature,
    /// so this is not an unsigned rewrite.
    fn compact(&self, hansa: HansaId, skipper: &Skipper) -> Result<()>;

    /// The currently-active members, by replaying and verifying the
    /// chain. An empty (un-founded) chain yields no members.
    fn members(&self, hansa: HansaId) -> Result<Vec<MemberRecord>> {
        let chain = self.read_chain(hansa)?;
        if chain.is_empty() {
            return Ok(Vec::new());
        }
        Ok(replay(&chain, None)?.active)
    }
}

/// Filesystem-local registry. Default root is `~/.hansa/`.
pub struct FileRegistry {
    /// Root directory; each hansa gets a subdirectory keyed by its id.
    pub root: PathBuf,
}

impl FileRegistry {
    /// Construct against an explicit root directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Default root: `$HOME/.hansa`. Falls back to `./.hansa` if `HOME`
    /// is unset.
    pub fn default_root() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self::new(home.join(".hansa"))
    }

    /// Path to a hansa's directory under [`Self::root`].
    pub fn hansa_dir(&self, hansa: HansaId) -> PathBuf {
        self.root.join(hansa.as_hex())
    }

    fn log_path(&self, hansa: HansaId) -> PathBuf {
        self.hansa_dir(hansa).join("members.log")
    }

    fn ensure_dir(&self, hansa: HansaId) -> Result<()> {
        std::fs::create_dir_all(self.hansa_dir(hansa))?;
        Ok(())
    }

    fn lock_path(&self, hansa: HansaId) -> PathBuf {
        self.hansa_dir(hansa).join("lock")
    }

    /// Run `f` while holding the per-hansa advisory lock, so chain
    /// extension (read head → append) is serialized across processes.
    fn with_lock<R>(&self, hansa: HansaId, f: impl FnOnce() -> Result<R>) -> Result<R> {
        self.ensure_dir(hansa)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.lock_path(hansa))?;
        lock.lock_exclusive()?;
        let r = f();
        let _ = FileExt::unlock(&lock);
        r
    }

    /// Replace the whole log atomically (temp file + rename).
    fn write_chain_atomic(&self, hansa: HansaId, links: &[Link]) -> Result<()> {
        let path = self.log_path(hansa);
        let tmp = path.with_extension("log.tmp");
        let mut buf = String::new();
        for link in links {
            buf.push_str(&serde_json::to_string(link)?);
            buf.push('\n');
        }
        std::fs::write(&tmp, buf.as_bytes())?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Collapse the chain to one signed checkpoint. Assumes the lock is
    /// already held.
    fn checkpoint_locked(&self, hansa: HansaId, skipper: &Skipper) -> Result<()> {
        let chain = self.read_chain(hansa)?;
        if chain.len() <= 1 {
            return Ok(()); // nothing but a root to compact
        }
        let out = replay(&chain, None)?;
        let checkpoint = Link::signed(
            skipper,
            out.head_seq + 1,
            out.head_hash,
            Body::Checkpoint {
                members: out.active,
                embedding_dim: out.embedding_dim,
                at: now_unix_seconds(),
            },
        );
        self.write_chain_atomic(hansa, &[checkpoint])
    }
}

/// Once a chain exceeds this many links, `append_next` collapses it to a
/// signed checkpoint.
const CHECKPOINT_AFTER_LINKS: usize = 256;

fn now_unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Registry for FileRegistry {
    fn append_link(&self, hansa: HansaId, link: &Link) -> Result<()> {
        self.ensure_dir(hansa)?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path(hansa))?;
        let mut buf = serde_json::to_string(link)?;
        buf.push('\n');
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    fn read_chain(&self, hansa: HansaId) -> Result<Vec<Link>> {
        let path = self.log_path(hansa);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut links = Vec::new();
        for (lineno, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let link: Link = serde_json::from_str(&line).map_err(|e| {
                HansaError::RegistryMalformed(format!("{}:{}: {e}", path.display(), lineno + 1))
            })?;
            links.push(link);
        }
        Ok(links)
    }

    fn found(
        &self,
        hansa: HansaId,
        skipper: &Skipper,
        embedding_dim: u32,
        created_at: i64,
    ) -> Result<()> {
        self.with_lock(hansa, || {
            if !self.read_chain(hansa)?.is_empty() {
                return Ok(()); // already founded
            }
            let (g, sig) = Genesis::found(skipper, embedding_dim, created_at, false);
            self.append_link(hansa, &Link::genesis(g, sig))
        })
    }

    fn append_next(&self, hansa: HansaId, skipper: &Skipper, body: Body) -> Result<()> {
        self.with_lock(hansa, || {
            let chain = self.read_chain(hansa)?;
            let head = replay(&chain, None)?;
            let link = Link::signed(skipper, head.head_seq + 1, head.head_hash, body);
            self.append_link(hansa, &link)?;
            // Keep the log bounded: once it grows past the threshold,
            // collapse it to a signed checkpoint (we already hold the
            // lock, so call the core directly).
            if chain.len() + 1 > CHECKPOINT_AFTER_LINKS {
                self.checkpoint_locked(hansa, skipper)?;
            }
            Ok(())
        })
    }

    fn compact(&self, hansa: HansaId, skipper: &Skipper) -> Result<()> {
        self.with_lock(hansa, || self.checkpoint_locked(hansa, skipper))
    }
}
