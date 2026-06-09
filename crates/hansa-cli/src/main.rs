//! `hansa`: a command-line front end for the hansa federation library,
//! so a trust group of agent memories can be created, fed, and queried
//! without writing any Rust.
//!
//! The secret that binds a hansa together is a **passphrase**. Everyone
//! who runs `hansa init` with the same hansa name and passphrase joins
//! the same trust group. One machine can run several agents at once:
//! `hansa agents` lists them, `hansa use` switches the active one.
//! Memories are private by default; `--share` federates a record.

mod config;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use hansa::prelude::*;
use skeg_rigging::{IterVectors, OpenError, RecordId, TenantId};
use skeg_rigging_ingest::{Embed, IngestOptions, OllamaEmbed, StubEmbed, ingest_tree, watch_tree};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

use config::{Config, Member, decode_hex, encode_hex};

#[derive(Parser)]
#[command(name = "hansa", version, about = "Federate agent memories. No code required.")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create or join a hansa, adding an agent on this machine.
    Init(InitArgs),
    /// List the agents this machine runs (active one marked).
    Agents,
    /// Switch the active agent.
    Use(UseArgs),
    /// Store a memory (private by default; use --share to federate it).
    Remember(RememberArgs),
    /// Ingest a file or directory: embed every chunk and store it.
    Ingest(IngestArgs),
    /// Watch a directory and ingest files live as they change.
    ///
    /// v1 limitations: a re-edited file APPENDS fresh records rather than
    /// replacing its old chunks (record ids are not yet stable per
    /// chunk), so editing the same file repeatedly grows the vault. The
    /// saga digest is NOT refreshed while watching (that would open a
    /// second writer on the same vault); run `hansa saga` afterwards, or
    /// the next `remember`/`ingest` refreshes it, so peers see the new
    /// records.
    Watch(IngestArgs),
    /// Ask the hansa a question; answers fan out to peers.
    Query(QueryArgs),
    /// List the members of the active agent's hansa.
    Members,
    /// Show status for the active agent.
    Status,
    /// Delete one local memory by its record id.
    Forget {
        /// Record id (shown by `query`/`status`).
        id: u64,
    },
    /// Evict a member from the hansa (skipper only).
    Revoke {
        /// Member tenant id, hex-encoded.
        tenant: String,
    },
    /// Remove a local agent (leaves the roster; keeps vault files).
    Leave(LeaveArgs),
    /// Rebuild the active agent's saga digest from its memories.
    Saga,
    /// Show the active hansa's id and how peers join it.
    Key,
}

#[derive(clap::Args)]
struct InitArgs {
    /// Name of the hansa (shared by everyone in the trust group).
    #[arg(long)]
    name: String,
    /// Friendly name for this agent. Default: "me".
    #[arg(long, default_value = "me")]
    tenant: String,
    /// Shared passphrase. If omitted, you are prompted for it.
    #[arg(long)]
    passphrase: Option<String>,
    /// Embedding endpoint (Ollama-compatible /api/embed).
    #[arg(long, default_value = "http://localhost:11434")]
    embed_url: String,
    /// Embedding model.
    #[arg(long, default_value = "mxbai-embed-large")]
    embed_model: String,
}

#[derive(clap::Args)]
struct UseArgs {
    /// Agent (tenant) name to make active.
    tenant: String,
    /// Disambiguate when the same tenant name exists in two hansas.
    #[arg(long)]
    name: Option<String>,
}

#[derive(clap::Args)]
struct LeaveArgs {
    /// Agent to remove. Defaults to the active agent.
    #[arg(long)]
    tenant: Option<String>,
    /// Hansa name, to disambiguate.
    #[arg(long)]
    name: Option<String>,
}

#[derive(clap::Args)]
struct RememberArgs {
    /// The memory text.
    text: String,
    /// Make this memory visible to peers (otherwise it stays private).
    #[arg(long)]
    share: bool,
    /// Attach a tag (repeatable).
    #[arg(long = "tag")]
    tags: Vec<String>,
}

#[derive(clap::Args)]
struct IngestArgs {
    /// File or directory to ingest.
    path: PathBuf,
    /// Keep ingested records private (default: shared with peers).
    #[arg(long)]
    private: bool,
    /// Attach a tag to every record (repeatable).
    #[arg(long = "tag")]
    tags: Vec<String>,
    /// Only ingest files with these extensions, e.g. --ext md --ext rs.
    #[arg(long = "ext")]
    exts: Vec<String>,
}

#[derive(clap::Args)]
struct QueryArgs {
    /// The question / search text.
    text: String,
    /// How many results to return.
    #[arg(short, long, default_value_t = 5)]
    k: u32,
    /// Token budget for the whole fan-out.
    #[arg(long, default_value_t = 24)]
    budget: u32,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Init(a) => cmd_init(a),
        Cmd::Agents => cmd_agents(),
        Cmd::Use(a) => cmd_use(a),
        Cmd::Remember(a) => cmd_remember(a),
        Cmd::Ingest(a) => cmd_ingest(a, false),
        Cmd::Watch(a) => cmd_ingest(a, true),
        Cmd::Query(a) => cmd_query(a),
        Cmd::Members => cmd_members(),
        Cmd::Status => cmd_status(),
        Cmd::Forget { id } => cmd_forget(id),
        Cmd::Revoke { tenant } => cmd_revoke(&tenant),
        Cmd::Leave(a) => cmd_leave(a),
        Cmd::Saga => cmd_saga(),
        Cmd::Key => cmd_key(),
    }
}

// ---------------------------------------------------------------------------
// Handle wiring
// ---------------------------------------------------------------------------

fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Re-derive the trust identity for one agent from its stored key.
fn identity(m: &Member) -> Result<(HansaKey, Skipper, HansaId, TenantId)> {
    let keystore = FileKeystore::new(Config::root().join("keys"));
    let key = keystore
        .load(&m.hansa)
        .with_context(|| format!("load key for hansa '{}'", m.hansa))?;
    let skipper = Skipper::from_hansa_key(&key);
    let hid = HansaId::from_skipper(&skipper.public());
    let tenant_id = TenantId::from_bytes(m.tenant_id_bytes()?);
    Ok((key, skipper, hid, tenant_id))
}

/// Filesystem dir of an agent's vault.
fn tenant_dir(m: &Member, hid: &HansaId) -> PathBuf {
    Config::root().join(hid.as_hex()).join(format!("tenant-{}", m.tenant))
}

/// Stub-embedder dimension (must match what `init` records for a stub).
const STUB_DIM: u32 = 64;

/// Build an embedder from a URL + model. `--embed-url stub` selects a
/// deterministic in-process embedder (no model server), used by tests
/// and offline runs; everything else connects to Ollama.
fn build_embedder(url: &str, model: &str) -> Result<Box<dyn Embed>> {
    if url == "stub" {
        Ok(Box::new(StubEmbed::new(STUB_DIM)))
    } else {
        Ok(Box::new(
            OllamaEmbed::connect(url, model).context("connect embedder")?,
        ))
    }
}

/// Build the embedder this agent is configured for.
fn embedder_for(m: &Member) -> Result<Box<dyn Embed>> {
    build_embedder(&m.embed_url, &m.embed_model)
}

/// Re-derive the key and open the hansa handle for one agent.
fn open_handle(m: &Member) -> Result<(Hansa<Tenant>, Arc<Tenant>)> {
    let (key, skipper, hid, tenant_id) = identity(m)?;
    let tenant_dir = tenant_dir(m, &hid);
    let tenant = Arc::new(
        Tenant::open(&tenant_dir, tenant_id, m.dim)
            .with_context(|| format!("open vault {}", tenant_dir.display()))?,
    );
    let root = Config::root();
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    let registry = Arc::new(FileRegistry::new(&root));
    let hansa = Hansa::open(HansaConfig {
        key,
        skipper: Some(skipper),
        hansa_id: Some(hid),
        registry,
        local_tenant: tenant.clone(),
        local_tenant_id: tenant_id,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(path_only_opener()),
        default_budget: TokenBudget::split(8, 12),
        head_cache_dir: None,
    })?;
    Ok((hansa, tenant))
}

fn refresh(hansa: &Hansa<Tenant>, tags: Vec<String>) -> Result<()> {
    hansa.refresh_saga(tags, now_unix(), rand::random::<u64>())?;
    Ok(())
}

/// Resolve the active member, or fail with a hint.
fn active() -> Result<Member> {
    Config::load()?.active_member()
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn read_passphrase(arg: Option<String>) -> Result<String> {
    if let Some(p) = arg {
        return Ok(p);
    }
    eprint!("passphrase for this hansa: ");
    use std::io::Write as _;
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).context("read passphrase")?;
    let p = line.trim().to_string();
    if p.is_empty() {
        anyhow::bail!("passphrase cannot be empty");
    }
    Ok(p)
}

fn cmd_init(a: InitArgs) -> Result<()> {
    let passphrase = read_passphrase(a.passphrase)?;
    let root = Config::root();
    std::fs::create_dir_all(&root)?;

    // Key derives deterministically from (name, passphrase): same inputs,
    // same trust group; that's how another agent joins.
    let key = HansaKey::from_passphrase(&passphrase, a.name.as_bytes());
    let keystore = FileKeystore::new(root.join("keys"));
    keystore.store(&a.name, &key).context("store key")?;

    let skipper = Skipper::from_hansa_key(&key);
    let hid = HansaId::from_skipper(&skipper.public());

    let embedder = build_embedder(&a.embed_url, &a.embed_model).context("probe embedder")?;
    let dim = embedder.dim();

    let mut cfg = Config::load()?;
    // Reuse the existing identity/counter if this agent already exists.
    let member = match cfg.find(&a.name, &a.tenant) {
        Some(i) => {
            let mut m = cfg.members[i].clone();
            m.embed_url = a.embed_url.clone();
            m.embed_model = a.embed_model.clone();
            m.dim = dim;
            m
        }
        None => {
            let mut tid = [0u8; 16];
            for b in &mut tid {
                *b = rand::random::<u8>();
            }
            Member {
                hansa: a.name.clone(),
                tenant: a.tenant.clone(),
                tenant_id_hex: encode_hex(&tid),
                embed_url: a.embed_url.clone(),
                embed_model: a.embed_model.clone(),
                dim,
                next_record_id: 0,
            }
        }
    };
    cfg.upsert_active(member.clone());
    cfg.save()?;

    let (hansa, _tenant) = open_handle(&member)?;
    hansa.join(Vec::<String>::new()).context("join hansa")?;
    refresh(&hansa, Vec::new())?;

    println!("⚓ agent '{}' active in hansa '{}'", a.tenant, a.name);
    println!("   id      {}", hid.as_hex());
    println!("   member  {} ({})", a.tenant, member.tenant_id_hex);
    println!("   embed   {} (dim {})", a.embed_model, dim);
    println!();
    println!("Add another agent (here or on another machine):");
    println!("   hansa init --name {} --tenant <other> --passphrase '<same passphrase>'", a.name);
    Ok(())
}

fn cmd_agents() -> Result<()> {
    let cfg = Config::load()?;
    if cfg.members.is_empty() {
        println!("no agents yet, run `hansa init`");
        return Ok(());
    }
    for m in &cfg.members {
        let mark = if Config::key_of(&m.hansa, &m.tenant) == cfg.active { "●" } else { " " };
        println!("  {mark} {:<12} hansa {:<12} dim {}", m.tenant, m.hansa, m.dim);
    }
    Ok(())
}

fn cmd_use(a: UseArgs) -> Result<()> {
    let mut cfg = Config::load()?;
    let idx = match &a.name {
        Some(name) => cfg
            .find(name, &a.tenant)
            .with_context(|| format!("no agent '{}' in hansa '{name}'", a.tenant))?,
        None => {
            let hits = cfg.find_by_tenant(&a.tenant);
            match hits.as_slice() {
                [] => anyhow::bail!("no agent named '{}'", a.tenant),
                [i] => *i,
                _ => {
                    let names: Vec<_> =
                        hits.iter().map(|&i| cfg.members[i].hansa.clone()).collect();
                    anyhow::bail!(
                        "'{}' exists in several hansas ({}); add --name <hansa>",
                        a.tenant,
                        names.join(", ")
                    );
                }
            }
        }
    };
    let m = &cfg.members[idx];
    cfg.active = Config::key_of(&m.hansa, &m.tenant);
    let (hansa, tenant) = (m.hansa.clone(), m.tenant.clone());
    cfg.save()?;
    println!("● now using '{tenant}' in hansa '{hansa}'");
    Ok(())
}

fn cmd_remember(a: RememberArgs) -> Result<()> {
    let mut cfg = Config::load()?;
    let idx = cfg.active_index()?;
    let member = cfg.members[idx].clone();
    let (hansa, tenant) = open_handle(&member)?;
    let embedder = embedder_for(&member)?;
    let v = embedder.passage(&a.text)?;

    let rid = cfg.members[idx].alloc_record_id();
    tenant
        .insert(RecordId(rid), v, a.share, a.tags.clone(), a.text.as_bytes().to_vec())
        .context("insert record")?;
    tenant.flush().context("flush vault")?;
    cfg.save()?;
    refresh(&hansa, a.tags)?;

    let vis = if a.share { "shared" } else { "private" };
    println!("✓ remembered #{rid} ({vis})");
    Ok(())
}

fn cmd_ingest(a: IngestArgs, watch: bool) -> Result<()> {
    let mut cfg = Config::load()?;
    let idx = cfg.active_index()?;
    let member = cfg.members[idx].clone();

    // Open the vault writable and standalone. We deliberately do NOT go
    // through open_handle here (which shares the tenant with a Hansa
    // handle); ingest needs `&mut` on the writer, so it owns the tenant
    // for the run, then we reopen via open_handle to refresh the saga.
    let (_, _, hid, tenant_id) = identity(&member)?;
    let dir = tenant_dir(&member, &hid);
    let mut tenant = Tenant::open(&dir, tenant_id, member.dim)
        .with_context(|| format!("open vault {}", dir.display()))?;
    let embed = embedder_for(&member)?;

    let opts = IngestOptions {
        shareable: !a.private,
        tags: a.tags.clone(),
        exts: a.exts.clone(),
        start_id: member.next_record_id,
        ..IngestOptions::default()
    };

    if watch {
        let vis = if a.private { "private" } else { "shared" };
        println!("👁  watching {} ({vis}), ctrl-c to stop", a.path.display());
        let hansa_name = member.hansa.clone();
        let tenant_name = member.tenant.clone();
        watch_tree(&mut tenant, embed.as_ref(), &a.path, opts, |s| {
            println!("  + {} chunk(s) · {} words (next id {})", s.chunks, s.words, s.next_id);
            // Persist the id counter so a restart resumes cleanly. Saga
            // refresh is deferred (a live `open_handle` here would open a
            // second writer on the same vault); run `hansa saga`, or the
            // next `remember`/`ingest` refreshes it.
            if let Ok(mut c) = Config::load()
                && let Some(i) = c.find(&hansa_name, &tenant_name)
            {
                c.members[i].next_record_id = s.next_id;
                let _ = c.save();
            }
        })?;
        return Ok(());
    }

    let stats = ingest_tree(&mut tenant, embed.as_ref(), &a.path, &opts, &mut |s| {
        use std::io::Write as _;
        print!("\r  {} file(s) · {} chunk(s) · {} words", s.files, s.chunks, s.words);
        std::io::stdout().flush().ok();
    })?;
    println!();
    drop(tenant);

    cfg.members[idx].next_record_id = stats.next_id;
    cfg.save()?;

    // Refresh the saga so peers' fan-out sees the new knowledge.
    let (hansa, _t) = open_handle(&member)?;
    refresh(&hansa, Vec::new())?;

    let vis = if a.private { "private" } else { "shared" };
    println!(
        "✓ ingested {} file(s) · {} chunk(s) · {} words ({vis})",
        stats.files, stats.chunks, stats.words
    );
    Ok(())
}

fn cmd_query(a: QueryArgs) -> Result<()> {
    let member = active()?;
    let (hansa, _tenant) = open_handle(&member)?;
    let embedder = embedder_for(&member)?;
    let v = embedder.query(&a.text)?;

    let (hits, stats) = hansa
        .query(&v)?
        .top_k(a.k)
        .budget(TokenBudget::flat(a.budget))
        .execute_with_stats()?;

    if hits.is_empty() {
        println!("no matches.");
    }
    for h in &hits {
        let who = match h.origin {
            HitOrigin::Local => "you".to_string(),
            HitOrigin::Remote { tenant_id } => {
                let hex = encode_hex(tenant_id.as_bytes());
                format!("peer {}", &hex[..hex.len().min(8)])
            }
        };
        let text = String::from_utf8_lossy(&h.payload);
        println!("  {:.3}  [{}]  #{}  {}", h.similarity, who, h.record_id.0, text);
    }
    println!(
        "\n  {} hit(s) · {} peer(s) asked, {} answered",
        hits.len(),
        stats.peers_attempted,
        stats.peers_completed
    );
    Ok(())
}

fn cmd_members() -> Result<()> {
    let member = active()?;
    let (hansa, _tenant) = open_handle(&member)?;
    let members = hansa.members()?;
    if members.is_empty() {
        println!("no members yet.");
    }
    for m in &members {
        println!(
            "  {}  dim {}  joined {}",
            encode_hex(m.tenant_id.as_bytes()),
            m.embedding_dim,
            m.joined_at
        );
    }
    println!("\n  {} member(s)", members.len());
    Ok(())
}

fn cmd_status() -> Result<()> {
    let member = active()?;
    let (hansa, tenant) = open_handle(&member)?;
    let count = tenant.iter_vectors().count();
    let peers = hansa.members().map(|m| m.len()).unwrap_or(0);
    println!("hansa    {}", member.hansa);
    println!("id       {}", hansa.id().as_hex());
    println!("agent    {} ({})", member.tenant, member.tenant_id_hex);
    println!("memories {count}");
    println!("peers    {peers}");
    println!("embed    {} @ {} (dim {})", member.embed_model, member.embed_url, member.dim);
    println!("vault    {}", vault_dir(&member, &hansa).display());
    Ok(())
}

fn cmd_forget(id: u64) -> Result<()> {
    let member = active()?;
    let (hansa, tenant) = open_handle(&member)?;
    let removed = tenant.delete(RecordId(id)).context("delete record")?;
    tenant.flush().ok();
    refresh(&hansa, Vec::new())?;
    if removed {
        println!("✓ forgot #{id}");
    } else {
        println!("no memory #{id}");
    }
    Ok(())
}

fn cmd_revoke(tenant_hex: &str) -> Result<()> {
    let member = active()?;
    let (hansa, _tenant) = open_handle(&member)?;
    let raw = decode_hex(tenant_hex)?;
    let arr: [u8; 16] = raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("tenant id must be 16 bytes (32 hex chars)"))?;
    hansa.revoke(TenantId::from_bytes(arr))?;
    println!("✓ revoked {tenant_hex}");
    Ok(())
}

fn cmd_leave(a: LeaveArgs) -> Result<()> {
    let mut cfg = Config::load()?;
    // Pick the target agent: explicit, or the active one.
    let idx = match (&a.name, &a.tenant) {
        (Some(n), Some(t)) => cfg.find(n, t).context("no such agent")?,
        (None, Some(t)) => {
            let hits = cfg.find_by_tenant(t);
            match hits.as_slice() {
                [i] => *i,
                [] => anyhow::bail!("no agent named '{t}'"),
                _ => anyhow::bail!("'{t}' exists in several hansas; add --name <hansa>"),
            }
        }
        _ => cfg.active_index()?,
    };
    let member = cfg.members[idx].clone();
    let (hansa, _tenant) = open_handle(&member)?;
    hansa.leave().context("leave roster")?;
    cfg.remove(&member.hansa, &member.tenant);
    cfg.save()?;
    println!(
        "✓ agent '{}' left hansa '{}' (vault files kept on disk)",
        member.tenant, member.hansa
    );
    Ok(())
}

fn cmd_saga() -> Result<()> {
    let member = active()?;
    let (hansa, _tenant) = open_handle(&member)?;
    refresh(&hansa, Vec::new())?;
    println!("✓ saga refreshed");
    Ok(())
}

fn cmd_key() -> Result<()> {
    let member = active()?;
    let (hansa, _tenant) = open_handle(&member)?;
    println!("hansa '{}'", member.hansa);
    println!("id    {}", hansa.id().as_hex());
    println!();
    println!("Peers join with the same name and passphrase:");
    println!("   hansa init --name {} --tenant <name> --passphrase '<passphrase>'", member.hansa);
    Ok(())
}

fn vault_dir(member: &Member, hansa: &Hansa<Tenant>) -> PathBuf {
    Config::root()
        .join(hansa.id().as_hex())
        .join(format!("tenant-{}", member.tenant))
}
