//! `hansa-agent` - a minimal one-shot agent binary used by hansa's
//! cross-process integration test.
//!
//! Configuration goes through environment variables so the test
//! harness can spawn many copies without parsing CLI flags:
//!
//! | env var          | meaning                                          |
//! |------------------|--------------------------------------------------|
//! | `HANSA_ROOT`     | directory used as hansa root and tenant parent   |
//! | `HANSA_KEY_HEX`  | 64-char hex of the shared HansaKey               |
//! | `HANSA_LABEL`    | single byte 1..=N marking this agent             |
//! | `HANSA_DIM`      | embedding dim (default 4)                        |
//! | `HANSA_AXIS`     | which axis this agent's records live on (0..dim) |
//! | `HANSA_ACTION`   | `populate` or `query`                            |
//! | `HANSA_QUERY_AXIS` | axis the querier targets (used by `query`)     |
//!
//! `populate` inserts deterministic records and joins the hansa.
//! `query` opens the local hansa handle, runs a membrane query, and
//! emits the hits as JSON on stdout (one document per invocation).

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use serde::Serialize;
use skeg_rigging::{OpenError, RecordId, TenantId};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

const DEFAULT_DIM: u32 = 4;

fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}
const RECORDS_PER_AGENT: u64 = 20;
const SHAREABLE_CUTOFF: u64 = 10;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing env: {name}"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_hex32(s: &str) -> [u8; 32] {
    assert_eq!(s.len(), 64, "HANSA_KEY_HEX must be 64 chars");
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex digit");
    }
    out
}

fn unit_on(axis: usize, dim: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; dim];
    v[axis] = 1.0;
    v
}

fn near_axis(axis: usize, dim: usize, jitter: f32) -> Vec<f32> {
    let mut v = unit_on(axis, dim);
    for x in &mut v {
        *x += jitter;
    }
    v
}

fn open_tenant(root: &std::path::Path, label: u8, dim: u32) -> (Arc<Tenant>, TenantId, PathBuf) {
    let tenant_id = TenantId::from_bytes([label; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, dim).unwrap());
    (tenant, tenant_id, tenant_dir)
}

fn build_hansa_handle(
    root: &std::path::Path,
    key: HansaKey,
    tenant: Arc<Tenant>,
    tenant_id: TenantId,
    tenant_dir: PathBuf,
) -> Hansa<Tenant> {
    let hid = key.hansa_id();
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    Hansa::open(HansaConfig {
        key,
        registry: Arc::new(FileRegistry::new(root)),
        local_tenant: tenant,
        local_tenant_id: tenant_id,
        local_tenant_location: TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(path_only_opener()),
        default_budget: TokenBudget::split(20, 30),
    })
    .unwrap()
}

fn populate(
    root: &std::path::Path,
    key: HansaKey,
    label: u8,
    axis: usize,
    dim: u32,
) -> Result<()> {
    let (tenant, tenant_id, tenant_dir) = open_tenant(root, label, dim);

    let mut tags: Vec<String> = Vec::new();
    for i in 0..RECORDS_PER_AGENT {
        let shareable = i < SHAREABLE_CUTOFF;
        let vec = if i < SHAREABLE_CUTOFF + 5 {
            // On-axis: shareable + 5 more on-axis that are non-shareable
            near_axis(axis, dim as usize, ((i % 5) as f32) * 0.01)
        } else {
            // Off-axis filler, also non-shareable
            let off = (axis + 1) % (dim as usize);
            near_axis(off, dim as usize, 0.02)
        };
        let record_id = label as u64 * 1000 + i;
        let topic = format!("topic-{axis}");
        tenant
            .insert(
                RecordId(record_id),
                vec,
                shareable,
                vec![topic.clone()],
                format!("agent-{label} record-{i}").into_bytes(),
            )
            .map_err(|e| HansaError::Invariant(format!("insert: {e}")))?;
        tags.push(topic);
    }
    tenant
        .flush()
        .map_err(|e| HansaError::Invariant(format!("flush: {e}")))?;

    let handle = build_hansa_handle(root, key, tenant, tenant_id, tenant_dir);
    handle.join(tags.clone())?;
    handle.refresh_saga(tags, 1, 7)?;
    Ok(())
}

#[derive(Serialize)]
struct HitOut {
    record_id: u64,
    similarity: f32,
    origin: String,
    tenant_byte: Option<u8>,
    payload: String,
}

#[derive(Serialize)]
struct QueryReport {
    label: u8,
    hansa_id: String,
    query_axis: usize,
    member_count: usize,
    hits: Vec<HitOut>,
}

fn query(
    root: &std::path::Path,
    key: HansaKey,
    label: u8,
    axis: usize,
    query_axis: usize,
    dim: u32,
) -> Result<()> {
    let (tenant, tenant_id, tenant_dir) = open_tenant(root, label, dim);
    // Re-insert records so the local FlatIndex is populated in this
    // process. The sidecar already exists from a prior populate run;
    // re-opening re-reads it, so we just need to construct the handle.
    let _ = (tenant.clone(), &tenant_dir, axis);
    let handle = build_hansa_handle(root, key, tenant, tenant_id, tenant_dir.clone());

    let q = near_axis(query_axis, dim as usize, 0.0);
    let hits = handle
        .query(&q)?
        .top_k(10)
        .budget(TokenBudget::split(12, 12))
        .execute()?;

    let members = handle.members()?;
    let report = QueryReport {
        label,
        hansa_id: handle.id().as_hex(),
        query_axis,
        member_count: members.len(),
        hits: hits
            .into_iter()
            .map(|h| {
                let (origin, byte) = match h.origin {
                    HitOrigin::Local => ("local".to_string(), None),
                    HitOrigin::Remote { tenant_id } => ("remote".to_string(), Some(tenant_id.0[0])),
                };
                HitOut {
                    record_id: h.record_id.0,
                    similarity: h.similarity,
                    origin,
                    tenant_byte: byte,
                    payload: String::from_utf8_lossy(&h.payload).into_owned(),
                }
            })
            .collect(),
    };
    println!("{}", serde_json::to_string(&report).unwrap());
    Ok(())
}

fn main() {
    let root = PathBuf::from(env("HANSA_ROOT"));
    let key_hex = env("HANSA_KEY_HEX");
    let label: u8 = env("HANSA_LABEL").parse().expect("HANSA_LABEL must be u8");
    let axis: usize = env("HANSA_AXIS").parse().expect("HANSA_AXIS must be usize");
    let dim: u32 = env_or("HANSA_DIM", &DEFAULT_DIM.to_string())
        .parse()
        .expect("HANSA_DIM");
    let action = env("HANSA_ACTION");
    let key = HansaKey::from_bytes(parse_hex32(&key_hex));

    let result = match action.as_str() {
        "populate" => populate(&root, key, label, axis, dim),
        "query" => {
            let qaxis: usize = env("HANSA_QUERY_AXIS").parse().expect("HANSA_QUERY_AXIS");
            query(&root, key, label, axis, qaxis, dim)
        }
        other => {
            eprintln!("unknown HANSA_ACTION: {other}");
            std::process::exit(2);
        }
    };

    if let Err(e) = result {
        eprintln!("hansa-agent failed: {e}");
        std::process::exit(1);
    }
}
