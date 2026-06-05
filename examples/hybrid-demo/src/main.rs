//! `hybrid-demo` - what cross-machine federation looks like end-to-end.
//!
//! In-process simulation of two machines:
//!
//! - Two agents (A, B) under separate tempdirs, simulating two boxes.
//! - Each runs a `SagaServer` on its own port, exposing its sagas + a
//!   `members.snap` for its own hansa root.
//! - Each agent's `HybridRegistry` points at the other's HTTP endpoint.
//! - Before querying, the agent runs `pull_sagas_into` so its local
//!   saga cache holds the peer's latest digest.
//! - The membrane's peer fan-out uses a filesystem-based PeerOpener
//!   (both tenants are visible because we're in one process). In a real
//!   deployment this opener would dispatch on `TenantLocation::Resp3`
//!   and connect to the peer's skeg-server.
//!
//! Run with:
//!
//!     cargo run -p hybrid-demo

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use hansa::HybridRegistry;
use hansa::prelude::*;
use skeg_rigging::{OpenError, RecordId, TenantId};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_net_http::SagaServer;
use skeg_rigging_skeg::Tenant;

const DIM: u32 = 4;

fn unit(axis: usize, jitter: f32) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM as usize];
    v[axis] = 1.0;
    for x in &mut v {
        *x += jitter;
    }
    v
}

fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

struct Box1 {
    #[allow(dead_code)] // surfaced via println! in main; useful for debug
    label: char,
    root: tempfile::TempDir,
    #[allow(dead_code)]
    tenant_id: TenantId,
    tenant_dir: PathBuf,
    saga_port: u16,
    saga_stop: Arc<AtomicBool>,
    saga_thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Box1 {
    fn drop(&mut self) {
        self.saga_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.saga_thread.take() {
            let _ = h.join();
        }
    }
}

fn populate_and_serve(
    label: char,
    label_byte: u8,
    axis: usize,
    key: &HansaKey,
) -> Box1 {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant_id = TenantId::from_bytes([label_byte; 16]);
    let tenant_dir = root.path().join("tenant");
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, DIM).unwrap());
    // 12 records: 8 on-axis (shareable), 4 off-axis (private).
    for i in 0..12u64 {
        let on_axis = i < 8;
        let vec = if on_axis {
            unit(axis, (i % 3) as f32 * 0.01)
        } else {
            unit((axis + 1) % DIM as usize, 0.01)
        };
        tenant
            .insert(
                RecordId(label_byte as u64 * 1000 + i),
                vec,
                on_axis,
                vec!["topic".into()],
                format!("{label} r{i:02} {}", if on_axis { "shareable" } else { "private" })
                    .into_bytes(),
            )
            .unwrap();
    }
    tenant.flush().unwrap();

    let hid = key.hansa_id();
    let saga_dir = root.path().join(hid.as_hex()).join("sagas");
    std::fs::create_dir_all(&saga_dir).unwrap();

    // Register the local member + emit the snapshot so the saga server's
    // /hansa/<id>/members endpoint returns something.
    let local_reg = FileRegistry::new(root.path());
    local_reg
        .join(
            hid,
            MemberRecord {
                tenant_id,
                tenant_location: TenantLocation::Path {
                    path: tenant_dir.clone(),
                },
                embedding_dim: DIM,
                joined_at: 1,
            },
        )
        .unwrap();
    local_reg.compact(hid).unwrap();

    // Write the saga to disk (would otherwise happen via Hansa::join).
    let handle = Hansa::open(HansaConfig {
        key: key.clone(),
        registry: Arc::new(FileRegistry::new(root.path())),
        local_tenant: tenant.clone(),
        local_tenant_id: tenant_id,
        local_tenant_location: TenantLocation::Path {
            path: tenant_dir.clone(),
        },
        saga_dir: saga_dir.clone(),
        peer_opener: None,
        default_budget: TokenBudget::default(),
            head_cache_dir: None,
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .unwrap();
    handle
        .refresh_saga(vec!["topic".into(); 12], 1, 7)
        .unwrap();
    drop(handle);

    // Spawn the SagaServer.
    let server = SagaServer::bind("127.0.0.1:0", saga_dir)
        .expect("bind saga server")
        .with_members_root(root.path().to_path_buf());
    let port = server.local_addr().port();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = stop.clone();
    let server_thread = thread::spawn(move || server.serve_until(stop_c));
    thread::sleep(Duration::from_millis(50));

    Box1 {
        label,
        root,
        tenant_id,
        tenant_dir,
        saga_port: port,
        saga_stop: stop,
        saga_thread: Some(server_thread),
    }
}

fn rule(title: &str) {
    let bar = "─".repeat(78);
    println!("\n{bar}\n  \x1b[1m{title}\x1b[0m\n{bar}");
}

fn main() {
    rule("hybrid-demo - cross-box federation in one process");
    let key = HansaKey::from_bytes([42; 32]);
    let hid = key.hansa_id();
    println!("  hansa id: {hid}\n");

    // Box A: axis 0. Box B: axis 1.
    let a = populate_and_serve('A', 1, 0, &key);
    let b = populate_and_serve('B', 2, 1, &key);
    println!(
        "  box A: saga server on 127.0.0.1:{}  tenant_dir={}",
        a.saga_port,
        a.tenant_dir.display()
    );
    println!(
        "  box B: saga server on 127.0.0.1:{}  tenant_dir={}",
        b.saga_port,
        b.tenant_dir.display()
    );

    // Box A's hansa: HybridRegistry with B as remote. Saga cache lives
    // under A's hansa root so the membrane finds it.
    let reg_a = HybridRegistry::new(FileRegistry::new(a.root.path()));
    reg_a.add_remote(format!("http://127.0.0.1:{}", b.saga_port));

    let saga_dir_a = a.root.path().join(hid.as_hex()).join("sagas");
    println!("\n  pulling B's saga into A's cache...");
    let pulled = reg_a.pull_sagas_into(&saga_dir_a).expect("pull");
    println!("  fetched {pulled} saga(s) from peers");

    let tenant_a = Arc::new(
        skeg_rigging_skeg::Tenant::open(&a.tenant_dir, a.tenant_id, DIM).unwrap(),
    );
    let hansa_a = Hansa::open(HansaConfig {
        key: key.clone(),
        registry: Arc::new(reg_a),
        local_tenant: tenant_a,
        local_tenant_id: a.tenant_id,
        local_tenant_location: TenantLocation::Path {
            path: a.tenant_dir.clone(),
        },
        saga_dir: saga_dir_a,
        peer_opener: Some(path_only_opener()),
        default_budget: TokenBudget::split(10, 15),
            head_cache_dir: None,
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .expect("open A");

    rule("query: A asks near axis-1 (B's territory)");
    let q = unit(1, 0.0);
    let hits = hansa_a
        .query(&q)
        .expect("query")
        .top_k(10)
        .execute()
        .expect("execute");

    println!("  got {} hits", hits.len());
    let local = hits.iter().filter(|h| matches!(h.origin, HitOrigin::Local)).count();
    let remote = hits.iter().filter(|h| matches!(h.origin, HitOrigin::Remote { .. })).count();
    println!("  {local} local + {remote} remote\n");
    for (rank, h) in hits.iter().take(10).enumerate() {
        let origin = match h.origin {
            HitOrigin::Local => "  LOCAL".to_string(),
            HitOrigin::Remote { tenant_id } => format!("peer-{}", tenant_id.0[0]),
        };
        let payload = std::str::from_utf8(&h.payload).unwrap_or("(non-utf8)");
        println!(
            "    {:>2}. [{origin}] sim={:.3}  \"{}\"",
            rank + 1,
            h.similarity,
            payload
        );
    }

    let bundle = ContextBuilder::from_hits(hits)
        .min_similarity(0.1)
        .token_budget(256)
        .dedup(true)
        .build();
    rule("bundle ready for LLM");
    println!(
        "  {} items, {} tokens",
        bundle.items.len(),
        bundle.total_tokens
    );
    println!("\n{}", bundle.render_compact());

    rule("teardown");
    drop(hansa_a);
    drop(a);
    drop(b);
    println!("  both saga servers stopped, tempdirs cleaned");
}
