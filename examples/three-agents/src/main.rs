//! `three-agents` - a walkthrough of how a hansa works.
//!
//! Three agents are spun up in-process:
//!
//! - **A - work**: meeting notes and design decisions. Some are
//!   shareable with peers, some (1:1s, personal reminders) are not.
//! - **B - research**: papers and blog posts. Almost all shareable.
//! - **C - code**: idiomatic Rust patterns and snippets. The one
//!   Python snippet is kept private because the team is Rust-only and
//!   exposing it would be noise to peers.
//!
//! The program then runs three queries that show *what* gets shared
//! and *how* the membrane decides which peer is worth asking.

mod knowledge;

use std::path::PathBuf;
use std::sync::Arc;

use hansa::prelude::*;
use hansa::saga::score_saga;
use skeg_rigging::{OpenError, RecordId, TenantId};
use skeg_rigging_net::TenantLocation;
use skeg_rigging_skeg::Tenant;

use knowledge::{CONCEPT_NAMES, DIM, Note, render_concepts};

fn path_only_opener() -> PeerOpener {
    Arc::new(|_tid, loc: &TenantLocation| match loc {
        TenantLocation::Path { path } => skeg_rigging_skeg::open_readonly(path),
        _ => Err(OpenError::NotFound),
    })
}

// =========================================================================
// Agent setup
// =========================================================================

struct Agent {
    label: char,
    role: &'static str,
    hansa: Hansa<Tenant>,
    tags: Vec<String>,
}

fn spawn_agent(
    root: &std::path::Path,
    label: char,
    byte: u8,
    role: &'static str,
    notes: Vec<Note>,
) -> Agent {
    let tenant_id = TenantId::from_bytes([byte; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, DIM).unwrap());

    let mut all_tags = Vec::new();
    for note in &notes {
        tenant
            .insert(
                RecordId(note.id),
                note.concept.to_vec(),
                note.shareable,
                note.tags.iter().map(|s| s.to_string()).collect(),
                note.text.as_bytes().to_vec(),
            )
            .unwrap();
        for t in note.tags {
            all_tags.push(t.to_string());
        }
    }
    tenant.flush().unwrap();

    let key = HansaKey::from_bytes([42; 32]);
    let skipper = Skipper::from_hansa_key(&key);
    let hid = HansaId::from_skipper(&skipper.public());
    let registry = Arc::new(FileRegistry::new(root));
    let saga_dir = root.join(hid.as_hex()).join("sagas");
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
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .unwrap();

    Agent {
        label,
        role,
        hansa,
        tags: all_tags,
    }
}

// =========================================================================
// Pretty printing
// =========================================================================

fn rule(title: &str) {
    println!("\n{}", "═".repeat(78));
    println!(" {title}");
    println!("{}", "═".repeat(78));
}

fn subrule(title: &str) {
    println!("\n── {title} ──");
}

fn print_agent_inventory(agent: &Agent, notes: &[Note]) {
    println!(
        "\nAgent {} ({}): {} records ({} shareable, {} private)",
        agent.label,
        agent.role,
        notes.len(),
        notes.iter().filter(|n| n.shareable).count(),
        notes.iter().filter(|n| !n.shareable).count(),
    );
    for n in notes {
        let flag = if n.shareable { "share" } else { " priv" };
        println!(
            "  [{flag}] {:<2}  {}",
            n.id,
            short(n.text, 64),
        );
    }
}

fn short(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

fn print_saga(agent: &Agent) {
    let path = agent.hansa.local_saga_path();
    let saga = Saga::read_from_path(&path).expect("read saga");
    println!(
        "Agent {} saga: {} centroids, top tags = {}",
        agent.label,
        saga.centroids.len(),
        saga.tags
            .iter()
            .take(5)
            .map(|t| format!("{}:{}", t.tag, t.count))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

// =========================================================================
// Demo queries
// =========================================================================

struct Query {
    label: &'static str,
    text: &'static str,
    intent: &'static str,
    /// Hand-crafted "embedding" of the query in concept-axis space.
    vector: [f32; 8],
}

fn demo_queries() -> Vec<Query> {
    let mix = |pairs: &[(usize, f32)]| {
        let mut x = [0.0f32; 8];
        for (a, w) in pairs {
            x[*a] = *w;
        }
        x
    };
    vec![
        Query {
            label: "Q1",
            text: "how did we set up BLAKE3 key derivation?",
            intent: "Cross-cutting question: a decision (work), a paper (research), and the code (code) should all surface.",
            vector: mix(&[(2, 1.0), (4, 0.3)]),
        },
        Query {
            label: "Q2",
            text: "what's the architecture of the federation layer?",
            intent: "Pulls a work design note, a research analogy (Hanseatic League), and a Rust pattern from code.",
            vector: mix(&[(5, 1.0), (6, 0.6)]),
        },
        Query {
            label: "Q3",
            text: "give me an idiomatic Rust pattern for parallel work",
            intent: "Code-flavoured; private Python snippet (axis 1) must not leak even though it is in the same tenant.",
            vector: mix(&[(0, 1.0), (7, 0.7)]),
        },
    ]
}

fn run_query(asker: &Agent, peers: &[&Agent], q: &Query) {
    rule(&format!("{}: {}", q.label, q.text));
    println!("intent: {}", q.intent);
    println!("query vector: {}", render_concepts(&q.vector));

    // Show what the asker's view of each peer's saga score looks like.
    // This is exactly what the membrane uses to decide where to fan out.
    subrule("step 1 - score peer sagas");
    let members = asker.hansa.members().expect("members");
    for m in &members {
        if m.tenant_id == TenantId::from_bytes([asker.label as u8 - b'A' + 1; 16]) {
            continue;
        }
        let saga = asker
            .hansa
            .load_peer_saga(m.tenant_id)
            .expect("load saga")
            .expect("peer saga present");
        let s = score_saga(&saga, &q.vector);
        let peer_label = peer_label_for(m.tenant_id, peers);
        println!(
            "  peer {peer_label}: saga score = {s:.3}  (top tags = {})",
            saga.tags
                .iter()
                .take(3)
                .map(|t| format!("{}:{}", t.tag, t.count))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Now run the query and render the hit set with the source text
    // attached, so the reader can see *what* came back from *whom*.
    subrule("step 2 - execute query and merge");
    let hits = asker
        .hansa
        .query(&q.vector)
        .expect("builder")
        .top_k(10)
        .budget(TokenBudget::split(6, 8))
        .execute()
        .expect("execute");

    let local = hits.iter().filter(|h| matches!(h.origin, HitOrigin::Local)).count();
    let remote = hits.iter().filter(|h| matches!(h.origin, HitOrigin::Remote { .. })).count();
    let shown: Vec<_> = hits.iter().filter(|h| h.similarity > 0.05).collect();
    let dropped = hits.len() - shown.len();
    println!(
        "{} hits returned (budget: 6 remote / 8 total) - {local} local + {remote} remote",
        hits.len()
    );
    if dropped > 0 {
        println!(
            "  ({dropped} hits with similarity < 0.05 hidden from this view)"
        );
    }

    for (rank, h) in shown.iter().enumerate() {
        let (origin_label, agent_label) = match h.origin {
            HitOrigin::Local => ("  LOCAL".to_string(), asker.label),
            HitOrigin::Remote { tenant_id } => {
                let l = peer_label_for(tenant_id, peers);
                (format!("from {l}"), l)
            }
        };
        let _ = agent_label;
        let payload = std::str::from_utf8(&h.payload).unwrap_or("(non-utf8)");
        println!(
            "  {:>2}. [{origin_label}] sim={:.3}  \"{}\"",
            rank + 1,
            h.similarity,
            short(payload, 72)
        );
    }

    subrule("step 3 - what was filtered out");
    explain_filter(asker, peers, q);
}

fn explain_filter(asker: &Agent, peers: &[&Agent], q: &Query) {
    // For each remote agent, list the non-shareable notes whose concept
    // vector would have matched - these are the ones the membrane
    // *deliberately* did not return.
    let asker_byte = asker.label as u8 - b'A' + 1;
    let asker_id = TenantId::from_bytes([asker_byte; 16]);
    for peer in peers {
        if peer.label == asker.label {
            continue;
        }
        let peer_byte = peer.label as u8 - b'A' + 1;
        let peer_id = TenantId::from_bytes([peer_byte; 16]);
        let _ = (asker_id, peer_id);

        let notes = match peer.label {
            'A' => knowledge::work_notes(),
            'B' => knowledge::research_notes(),
            'C' => knowledge::code_notes(),
            _ => continue,
        };
        let blocked: Vec<&Note> = notes
            .iter()
            .filter(|n| !n.shareable)
            .filter(|n| cosine(&n.concept, &q.vector) > 0.2)
            .collect();
        if blocked.is_empty() {
            continue;
        }
        println!(
            "  peer {}'s non-shareable notes that WOULD have matched but were blocked:",
            peer.label
        );
        for n in blocked {
            println!(
                "     [held back] \"{}\" (sim={:.3})",
                short(n.text, 60),
                cosine(&n.concept, &q.vector)
            );
        }
    }
}

fn peer_label_for(id: TenantId, peers: &[&Agent]) -> char {
    let byte = id.0[0];
    peers
        .iter()
        .find(|a| (a.label as u8 - b'A' + 1) == byte)
        .map(|a| a.label)
        .unwrap_or('?')
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

// =========================================================================
// Main
// =========================================================================

fn main() {
    rule("hansa M1 walkthrough - three agents share what they choose to");
    println!(
        "Concept axes:\n  {}",
        CONCEPT_NAMES
            .iter()
            .enumerate()
            .map(|(i, n)| format!("{i}:{n}"))
            .collect::<Vec<_>>()
            .join("  ")
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    println!("\nhansa root: {}", root.display());

    let work_notes_data = knowledge::work_notes();
    let research_notes_data = knowledge::research_notes();
    let code_notes_data = knowledge::code_notes();

    let a = spawn_agent(root, 'A', 1, "work", work_notes_data.clone());
    let b = spawn_agent(root, 'B', 2, "research", research_notes_data.clone());
    let c = spawn_agent(root, 'C', 3, "code", code_notes_data.clone());

    rule("setup - what each agent stores");
    print_agent_inventory(&a, &work_notes_data);
    print_agent_inventory(&b, &research_notes_data);
    print_agent_inventory(&c, &code_notes_data);

    for agent in [&a, &b, &c] {
        agent.hansa.join(agent.tags.clone()).expect("join");
        agent
            .hansa
            .refresh_saga(agent.tags.clone(), 1, 7)
            .expect("refresh_saga");
    }

    rule("setup - what each agent published to the registry");
    println!("\nHansaId: {}\n", a.hansa.id());
    println!("All three agents derived the same HansaId from the shared HansaKey.");
    println!("Each one wrote its saga to ~/.hansa/<HansaId>/sagas/<tenant>.saga\n");
    for agent in [&a, &b, &c] {
        print_saga(agent);
    }

    // Re-open A so it observes the registry state after B and C joined.
    let a = spawn_agent(root, 'A', 1, "work", work_notes_data);

    let queries = demo_queries();
    let peers = [&a, &b, &c];
    for q in &queries {
        run_query(&a, &peers, q);
    }

    // ---------------------------------------------------------------
    // Bonus: assemble a context bundle ready for an LLM prompt.
    // ---------------------------------------------------------------
    rule("context assembly - from hits to LLM prompt");
    let q = &queries[0]; // BLAKE3 query
    println!("query: \"{}\"\n", q.text);
    let hits = a
        .hansa
        .query(&q.vector)
        .expect("query builder")
        .top_k(10)
        .budget(TokenBudget::split(8, 12))
        .execute()
        .expect("execute");

    let bundle = ContextBuilder::from_hits(hits)
        .min_similarity(0.3)
        .token_budget(120)
        .dedup(true)
        .build();

    println!(
        "kept {} items, ~{} tokens; dropped {} below threshold, {} duplicates, {} over budget",
        bundle.len(),
        bundle.total_tokens,
        bundle.dropped_below_threshold,
        bundle.dropped_duplicates,
        bundle.dropped_over_budget
    );
    subrule("rendered as markdown");
    println!("{}", bundle.render_markdown());

    rule("summary");
    println!(
        "\nWhat hansa is doing under the hood:\n\
         1. Each agent's saga is the cheap \"is this peer relevant?\" digest. The query\n   \
            scored every peer's saga first, *before* committing budget to fan-out.\n\
         2. Records explicitly marked `shareable: false` (1:1s, personal reminders,\n   \
            the Python snippet on a Rust-only team) were filtered at the source -\n   \
            they are not even visible to peers, regardless of similarity.\n\
         3. Hits carry a provenance marker (Local vs Remote{{tenant_id}}) so the\n   \
            caller can render or rerank by source.\n\
         4. The membrane never exposes raw vectors, only the records the owner chose\n   \
            to share. Knowing the HansaId without the HansaKey grants nothing.\n"
    );

    drop(tmp);
}
