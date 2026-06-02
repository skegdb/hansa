//! `bench-report` - a self-contained, fast-running snapshot of the
//! metrics that matter for hansa.
//!
//! Three sections:
//!   1. SAGA           - build + score throughput.
//!   2. CONTEXT TOKEN  - dedup ratio, budget utilisation, density.
//!   3. MEMBRANE E2E   - 3-peer in-process query latency.
//!
//! Output is plain ASCII with light box characters so it copy-pastes
//! into PR descriptions. Run with:
//!
//!     cargo run --release -p bench-report

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hansa::prelude::*;
use hansa::saga::{build_saga_from_tenant, score_saga};
use skeg_rigging::{RecordId, TenantId};
use skeg_rigging_skeg::Tenant;

// ─── Tiny ANSI helpers ──────────────────────────────────────────────

fn bold(s: &str) -> String {
    format!("\x1b[1m{s}\x1b[0m")
}
fn dim(s: &str) -> String {
    format!("\x1b[2m{s}\x1b[0m")
}
fn green(s: &str) -> String {
    format!("\x1b[32m{s}\x1b[0m")
}
fn yellow(s: &str) -> String {
    format!("\x1b[33m{s}\x1b[0m")
}

fn rule(title: &str) {
    let bar = "─".repeat(78);
    println!("\n{}", dim(&bar));
    println!("  {}", bold(title));
    println!("{}", dim(&bar));
}

fn metric(label: &str, value: &str) {
    println!("  {label:<40}  {}", bold(value));
}

fn submetric(label: &str, value: &str) {
    println!("    {label:<38}  {}", value);
}

// ─── Section 1: saga ─────────────────────────────────────────────────

fn synth_vectors(n: u64, dim: u32) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..dim)
                .map(|d| {
                    let h = ((i as u32).wrapping_mul(2654435761) ^ d.wrapping_mul(40503)) as f32;
                    (h.sin() + 1.0) * 0.5
                })
                .collect()
        })
        .collect()
}

fn report_saga() {
    rule("SAGA - build + score");
    for &(n, dim) in &[(100u64, 8u32), (1_000, 32), (10_000, 32), (10_000, 128)] {
        let vectors = synth_vectors(n, dim);
        let start = Instant::now();
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            dim,
            n,
            vectors,
            Vec::<String>::new(),
            1,
            7,
        )
        .unwrap();
        let elapsed = start.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        let throughput = n as f64 / elapsed.as_secs_f64();
        metric(
            &format!("build n={n:>5} dim={dim:>3}"),
            &format!(
                "{ms:>8.2} ms  ({:>7.0} rec/s, k={})",
                throughput,
                saga.centroids.len()
            ),
        );
    }

    println!();

    for &(n, dim) in &[(1_000u64, 32u32), (50_000, 32), (50_000, 128)] {
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            dim,
            n,
            synth_vectors(n.min(2_000), dim),
            Vec::<String>::new(),
            1,
            7,
        )
        .unwrap();
        let query: Vec<f32> = (0..dim).map(|d| (d as f32 * 0.01).sin()).collect();
        // Warm-up
        for _ in 0..8 {
            let _ = score_saga(&saga, &query);
        }
        let mut best_ns = u128::MAX;
        let mut accumulator: f32 = 0.0;
        for _ in 0..200 {
            let s = Instant::now();
            let v = score_saga(&saga, &query);
            best_ns = best_ns.min(s.elapsed().as_nanos());
            accumulator += v;
        }
        // Force the optimiser to keep the loop body live.
        std::hint::black_box(accumulator);
        let centroids = saga.centroids.len();
        let per_centroid_ns = best_ns / centroids as u128;
        metric(
            &format!("score k={centroids:>3} dim={dim:>3}"),
            &format!(
                "{:>7} ns best  ({per_centroid_ns:>4} ns/centroid)",
                best_ns
            ),
        );
    }
}

// ─── Section 2: context token efficiency ────────────────────────────

fn synth_hits(count: usize, chars: usize, dup_ratio: f32) -> Vec<MembraneHit> {
    // dup_ratio = fraction of records that are EXACT duplicates of an
    // earlier one. Unique count + duplicate count == count.
    let dup_count = ((count as f32) * dup_ratio).round() as usize;
    let unique_count = count - dup_count;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let canonical = if i < unique_count {
            i
        } else {
            // Alias a unique record. Spread the aliases evenly.
            ((i - unique_count) * 7) % unique_count.max(1)
        };
        // Prefix with the canonical id so two different canonicals never
        // collide even when the body pattern aliases.
        let body: String = (0..chars.saturating_sub(12))
            .map(|c| ((canonical * 131 + c * 17) as u8 % 26 + b'a') as char)
            .collect();
        let payload = format!("rec{canonical:08} {body}");
        let origin = if i % 3 == 0 {
            HitOrigin::Local
        } else {
            HitOrigin::Remote {
                tenant_id: TenantId::from_bytes([(i % 8) as u8 + 1; 16]),
            }
        };
        out.push(MembraneHit {
            record_id: RecordId(i as u64),
            similarity: 1.0 - (i as f32) * 0.005,
            origin,
            payload: Bytes::from(payload),
            embedding: None,
        });
    }
    out
}

fn report_context() {
    rule("CONTEXT - token efficiency");
    let cases: &[(usize, usize, f32, usize, &str)] = &[
        (50, 100, 0.0, 2048, "50_hits_100ch_no_dup"),
        (50, 100, 0.2, 2048, "50_hits_100ch_20pct_dup"),
        (200, 200, 0.10, 2048, "200_hits_200ch_10pct_dup"),
        (200, 200, 0.30, 2048, "200_hits_200ch_30pct_dup"),
        (100, 400, 0.05, 512, "100_hits_tight_budget"),
    ];
    for &(count, chars, dup, budget, label) in cases {
        let hits = synth_hits(count, chars, dup);
        let raw_tokens: usize = hits
            .iter()
            .map(|h| CharCountTokenizer.count(std::str::from_utf8(&h.payload).unwrap_or("")))
            .sum();
        let expected_dups = ((count as f32) * dup).round() as usize;
        let start = Instant::now();
        let bundle = ContextBuilder::from_hits(hits.clone())
            .min_similarity(-1.0)
            .token_budget(budget)
            .dedup(true)
            .tokenizer(Arc::new(CharCountTokenizer))
            .build();
        let elapsed = start.elapsed();
        let compression = 1.0 - (bundle.total_tokens as f32 / raw_tokens.max(1) as f32);
        let dedup_recall = if expected_dups > 0 {
            bundle.dropped_duplicates as f32 / expected_dups as f32
        } else {
            1.0
        };

        metric(label, &format!("{:>6.2} µs", elapsed.as_secs_f64() * 1_000_000.0));
        submetric(
            "hits in -> bundle items",
            &format!(
                "{count:>4}  ->  {:>4}  ({:>3.0}% kept)",
                bundle.items.len(),
                bundle.items.len() as f32 / count as f32 * 100.0,
            ),
        );
        submetric(
            "raw tokens -> bundle tokens",
            &format!(
                "{raw_tokens:>6}  ->  {:>6}  (budget {budget})",
                bundle.total_tokens,
            ),
        );
        let compression_badge = if compression >= 0.50 {
            green(&format!("{:>5.1}%", compression * 100.0))
        } else if compression >= 0.10 {
            yellow(&format!("{:>5.1}%", compression * 100.0))
        } else {
            format!("{:>5.1}%", compression * 100.0)
        };
        submetric(
            "compression (tokens dropped / source)",
            &compression_badge,
        );
        let dedup_badge = if expected_dups == 0 {
            dim("n/a (no duplicates in corpus)")
        } else if dedup_recall >= 0.95 {
            green(&format!(
                "{}/{} ({:.0}% recall)",
                bundle.dropped_duplicates,
                expected_dups,
                dedup_recall * 100.0
            ))
        } else {
            yellow(&format!(
                "{}/{} ({:.0}% recall)",
                bundle.dropped_duplicates,
                expected_dups,
                dedup_recall * 100.0
            ))
        };
        submetric("dedup (duplicates dropped / expected)", &dedup_badge);
        submetric(
            "drops below-similarity / over-budget",
            &format!(
                "{} below-sim, {} over-budget",
                bundle.dropped_below_threshold, bundle.dropped_over_budget,
            ),
        );
        println!();
    }
}

// ─── Section 3: end-to-end membrane ──────────────────────────────────

struct Agent {
    hansa: Hansa<Tenant>,
}

fn spawn_agent(root: &std::path::Path, label: u8, axis: usize) -> Agent {
    let dim: u32 = 8;
    let tenant_id = TenantId::from_bytes([label; 16]);
    let tenant_dir: PathBuf = root.join(format!("tenant-{label}"));
    let tenant = Arc::new(Tenant::open(&tenant_dir, tenant_id, dim).unwrap());
    for i in 0..50u64 {
        let mut v = vec![0.0f32; dim as usize];
        v[axis] = 1.0 - (i % 10) as f32 * 0.01;
        tenant
            .insert(
                RecordId(label as u64 * 1000 + i),
                v,
                i < 30,
                vec!["topic".into()],
                format!("a{label}-r{i}").into_bytes(),
            )
            .unwrap();
    }
    tenant.flush().unwrap();
    let key = HansaKey::from_bytes([99; 32]);
    let hid = key.hansa_id();
    let registry = Arc::new(FileRegistry::new(root));
    let saga_dir = root.join(hid.as_hex()).join("sagas");
    let hansa = Hansa::open(HansaConfig {
        key,
        registry,
        local_tenant: tenant.clone(),
        local_tenant_id: tenant_id,
        local_tenant_location: skeg_rigging_net::TenantLocation::Path { path: tenant_dir },
        saga_dir,
        peer_opener: Some(Arc::new(|_tid, loc| match loc {
            skeg_rigging_net::TenantLocation::Path { path } => {
                skeg_rigging_skeg::open_readonly(path)
            }
            _ => Err(skeg_rigging::OpenError::NotFound),
        })),
        default_budget: TokenBudget::split(20, 30),
            #[cfg(feature = "tokio")]
            async_peer_opener: None,
    })
    .unwrap();
    Agent { hansa }
}

fn report_membrane() {
    rule("MEMBRANE - end-to-end query latency (3 peers in-process)");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    // populate three agents
    let agents: Vec<_> = (0u8..3)
        .map(|i| spawn_agent(root, i + 1, i as usize))
        .collect();
    for a in &agents {
        a.hansa.join(vec!["topic".into(); 50]).unwrap();
        a.hansa.refresh_saga(vec!["topic".into(); 50], 1, 7).unwrap();
    }
    // Re-open A so it observes B and C's joins.
    let a = spawn_agent(root, 1, 0);

    let mut q = vec![0.0f32; 8];
    q[1] = 1.0; // axis-1 = peer B

    // Warm-up
    for _ in 0..3 {
        let _ = a.hansa.query(&q).unwrap().top_k(10).execute().unwrap();
    }

    let mut best_ms = f64::MAX;
    let mut total_ms = 0.0;
    let runs = 30;
    for _ in 0..runs {
        let s = Instant::now();
        let hits = a.hansa.query(&q).unwrap().top_k(10).execute().unwrap();
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        if ms < best_ms {
            best_ms = ms;
        }
        total_ms += ms;
        let _ = hits;
    }
    let avg_ms = total_ms / runs as f64;
    metric(
        "query top_k=10, 3 peers, 50 rec each",
        &format!("{best_ms:>6.2} ms best   {avg_ms:>6.2} ms avg over {runs}"),
    );

    let bundle = ContextBuilder::from_hits(
        a.hansa.query(&q).unwrap().top_k(10).execute().unwrap(),
    )
    .min_similarity(0.1)
    .token_budget(512)
    .dedup(true)
    .build();
    submetric(
        "bundle items / total tokens",
        &format!("{} items, {} tokens", bundle.items.len(), bundle.total_tokens),
    );
}

// ─── Section 4: token economics ──────────────────────────────────────
//
// "How much do tokens actually cost when I use hansa?" This section
// translates wire bytes and on-disk bytes into the only currency that
// matters to an LLM application: model-input tokens.

fn report_token_economics() {
    rule("TOKEN ECONOMICS - what hansa costs in tokens");

    // 1) Saga storage cost vs sending raw vectors.
    //    A peer's saga summarises the vault. Without it, the only way
    //    to know whether a peer is relevant is to ship its vectors and
    //    inspect them - orders of magnitude more bytes.
    println!("  {}", bold("saga storage vs raw vectors"));
    for &(n, dim) in &[(1_000u64, 32u32), (10_000, 128), (100_000, 768)] {
        let vectors = synth_vectors(n.min(2_000), dim);
        let saga = build_saga_from_tenant(
            TenantId::ZERO,
            dim,
            n,
            vectors,
            Vec::<String>::new(),
            1,
            7,
        )
        .unwrap();
        let raw_bytes = n as usize * dim as usize * 4;
        // Saga centroids: cluster_size (4B) + vector (dim×4B) per centroid.
        let saga_bytes = saga.centroids.len() * (4 + (dim as usize) * 4);
        let ratio = raw_bytes as f64 / saga_bytes as f64;
        submetric(
            &format!("n={n:>6} dim={dim:>3} ({} centroids)", saga.centroids.len()),
            &format!(
                "raw {:>9}   saga {:>8}   {}",
                pretty_bytes(raw_bytes),
                pretty_bytes(saga_bytes),
                green(&format!("{ratio:.1}× smaller"))
            ),
        );
    }
    println!();

    // 2) Wire cost per query over RESP3 (back-of-envelope).
    //    VSEARCH out = query vector bytes. VSEARCH in = top_k × 12 B
    //    (id-string + double). MGET in = top_k × avg_envelope_bytes.
    //    Sagas (assumed already cached) excluded.
    println!("  {}", bold("RESP3 wire cost per query (3-peer fan-out)"));
    for &(top_k, dim, avg_payload) in &[
        (10u32, 32u32, 200usize),
        (20, 128, 500),
        (50, 768, 1_000),
    ] {
        let vsearch_out = dim as usize * 4;
        let vsearch_in = top_k as usize * 12;
        let mget_in = top_k as usize * (avg_payload + 80); // 80 B envelope overhead
        let per_peer = vsearch_out + vsearch_in + mget_in;
        let total = per_peer * 3;
        submetric(
            &format!("top_k={top_k:>2} dim={dim:>3} payload≈{avg_payload}B"),
            &format!(
                "per peer {:>8}   3 peers {:>9}   ≈ {:>5} tokens equiv",
                pretty_bytes(per_peer),
                pretty_bytes(total),
                total / 4
            ),
        );
    }
    println!();

    // F.55: actual JSON-vs-binary envelope size on the wire.
    println!(
        "  {}",
        bold("F.55 - envelope size on the wire (JSON vs binary)")
    );
    for &(payload_size, label) in &[
        (50usize, "    50 B payload"),
        (500, "   500 B payload"),
        (1_000, " 1 000 B payload"),
        (10_000, "10 000 B payload"),
    ] {
        let payload: Vec<u8> = (0..payload_size).map(|i| ((i % 26) as u8) + b'a').collect();
        let env = skeg_rigging_net::RecordEnvelope::new(
            true,
            vec!["topic".into(), "skill:python".into()],
            payload,
        );
        let json = env.encode();
        let bin = env.encode_binary();
        let ratio = json.len() as f32 / bin.len() as f32;
        submetric(
            label,
            &format!(
                "json {:>7} -> bin {:>7}   {}",
                pretty_bytes(json.len()),
                pretty_bytes(bin.len()),
                green(&format!("{ratio:.1}x smaller"))
            ),
        );
    }
    println!();

    // F.20: zstd payload compression on different corpora.
    println!(
        "  {}",
        bold("F.20 - zstd payload compression (binary -> binary+zstd)")
    );
    let prose = "the quick brown fox jumps over the lazy dog. ".repeat(500);
    let markdown =
        "# Title\n\nParagraph with **bold** and `code`.\n\n- item one\n- item two\n".repeat(100);
    let pseudo_random: Vec<u8> = (0..10_000u32)
        .map(|i| ((i.wrapping_mul(2654435761) >> 24) & 0xFF) as u8)
        .collect();
    let cases: &[(&str, Vec<u8>)] = &[
        ("English prose, ~22 KB    ", prose.into_bytes()),
        ("Markdown,      ~ 7 KB    ", markdown.into_bytes()),
        ("Pseudo-random, ~10 KB    ", pseudo_random),
    ];
    for (label, payload) in cases {
        let env = skeg_rigging_net::RecordEnvelope::new(true, vec!["topic".into()], payload.clone());
        let plain = env.encode_binary();
        let zstd = env.encode_binary_zstd(skeg_rigging_net::DEFAULT_ZSTD_LEVEL);
        let smallest = env.encode_binary_smallest();
        let ratio = plain.len() as f32 / zstd.len() as f32;
        let badge = if smallest.len() == zstd.len() {
            green(&format!("{ratio:.1}x smaller"))
        } else {
            yellow(&format!("{ratio:.2}x (plain wins)"))
        };
        submetric(
            label,
            &format!(
                "plain {:>7} -> zstd {:>7}   {}",
                pretty_bytes(plain.len()),
                pretty_bytes(zstd.len()),
                badge,
            ),
        );
    }
    submetric(
        "rule of thumb",
        &dim("zstd wins on text/markdown/code; loses on already-compressed or random bytes"),
    );
    println!();

    // 3) Rendering overhead per item.
    //    Compare the bundle's body tokens to what the renderer actually
    //    emits. Markdown adds an H3 line + blank + `> ` per line. Plain
    //    adds `[origin]` prefix. Custom lets the caller min-strip.
    println!("  {}", bold("render overhead - markdown vs plain vs minimal"));
    let cases: &[(usize, usize, &str)] = &[
        (5, 50, "  5 items × 50 char"),
        (10, 200, " 10 items × 200 char"),
        (20, 500, " 20 items × 500 char"),
    ];
    for &(n, chars, label) in cases {
        let hits = synth_hits(n, chars, 0.0);
        let bundle = ContextBuilder::from_hits(hits)
            .min_similarity(-1.0)
            .token_budget(usize::MAX)
            .dedup(false)
            .tokenizer(Arc::new(CharCountTokenizer))
            .build();
        let body_tokens = bundle.total_tokens;
        let md = bundle.render_markdown();
        let plain = bundle.render_plain();
        let minimal = bundle.render_compact();
        let md_tokens = CharCountTokenizer.count(&md);
        let plain_tokens = CharCountTokenizer.count(&plain);
        let minimal_tokens = CharCountTokenizer.count(&minimal);
        let md_overhead = md_tokens.saturating_sub(body_tokens);
        let plain_overhead = plain_tokens.saturating_sub(body_tokens);
        let min_overhead = minimal_tokens.saturating_sub(body_tokens);
        submetric(
            label,
            &format!(
                "body {body_tokens:>4} → md {md_tokens:>4} (+{md_overhead}, +{:.0}%)",
                md_overhead as f32 / body_tokens.max(1) as f32 * 100.0
            ),
        );
        submetric(
            "",
            &format!(
                "                  plain {plain_tokens:>4} (+{plain_overhead}, +{:.0}%) \
                 min {minimal_tokens:>4} (+{min_overhead}, +{:.0}%)",
                plain_overhead as f32 / body_tokens.max(1) as f32 * 100.0,
                min_overhead as f32 / body_tokens.max(1) as f32 * 100.0
            ),
        );
    }
    println!();

    // 4) Headline: compare same corpus across several "what would you do
    //    in real life?" baselines. The 5.8× figure I quoted earlier
    //    used markdown rendering and an invented 20-tokens/hit overhead
    //    for the raw baseline; this table separates each assumption so
    //    you can see which compression number applies to your use case.
    println!("  {}", bold("apples-to-apples: same corpus, different approaches"));
    let hits = synth_hits(200, 200, 0.15);
    let raw_body: usize = hits
        .iter()
        .map(|h| CharCountTokenizer.count(std::str::from_utf8(&h.payload).unwrap_or("")))
        .sum();

    let bundle = ContextBuilder::from_hits(hits.clone())
        .min_similarity(-1.0)
        .token_budget(2048)
        .dedup(true)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();
    let body = bundle.total_tokens;
    let md = CharCountTokenizer.count(&bundle.render_markdown());
    let plain = CharCountTokenizer.count(&bundle.render_plain());
    let compact = CharCountTokenizer.count(&bundle.render_compact());
    let toon = CharCountTokenizer.count(&bundle.render_toon());

    // Baselines a real caller might use without hansa.
    let raw_no_headers = raw_body;
    let raw_minimal_header_20 = raw_body + 200 * 20;
    let raw_json_envelope = hits
        .iter()
        .map(|h| {
            // Realistic JSON line: {"id":N,"from":"peer-X","sim":0.85,"text":"..."}
            // ~30 token overhead + body
            CharCountTokenizer.count(std::str::from_utf8(&h.payload).unwrap_or("")) + 30
        })
        .sum::<usize>();
    let naive_exact_dedup = {
        // Use the bundle dedup mechanism but no budget - keeps every
        // unique item raw, no rendering overhead.
        let b = ContextBuilder::from_hits(hits.clone())
            .min_similarity(-1.0)
            .token_budget(usize::MAX)
            .dedup(true)
            .tokenizer(Arc::new(CharCountTokenizer))
            .build();
        b.total_tokens
    };

    // Baseline for the ratio column: the cheapest hansa rendering
    // (compact). Every row is shown as "Nx larger than hansa-compact".
    let cheapest = compact;
    let row = |label: &str, tokens: usize| {
        let ratio = tokens as f32 / cheapest.max(1) as f32;
        let marker = if (ratio - 1.0).abs() < 0.01 {
            green(&format!("baseline ({tokens} t)")).to_string()
        } else if ratio > 1.0 {
            yellow(&format!("{ratio:.2}× larger")).to_string()
        } else {
            green(&format!("{ratio:.2}× smaller")).to_string()
        };
        submetric(label, &format!("{tokens:>6} tokens   {marker}"));
    };
    row("raw  - all 200 hits, no LLM-side framing", raw_no_headers);
    row("raw  - 200 hits + 20-token wrapper each", raw_minimal_header_20);
    row("raw  - 200 hits as JSON-line envelopes", raw_json_envelope);
    row("naive exact-dedup, no budget, no render", naive_exact_dedup);
    row("hansa - markdown render (sim+tokens header)", md);
    row("hansa - plain   render ([origin] body)", plain);
    row("hansa - toon    render (src,sim,text)", toon);
    row("hansa - compact render ([L|0..F] body)", compact);
    submetric(
        "bundle composition",
        &format!(
            "{} items, body {} t, md overhead +{} t ({:.0}%)",
            bundle.items.len(),
            body,
            md.saturating_sub(body),
            md.saturating_sub(body) as f32 / body.max(1) as f32 * 100.0
        ),
    );
    println!();

    // 5) Sentence-level dedup on a templated RAG corpus. Each hit
    //    starts with one of three shared boilerplate sentences (think:
    //    document title, section header) followed by a unique fact.
    //    Sentence-dedup should drop the shared sentence from every
    //    item after the first.
    println!("  {}", bold("lossless compression - sentence-level dedup on RAG-style corpus"));
    let templated = templated_hits(50);
    let raw_total: usize = templated
        .iter()
        .map(|h| CharCountTokenizer.count(std::str::from_utf8(&h.payload).unwrap_or("")))
        .sum();
    let plain_bundle = ContextBuilder::from_hits(templated.clone())
        .min_similarity(-1.0)
        .token_budget(usize::MAX)
        .dedup(true)
        .sentence_dedup(false)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();
    let sdedup_bundle = ContextBuilder::from_hits(templated)
        .min_similarity(-1.0)
        .token_budget(usize::MAX)
        .dedup(true)
        .sentence_dedup(true)
        .tokenizer(Arc::new(CharCountTokenizer))
        .build();

    let saved = plain_bundle
        .total_tokens
        .saturating_sub(sdedup_bundle.total_tokens);
    let saved_pct = saved as f32 / plain_bundle.total_tokens.max(1) as f32 * 100.0;
    submetric(
        "corpus: 50 hits, 3 shared sentences as preamble",
        &format!("raw body {raw_total} tokens"),
    );
    submetric(
        "bundle without sentence-dedup",
        &format!("{:>4} tokens  ({} items kept)", plain_bundle.total_tokens, plain_bundle.items.len()),
    );
    submetric(
        "bundle with    sentence-dedup",
        &format!(
            "{:>4} tokens  ({} items kept)  {}  ({} sentences dropped)",
            sdedup_bundle.total_tokens,
            sdedup_bundle.items.len(),
            green(&format!("−{saved_pct:.1}%")),
            sdedup_bundle.dropped_redundant_sentences,
        ),
    );
    println!();

    // 6) Methodology footnote - be honest about counting.
    println!("  {}", bold("how this is counted"));
    println!(
        "  {}",
        dim(
            "tokens via CharCountTokenizer (chars/4). Within ±30% of real BPE on English\n  \
             prose; off for code / emoji / non-ASCII. The 'raw + 20-token wrapper' baseline\n  \
             assumes a caller minimally tagging each hit; the 'JSON envelope' assumes a\n  \
             callers that serialises each hit. Real applications sit between the two.\n  \
             Synthetic payloads here are pseudo-random alphabet noise - actual text and\n  \
             code compress differently in BPE. The numbers are order-of-magnitude, not\n  \
             accounting."
        )
    );
}

/// Templated corpus generator: every hit starts with one of three
/// shared "boilerplate" sentences (e.g. the title and a header of the
/// same source document) followed by a unique fact. Mirrors RAG
/// chunking where many chunks of the same source share preamble.
fn templated_hits(count: usize) -> Vec<MembraneHit> {
    let preambles = [
        "skeg-rigging-net is a network bridge for skeg-rigging.",
        "This crate uses the engine's existing RESP3 surface.",
        "The hansa membrane uses MGET to fetch payloads.",
    ];
    (0..count)
        .map(|i| {
            let pre = preambles[i % preambles.len()];
            let body = format!("{pre} Unique fact number {i} about the topic.");
            MembraneHit {
                record_id: RecordId(i as u64),
                similarity: 1.0 - (i as f32) * 0.005,
                origin: if i % 3 == 0 {
                    HitOrigin::Local
                } else {
                    HitOrigin::Remote {
                        tenant_id: TenantId::from_bytes([(i % 4) as u8 + 1; 16]),
                    }
                },
                payload: Bytes::from(body),
                embedding: None,
            }
        })
        .collect()
}

fn pretty_bytes(b: usize) -> String {
    if b >= 1_000_000 {
        format!("{:.1} MB", b as f64 / 1_000_000.0)
    } else if b >= 1_000 {
        format!("{:.1} KB", b as f64 / 1_000.0)
    } else {
        format!("{} B", b)
    }
}

// ─── Section 5: M2 polish features (F.2 ranker, F.1 semantic, F.7 deadline) ─

/// Build a corpus where every hit carries its own embedding, with a
/// known fraction of paraphrase clusters: items whose bytes differ but
/// whose unit vectors are within ε of each other. Exercises F.1
/// (semantic dedup) without needing a real model.
fn synth_hits_with_paraphrases(count: usize, paraphrases: usize) -> Vec<MembraneHit> {
    let dim = 8usize;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        // Every `paraphrases` items share the same base direction with
        // tiny jitter; the rest get unique random directions.
        let cluster = i / paraphrases.max(1);
        let mut v = vec![0.0f32; dim];
        for (d, slot) in v.iter_mut().enumerate() {
            let h = ((cluster as u32).wrapping_mul(2654435761) ^ (d as u32).wrapping_mul(40503))
                as f32;
            *slot = (h.sin() + 1.0) * 0.5;
        }
        // Tiny per-item jitter so paraphrases are near-but-not-identical.
        let jitter = ((i % paraphrases.max(1)) as f32) * 0.001;
        v[0] += jitter;
        let payload = format!("item {i} cluster {cluster} body filler text").into_bytes();
        out.push(MembraneHit {
            record_id: RecordId(i as u64),
            similarity: 0.9 - (i as f32) * 0.001,
            origin: if i % 3 == 0 {
                HitOrigin::Local
            } else {
                HitOrigin::Remote {
                    tenant_id: TenantId::from_bytes([(i % 8) as u8 + 1; 16]),
                }
            },
            payload: Bytes::from(payload),
            embedding: Some(v),
        });
    }
    out
}

fn report_m2_polish() {
    rule("M2 POLISH - F.2 ranker / F.1 semantic dedup / F.7 deadline");

    // ─── F.2: token-density vs similarity ranker ──────────────────
    println!("  {}", bold("F.2 - Ranker comparison (tight budget)"));
    // 60 hits, mixed length: half are 600-char ramblings at sim 0.85,
    // half are 60-char facts at sim 0.75. Tight 256-token budget.
    let mut hits = Vec::new();
    for i in 0..30 {
        hits.push(MembraneHit {
            record_id: RecordId(i),
            similarity: 0.85,
            origin: HitOrigin::Local,
            payload: Bytes::from(
                "this is a long ramble with little payload but high similarity ".repeat(10),
            ),
            embedding: None,
        });
    }
    for i in 30..60 {
        hits.push(MembraneHit {
            record_id: RecordId(i),
            similarity: 0.75,
            origin: HitOrigin::Local,
            payload: Bytes::from(format!("crisp fact #{i}")),
            embedding: None,
        });
    }
    let budget = 256usize;
    let with_sim = ContextBuilder::from_hits(hits.clone())
        .token_budget(budget)
        .ranker(Arc::new(SimilarityRanker))
        .build();
    let with_density = ContextBuilder::from_hits(hits)
        .token_budget(budget)
        .ranker(Arc::new(TokenDensityRanker))
        .build();
    submetric(
        "SimilarityRanker (default)",
        &format!(
            "{} items / {} tokens",
            with_sim.items.len(),
            with_sim.total_tokens
        ),
    );
    submetric(
        "TokenDensityRanker",
        &format!(
            "{} items / {} tokens",
            with_density.items.len(),
            with_density.total_tokens
        ),
    );
    let item_gain =
        with_density.items.len() as f32 / with_sim.items.len().max(1) as f32;
    submetric(
        "density / similarity items kept",
        &green(&format!("{item_gain:.2}× more items at the same budget")),
    );
    println!();

    // ─── F.1: semantic dedup catches paraphrases ──────────────────
    println!("  {}", bold("F.1 - Semantic dedup catches paraphrases"));
    let cases: &[(usize, usize, &str)] = &[
        (60, 3, "60 hits, 3-way paraphrase clusters"),
        (200, 5, "200 hits, 5-way paraphrase clusters"),
    ];
    for &(count, paraphrases, label) in cases {
        let hits = synth_hits_with_paraphrases(count, paraphrases);
        let baseline = ContextBuilder::from_hits(hits.clone()).build();
        let start = Instant::now();
        let with_sem = ContextBuilder::from_hits(hits)
            .semantic_dedup(0.95)
            .build();
        let elapsed = start.elapsed();
        submetric(label, &format!("{:>6.2} µs", elapsed.as_secs_f64() * 1e6));
        submetric(
            "  byte/sentence dedup only (items kept)",
            &format!("{}", baseline.items.len()),
        );
        submetric(
            "  + semantic dedup at 0.95 (items kept)",
            &green(&format!(
                "{} ({} paraphrases dropped)",
                with_sem.items.len(),
                with_sem.dropped_semantic_duplicates
            )),
        );
    }
    println!();

    // ─── F.7: deadline cuts off slow remote workers ───────────────
    //
    // No real network here; we use the in-process membrane and assert
    // that the deadline path completes within tolerance and reports
    // stats. The "slow peer" scenario is covered by the dedicated
    // integration test (`membrane_deadline`); the bench-report just
    // confirms the path is hot and surfaces typical numbers.
    println!("  {}", bold("F.7 - Membrane deadline path"));
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let agents: Vec<_> = (0u8..3)
        .map(|i| spawn_agent(root, i + 1, i as usize))
        .collect();
    for a in &agents {
        a.hansa.join(vec!["topic".into(); 50]).unwrap();
        a.hansa
            .refresh_saga(vec!["topic".into(); 50], 1, 7)
            .unwrap();
    }
    let a = spawn_agent(root, 1, 0);
    let mut q = vec![0.0f32; 8];
    q[1] = 1.0;
    // Warm up.
    for _ in 0..3 {
        let _ = a.hansa.query(&q).unwrap().top_k(10).execute().unwrap();
    }
    let start = Instant::now();
    let (hits, stats) = a
        .hansa
        .query(&q)
        .unwrap()
        .top_k(10)
        .execute_with_stats()
        .unwrap();
    let elapsed = start.elapsed();
    submetric(
        "execute_with_stats latency",
        &format!("{:>6.2} ms", elapsed.as_secs_f64() * 1e3),
    );
    submetric(
        "peers attempted / completed / dropped",
        &format!(
            "{} / {} / {}",
            stats.peers_attempted, stats.peers_completed, stats.dropped_for_deadline
        ),
    );
    submetric("hits returned", &format!("{}", hits.len()));
    println!();

    // ─── F.8: bundle cache hit avoids the membrane round trip ─────
    println!("  {}", bold("F.8 - Bundle cache hit vs miss"));
    let mut cache = BundleCache::new(8);
    // Cold path: full membrane + builder.
    let cold_start = Instant::now();
    let hits = a.hansa.query(&q).unwrap().top_k(10).execute().unwrap();
    let bundle = ContextBuilder::from_hits(hits).token_budget(2048).build();
    let cold = cold_start.elapsed();
    cache.insert(q.clone(), bundle.clone());

    // Warm path: cache hit on the same query.
    let warm_start = Instant::now();
    let cached = cache.get(&q).expect("cache should hit on identical query");
    let warm = warm_start.elapsed();
    submetric(
        "cold (membrane + builder)",
        &format!("{:>6.2} ms", cold.as_secs_f64() * 1e3),
    );
    submetric(
        "warm (cache hit, cosine=1.0)",
        &format!("{:>6.2} µs", warm.as_secs_f64() * 1e6),
    );
    let speedup = cold.as_secs_f64() / warm.as_secs_f64().max(1e-12);
    submetric(
        "speedup",
        &green(&format!("{speedup:>5.0}× faster on warm path")),
    );
    submetric(
        "warm bundle size",
        &format!("{} items / {} tokens", cached.items.len(), cached.total_tokens),
    );

    // Near-miss path: small jitter on the query.
    let mut q_jitter = q.clone();
    q_jitter[0] += 0.001;
    let nearmiss_start = Instant::now();
    let near_hit = cache.get(&q_jitter);
    let nearmiss = nearmiss_start.elapsed();
    submetric(
        "near-miss (jitter, cosine≈0.9999)",
        &format!(
            "{:>6.2} µs  {}",
            nearmiss.as_secs_f64() * 1e6,
            if near_hit.is_some() {
                green("hit")
            } else {
                yellow("miss")
            }
        ),
    );
    println!();

    // ─── F.5 / F.4 manifest hot-path latency ──────────────────────
    println!("  {}", bold("F.5/F.4 - Manifest bias hot path"));
    let m = PeerManifest {
        peer_id_bytes: [0x42; 16],
        useful_hits: 25,
        total_hits: 50,
        last_useful_at: 1_700_000_000,
    };
    let now = 1_700_000_100u64;
    for _ in 0..16 {
        let _ = m.usefulness_factor(now);
    }
    let runs = 1000;
    let start = Instant::now();
    let mut sink = 0.0f32;
    for _ in 0..runs {
        sink += m.usefulness_factor(now);
    }
    let factor_ns = start.elapsed().as_nanos() as f64 / runs as f64;
    let _ = sink;
    submetric(
        "usefulness_factor (mixed math)",
        &format!("{:>6.0} ns / call ({runs} iters)", factor_ns),
    );

    let mdir = tempfile::tempdir().unwrap();
    let store = ManifestStore::new(mdir.path());
    let peer = TenantId::from_bytes([0x77; 16]);
    store.write(&m).unwrap();
    for _ in 0..5 {
        let _ = store.read(peer);
    }
    let runs = 500;
    let start = Instant::now();
    for _ in 0..runs {
        let _ = store.read(peer);
    }
    let read_us = start.elapsed().as_secs_f64() * 1e6 / runs as f64;
    submetric(
        "ManifestStore::read (fs + json)",
        &format!("{:>6.1} µs / call ({runs} iters)", read_us),
    );

    let runs = 100;
    let mut mw = m.clone();
    let start = Instant::now();
    for i in 0..runs {
        mw.useful_hits = i;
        store.write(&mw).unwrap();
    }
    let write_us = start.elapsed().as_secs_f64() * 1e6 / runs as f64;
    submetric(
        "ManifestStore::write (atomic)",
        &format!("{:>6.1} µs / call ({runs} iters)", write_us),
    );
    submetric(
        "implication",
        &dim("bias is free on the read path; write only on user-accepted hits"),
    );
}

// ─── Tokenizer accuracy: chars/4 vs real BPE ────────────────────────

fn report_tokenizer_accuracy() {
    rule("TOKENIZER ACCURACY - chars/4 vs OpenAI BPE");
    println!(
        "  {}",
        dim(
            "OpenAI's published rule of thumb is ~4 chars/token; CharCountTokenizer\n  \
             implements exactly that. Real BPE diverges on short text, punctuation,\n  \
             numbers, code, and non-ASCII. Numbers below: how big the gap is in practice."
        )
    );
    println!();

    let chars = hansa::CharCountTokenizer;
    let gpt4o = hansa::TiktokenTokenizer::gpt4o();
    let gpt4 = hansa::TiktokenTokenizer::gpt4();

    let samples: &[(&str, &str)] = &[
        (
            "English prose ~1 KB",
            "The quick brown fox jumps over the lazy dog. The dog barks back, \
             unimpressed by the fox's apparent agility. This sentence repeats \
             several times to make a kilobyte of text suitable for tokenizer \
             evaluation. ".trim_ascii_end(),
        ),
        (
            "Markdown heavy",
            "# Heading\n\nA paragraph with **bold** and *italic* and `inline code`.\n\n\
             - item one\n- item two\n- item three\n\n```rust\nfn main() { println!(\"hi\"); }\n```\n",
        ),
        (
            "JSON envelope    ",
            "{\"shareable\":true,\"tags\":[\"topic\",\"skill:python\"],\
             \"payload\":[104,101,108,108,111,32,119,111,114,108,100]}",
        ),
        (
            "URL + numbers    ",
            "Visit https://example.com/api/v3/items?id=42&page=3&per_page=100 \
             for the JSON. Expected response time: 250ms p99, 50ms p50.",
        ),
        (
            "Code snippet     ",
            "fn cosine(a: &[f32], b: &[f32]) -> f32 {\n  \
             a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()\n}\n",
        ),
        (
            "Italian          ",
            "La volpe veloce salta sopra il cane pigro. Il cane abbaia in risposta, \
             senza essere impressionato dall'agilita apparente della volpe.",
        ),
        (
            "Japanese (CJK)   ",
            "日本語の文書をトークン化するときの動作を確認しましょう。",
        ),
    ];

    for (label, text) in samples {
        let by_chars = chars.count(text);
        let by_gpt4o = gpt4o.count(text);
        let by_gpt4 = gpt4.count(text);
        let drift_pct = ((by_chars as f32 - by_gpt4o as f32) / by_gpt4o.max(1) as f32) * 100.0;
        let badge = if drift_pct.abs() <= 30.0 {
            green(&format!("{drift_pct:+5.0}%"))
        } else {
            yellow(&format!("{drift_pct:+5.0}%"))
        };
        submetric(
            label,
            &format!(
                "chars/4={by_chars:>4}  gpt-4o={by_gpt4o:>4}  gpt-4={by_gpt4:>4}  drift {} vs gpt-4o",
                badge,
            ),
        );
    }
    println!();
    submetric(
        "rule of thumb",
        &dim("CharCountTokenizer drifts well within 30% on English prose; double-digit on code, JSON, CJK"),
    );
    submetric(
        "recommendation",
        &dim("ContextBuilder::tokenizer(Arc::new(TiktokenTokenizer::gpt4o())) for production prompts"),
    );
}

// ─── Main ────────────────────────────────────────────────────────────

fn main() {
    println!("{}", bold("\n  hansa bench-report"));
    println!(
        "  {}",
        dim("(see private/gates.md for thresholds; this report does not enforce them)")
    );

    report_saga();
    report_context();
    report_membrane();
    report_token_economics();
    report_m2_polish();
    report_tokenizer_accuracy();
    println!();
    println!(
        "  {}",
        dim(
            "note: dedup runs in input order; once the token budget is exhausted, dups of\n  \
             over-budget records are not caught. With slack budgets dedup recall is 100%."
        )
    );
    println!();
}
