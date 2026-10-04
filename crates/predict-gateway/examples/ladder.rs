//! Signal-to-ready-order latency: sign on the signal (`prepare`) vs take from a pre-signed
//! [`Ladder`]. Offline, sends nothing. Uses `.env` if present, else a throwaway test key.
//!
//!   cargo run --release --example ladder

use std::time::{Duration, Instant};

use predict_gateway::{Chain, Client, Config, Ladder, LimitOrder, Market, Outcome, Side};

const N: usize = 20_000;
const DEPTH: u32 = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let cfg = Config::from_env().unwrap_or(Config {
        chain: Chain::Mainnet,
        api_key: None,
        private_key: "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
        predict_account: None,
    });
    let kernel = cfg.predict_account.is_some();
    let client = Client::new(cfg)?;
    let market = Market {
        id: 0,
        title: String::new(),
        question: String::new(),
        fee_rate_bps: 200,
        decimal_precision: 2,
        is_neg_risk: false,
        is_yield_bearing: false,
        trading_status: "OPEN".into(),
        outcomes: vec![Outcome {
            name: "Yes".into(),
            on_chain_id: "52114319501245915516055106046884209969926127482827954674443846427813813222426".into(),
        }],
    };
    let template = client.template(&market, 0)?;
    println!("signer: {}\n", if kernel { "Predict smart account (2 ECDSA-path hashes)" } else { "EOA" });

    // Pseudo-random signals inside the window.
    let mut rng = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let center = 50u32;

    // A: sign on the signal.
    let mut sign_now = Vec::with_capacity(N);
    for _ in 0..N {
        let r = next();
        let tick = center - DEPTH + (r % (2 * DEPTH as u64 + 1)) as u32;
        let side = if r & 1 << 40 == 0 { Side::Buy } else { Side::Sell };
        let t = Instant::now();
        let o = client.prepare(&template, &LimitOrder::new(side, tick as f64 / 100.0, 10.0).fill_or_kill())?;
        sign_now.push(t.elapsed());
        std::hint::black_box(o);
    }

    // B: take from the ladder; refill off the clock.
    let mut ladder = Ladder::new(template, 10.0, DEPTH)?.fill_or_kill();
    let t = Instant::now();
    let built = ladder.recenter(&client, center)?;
    let build = t.elapsed();
    let mut take = Vec::with_capacity(N);
    let mut refill = Vec::with_capacity(N);
    for _ in 0..N {
        let r = next();
        let tick = center - DEPTH + (r % (2 * DEPTH as u64 + 1)) as u32;
        let side = if r & 1 << 40 == 0 { Side::Buy } else { Side::Sell };
        let t = Instant::now();
        let o = ladder.take(side, tick).expect("level signed");
        take.push(t.elapsed());
        std::hint::black_box(o);
        let t = Instant::now();
        ladder.refill(&client)?;
        refill.push(t.elapsed());
    }

    // C: cost of following the market one tick.
    let mut shift = Vec::with_capacity(1000);
    for i in 0..1000u32 {
        let c = center + if i % 2 == 0 { 1 } else { 0 };
        let t = Instant::now();
        ladder.recenter(&client, c)?;
        shift.push(t.elapsed());
    }

    println!("{:<40}{:>10}{:>10}{:>10}", "", "p50", "p99", "max");
    row("A  sign on signal (prepare)", &mut sign_now);
    row("B  ladder take (hot path)", &mut take);
    row("   refill 1 level (off hot path)", &mut refill);
    row("   recenter by 1 tick (2 levels)", &mut shift);
    println!("{:<40}{:>10}   ({built} orders, ±{DEPTH} ticks, both sides)", "   initial build", fmt(build));
    Ok(())
}

fn row(name: &str, v: &mut [Duration]) {
    v.sort_unstable();
    let p = |q: f64| v[((v.len() - 1) as f64 * q) as usize];
    println!("{:<40}{:>10}{:>10}{:>10}", name, fmt(p(0.5)), fmt(p(0.99)), fmt(v[v.len() - 1]));
}

fn fmt(d: Duration) -> String {
    let ns = d.as_nanos();
    match ns {
        0..=9_999 => format!("{ns}ns"),
        10_000..=9_999_999 => format!("{:.1}µs", ns as f64 / 1e3),
        _ => format!("{:.2}ms", ns as f64 / 1e6),
    }
}
