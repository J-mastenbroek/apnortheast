//! Cancel latency benchmark. Reads `.env`.
//!
//!   cargo run --release --example cancel_bench              # no-op cancels only, sends NO orders
//!   cargo run --release --example cancel_bench -- --orders   # also cancels real resting orders
//!
//! Without `--orders`, cancels-by-hash for hashes that do not exist: the server looks them up and
//! answers `noop`, so the endpoint round trip is measured without trading. With `--orders`, places
//! $1 post-only BUYs at the lowest tick and cancels them three ways:
//!   by id      place, wait for the id, then `cancel(id)` (the original path)
//!   by hash    cancel body built before placing, sent with `submit_cancel` after the place returns
//!   early      place and cancel-by-hash fired together, without waiting for the place response
//! Anything an early cancel misses is cleaned up by id.

use std::time::{Duration, Instant};

use predict_gateway::crypto::keccak256;
use predict_gateway::{prepare_cancel, Client, Config, LimitOrder};

const NOOP_N: usize = 30;
const REAL_N: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let send_orders = std::env::args().any(|a| a == "--orders");
    let client = Client::new(Config::from_env()?)?;
    println!("chain {:?}  maker {}\n", client.chain(), client.maker());
    client.warm().await?;
    client.login().await?;

    // ---- local: build a cancel body ----
    let mut build = Vec::with_capacity(10_000);
    for i in 0..10_000u64 {
        let h = keccak256(&i.to_be_bytes());
        let t = Instant::now();
        std::hint::black_box(prepare_cancel(&[h]));
        build.push(t.elapsed());
    }

    // ---- network: no-op cancels on the dedicated cancel connection ----
    let mut noop = Vec::with_capacity(NOOP_N);
    let mut last = String::new();
    for i in 0..NOOP_N as u64 {
        let c = prepare_cancel(&[keccak256(&(u64::MAX - i).to_be_bytes())]);
        let t = Instant::now();
        let r = client.submit_cancel(c).await;
        noop.push(t.elapsed());
        last = match r {
            Ok(r) => format!("removed {} noop {}", r.removed.len(), r.noop.len()),
            Err(e) => e.to_string(),
        };
    }

    println!("{:<34}{:>10}{:>10}{:>10}", "", "p50", "p99", "max");
    row("prepare_cancel (cpu)", &mut build);
    row("cancel round trip (no-op)", &mut noop);
    println!("no-op response: {last}");

    if !send_orders {
        println!("\n(no orders sent; pass --orders to cancel real resting orders)");
        return Ok(());
    }

    let market = client
        .open_markets()
        .await?
        .into_iter()
        .find(|m| m.trading_status == "OPEN" && m.is_btc_5m())
        .ok_or("no open BTC 5-minute market")?;
    let template = client.template(&market, 0)?;
    let tick = template.tick();
    let order = LimitOrder::buy(tick, 1.0 / tick).post_only(); // $1 notional at the lowest tick
    println!("\nmarket {} ({})  BUY {} @ {} post-only", market.id, market.title, order.size, order.price);

    let (mut by_id, mut by_hash, mut early) = (vec![], vec![], vec![]);
    let mut early_hits = 0;
    for _ in 0..REAL_N {
        // by id
        let placed = client.place(&template, &order).await?;
        let t = Instant::now();
        let r = client.cancel(&[&placed.order_id]).await?;
        by_id.push(t.elapsed());
        check(&r.removed, "by id");

        // by hash, body prebuilt
        let signed = client.prepare(&template, &order)?;
        let cancel = prepare_cancel(&[signed.hash]);
        client.submit(signed).await?;
        let t = Instant::now();
        let r = client.submit_cancel(cancel).await?;
        by_hash.push(t.elapsed());
        check(&r.removed, "by hash");

        // early: cancel races the place; time is place sent → both answered
        let signed = client.prepare(&template, &order)?;
        let cancel = prepare_cancel(&[signed.hash]);
        let t = Instant::now();
        let (placed, removed) = tokio::join!(client.submit(signed), async {
            tokio::time::sleep(Duration::from_micros(200)).await;
            client.submit_cancel(cancel).await
        });
        early.push(t.elapsed());
        let placed = placed?;
        if removed.map(|r| !r.removed.is_empty()).unwrap_or(false) {
            early_hits += 1;
        } else {
            client.cancel(&[&placed.order_id]).await?; // cancel arrived first: clean up
        }
    }

    row("cancel by id", &mut by_id);
    row("cancel by hash (prebuilt)", &mut by_hash);
    row("early: place + cancel together", &mut early);
    println!("early cancels that removed the order: {early_hits}/{REAL_N}");
    Ok(())
}

fn check(removed: &[String], how: &str) {
    if removed.is_empty() {
        eprintln!("warning: cancel {how} removed nothing");
    }
}

fn row(name: &str, v: &mut [Duration]) {
    v.sort_unstable();
    let p = |q: f64| v[((v.len() - 1) as f64 * q) as usize];
    println!("{:<34}{:>10}{:>10}{:>10}", name, fmt(p(0.5)), fmt(p(0.99)), fmt(v[v.len() - 1]));
}

fn fmt(d: Duration) -> String {
    let ns = d.as_nanos();
    match ns {
        0..=9_999 => format!("{ns}ns"),
        10_000..=9_999_999 => format!("{:.1}µs", ns as f64 / 1e3),
        _ => format!("{:.2}ms", ns as f64 / 1e6),
    }
}
