//! Latency benchmark. Reads `.env`.
//!
//!   cargo run --release --example bench              # local signing + connection only, sends NO orders
//!   cargo run --release --example bench -- --orders   # also measures POST /v1/orders
//!
//! With `--orders`, timings use *probe* orders signed with `feeRateBps = 0`: the server validates
//! and rejects them (`create_order_fee_rate_too_low`), so nothing can trade. One final $1
//! post-only order at the lowest tick with the real fee is placed and immediately cancelled.

use std::time::{Duration, Instant};

use predict_gateway::{Client, Config, Error, LimitOrder};

const LOCAL_N: usize = 20_000;
const WARM_N: usize = 30;
const PROBE_N: usize = 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let send_orders = std::env::args().any(|a| a == "--orders");
    let client = Client::new(Config::from_env()?)?;
    println!("chain {:?}  maker {}\n", client.chain(), client.maker());

    // ---- connection ----
    let cold = client.warm().await?;
    let mut warm = Vec::with_capacity(WARM_N);
    for _ in 0..WARM_N {
        warm.push(client.warm().await?);
    }
    let t = Instant::now();
    client.login().await?;
    let login = t.elapsed();

    // Prefer an explicit id (`-- <id>`): open_markets() pages every open market (~125 requests,
    // a quarter of the 500/min budget), so only scan when no id is given.
    let explicit_id = std::env::args().skip(1).find_map(|a| a.parse::<u64>().ok());
    let market = match explicit_id {
        Some(id) => {
            let m = client.market(id).await?;
            if !m.is_btc_5m() {
                return Err(format!("refusing: market {} is not a BTC 5-minute market", m.id).into());
            }
            m
        }
        None => client
            .open_markets()
            .await?
            .into_iter()
            .find(|m| m.trading_status == "OPEN" && m.is_btc_5m())
            .ok_or("no open BTC 5-minute market")?,
    };
    let template = client.template(&market, 0)?;
    let tick = template.tick();

    // ---- local: hash / sign / encode ----
    let (mut hash, mut sign, mut encode, mut total) = (vec![], vec![], vec![], vec![]);
    let mut first = Duration::ZERO;
    for i in 0..LOCAL_N {
        let o = client.prepare(&template, &LimitOrder::buy(tick * (1 + i % 50) as f64, 10.0))?;
        if i == 0 {
            first = o.timings.total();
        }
        hash.push(o.timings.hash);
        sign.push(o.timings.sign);
        encode.push(o.timings.encode);
        total.push(o.timings.total());
    }

    println!("{:<34}{:>10}{:>10}{:>10}", "", "p50", "p99", "max");
    row("prepare: hash (amounts+eip712)", &mut hash);
    row("prepare: sign", &mut sign);
    row("prepare: encode (json)", &mut encode);
    row("prepare: total", &mut total);
    println!("{:<34}{:>10}", "prepare: first call (cold)", fmt(first));
    println!();
    println!("{:<34}{:>10}", "connect (cold: dns+tcp+tls+h2)", fmt(cold));
    row("GET round trip (warm)", &mut warm);
    println!("{:<34}{:>10}", "login (2 requests + sign)", fmt(login));

    if !send_orders {
        println!("\n(no orders sent; pass --orders to measure POST /v1/orders)");
        return Ok(());
    }

    // ---- network: order endpoint (probes are rejected server-side) ----
    println!("\nmarket {} ({})", market.id, market.title);
    let mut probe_market = market.clone();
    probe_market.fee_rate_bps = 0;
    let probe = client.template(&probe_market, 0)?;
    let mut rtt = Vec::with_capacity(PROBE_N);
    let mut last = String::new();
    for _ in 0..PROBE_N {
        let o = client.prepare(&probe, &LimitOrder::buy(tick, 1.0 / tick).post_only())?;
        let t = Instant::now();
        let r = client.submit(o).await;
        rtt.push(t.elapsed());
        last = match r {
            Err(Error::Api { code, .. }) => code,
            Ok(p) => {
                client.cancel(&[&p.order_id]).await?;
                "UNEXPECTEDLY ACCEPTED (cancelled)".into()
            }
            Err(e) => e.to_string(),
        };
    }
    row("POST /v1/orders round trip", &mut rtt);
    println!("probe response: {last}");

    let check = client.place(&template, &LimitOrder::buy(tick, 1.0 / tick).post_only()).await?;
    let removed = client.cancel(&[&check.order_id]).await?;
    println!(
        "real order {} accepted in {}, cancelled {:?}",
        check.order_id,
        fmt(check.timings.round_trip),
        removed.removed
    );
    Ok(())
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
