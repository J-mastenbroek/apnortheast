//! Benchmark the hitter against the plain REST path on a BTC 5-minute market. Reads `.env`.
//!
//!   cargo run --release --example hitter -- <btc_5m_id> [--rounds 20] [--real]
//!
//! Each round, in rotating order, from the moment of the signal:
//!   submit   sign on the signal, Client::submit                  (baseline)
//!   cold     Hitter::hit on an unarmed level: pre-signed, sent in full on 6 connections
//!   armed    Hitter::hit on an armed level: 1 byte on each of 6 connections
//! Default: probe orders (`feeRateBps = 0`), rejected by the server after full validation, so
//! nothing trades; time = first response. `--real`: $1 post-only BUYs at the lowest ticks (rest,
//! cannot cross), cancelled at once; time = the accepted `201`.

use std::time::{Duration, Instant};

use predict_gateway::{Client, Config, Hitter, Ladder, LimitOrder, Settled, Side};

const ARMED: u32 = 1; // tick kept armed
const COLD: u32 = 2; // signed, never armed

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let real = args.iter().any(|a| a == "--real");
    let rounds: usize = args.iter().position(|a| a == "--rounds").and_then(|i| args.get(i + 1)?.parse().ok()).unwrap_or(20);
    let id = args
        .iter()
        .filter_map(|a| a.parse::<u64>().ok())
        .find(|&n| n != rounds as u64)
        .ok_or("usage: hitter <btc_5m_id> [--rounds N] [--real]")?;

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let mut market = client.market(id).await?;
    if !market.is_btc_5m() || market.trading_status != "OPEN" {
        return Err(format!("market {id} is not an open BTC 5-minute market: '{}'", market.title).into());
    }
    if !real {
        market.fee_rate_bps = 0;
    }
    let template = client.template(&market, 0)?;
    let tick = template.tick();
    let size = 1.0 / tick; // $1 at the lowest tick
    let mut ladder = Ladder::new(template.clone(), size, 2)?.post_only();
    ladder.recenter(&client, 3)?;
    let mut hitter = Hitter::new(client.fanout(6).await?, ladder);
    hitter.set_targets(&[(Side::Buy, ARMED)]);
    hitter.maintain(&client).await?;
    println!("market {} '{}'  {}", market.id, market.title, if real { "REAL orders" } else { "probe orders" });

    let names = ["submit", "cold", "armed"];
    let mut times: [Vec<Duration>; 3] = Default::default();
    let mut sends: [Vec<Duration>; 3] = Default::default();
    let mut errors: Vec<String> = Vec::new();
    for round in 0..rounds {
        for k in 0..3 {
            let v = (round + k) % 3;
            let (time, send, hash) = if v == 0 {
                let t = Instant::now();
                let order = client.prepare(&template, &LimitOrder::buy(tick, size).post_only())?;
                let (send, hash) = (t.elapsed(), order.hash);
                let r = client.submit(order).await;
                let time = t.elapsed();
                match r {
                    Ok(_) => (Some(time), send, Some(hash)),
                    Err(e) => {
                        if real || !e.to_string().contains("create_order_fee_rate_too_low") {
                            errors.push(format!("submit: {e}"));
                        }
                        ((!real).then_some(time), send, None)
                    }
                }
            } else {
                let shot = hitter.hit(Side::Buy, if v == 1 { COLD } else { ARMED }).await?;
                let (send, hash) = (shot.send, shot.hash);
                let s = shot.settle().await;
                report(&s, names[v], &mut errors);
                let time = if real { s.accepted.as_ref().map(|a| a.0) } else { s.first };
                (time, send, s.accepted.is_some().then_some(hash))
            };
            if let Some(h) = hash {
                client.cancel_by_hash(&[h]).await?;
                if !real {
                    return Err("probe order was ACCEPTED (cancelled); aborting".into());
                }
            }
            if let Some(t) = time {
                times[v].push(t);
                sends[v].push(send);
            }
            // ~1 s between samples keeps a round at ~290 req/min (limit 500).
            for _ in 0..5 {
                hitter.maintain(&client).await?;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        eprint!("\rround {}/{rounds}", round + 1);
    }
    eprintln!();

    let st = hitter.stats();
    println!(
        "\nhitter: {} arms, {} rotations, {} dead copies, {} reconnects",
        st.arms, st.rotations, st.dead_copies, st.reconnects
    );
    println!("{:<8}{:>4}{:>10} |{:>8}{:>8}{:>8}{:>8}{:>8}  ms", "", "n", "send µs", "min", "p25", "p50", "p90", "max");
    for v in 0..3 {
        let (t, s) = (&mut times[v], &mut sends[v]);
        if t.is_empty() {
            println!("{:<8}   0", names[v]);
            continue;
        }
        t.sort_unstable();
        s.sort_unstable();
        let q = |p: f64| format!("{:.2}", t[((t.len() - 1) as f64 * p) as usize].as_secs_f64() * 1e3);
        println!(
            "{:<8}{:>4}{:>10.1} |{:>8}{:>8}{:>8}{:>8}{:>8}",
            names[v],
            t.len(),
            s[s.len() / 2].as_secs_f64() * 1e6,
            q(0.0),
            q(0.25),
            q(0.5),
            q(0.9),
            q(1.0)
        );
    }
    errors.sort();
    errors.dedup();
    if !errors.is_empty() {
        println!("responses other than the expected ones: {errors:?}");
    }
    Ok(())
}

/// Collect unexpected outcomes: anything but accepted / duplicate (real) or the fee rejection (probe).
fn report(s: &Settled, name: &str, errors: &mut Vec<String>) {
    for r in &s.rejected {
        let body = String::from_utf8_lossy(&r.body);
        if !body.contains("create_order_fee_rate_too_low") {
            errors.push(format!("{name}: {} {body}", r.status));
        }
    }
    errors.extend(s.errors.iter().map(|e| format!("{name}: {e}")));
}
