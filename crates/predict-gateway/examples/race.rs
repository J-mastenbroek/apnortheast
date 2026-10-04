//! Race the same pre-sent order over several connections (one per Cloudflare edge IP) and take
//! the first response. Reads `.env`.
//!
//!   cargo run --release --example race -- [--rounds N] [--dedupe] [marketId]
//!
//! Probes are signed with `feeRateBps = 0` and rejected by the server, so nothing can trade.
//! Modes, interleaved, all pre-sent with the last byte held (`Hold::LastByte`):
//!   single   one copy on one connection (rotating over the edge IPs; per-IP stats printed)
//!   race3    the same order on 3 connections, one per edge IP; time to the FIRST response
//!   race6    6 connections, 2 per edge IP
//!
//! --dedupe  sends ONE real $1 post-only BUY at the lowest tick (rests, cannot cross) on 3
//!           connections at once, prints what each copy got back, then cancels it. Shows how the
//!           server treats duplicates of one signed order.

use std::time::{Duration, Instant};

use http::{HeaderMap, HeaderValue};
use predict_gateway::presend::{self, ArmedSet, Fanout, Hold, RawResponse};
use predict_gateway::{Client, Config, Error, LimitOrder};

const HOST: &str = "api.predict.fun";
const HOLD: Duration = Duration::from_millis(200);
const PACE: Duration = Duration::from_millis(500);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rounds: usize = args
        .iter()
        .position(|a| a == "--rounds")
        .and_then(|i| args.get(i + 1)?.parse().ok())
        .unwrap_or(40);
    let market_arg = args.iter().filter_map(|a| a.parse::<u64>().ok()).find(|&n| n != rounds as u64);

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let market = match market_arg {
        Some(id) => client.market(id).await?,
        None => client
            .open_markets()
            .await?
            .into_iter()
            .find(|m| m.trading_status == "OPEN" && m.is_btc_5m())
            .ok_or("no open BTC 5-minute market")?,
    };
    println!("market {} ({})", market.id, market.title);
    let real = client.template(&market, 0)?;
    let tick = real.tick();
    let order = LimitOrder::buy(tick, 1.0 / tick).post_only();
    let mut probe_market = market.clone();
    probe_market.fee_rate_bps = 0;
    let probe = client.template(&probe_market, 0)?;

    let mut headers = HeaderMap::new();
    let bearer = client.bearer().ok_or("no jwt")?;
    headers.insert("authorization", HeaderValue::from_str(&*bearer)?);
    if let Ok(key) = std::env::var("PREDICT_API_KEY") {
        headers.insert("x-api-key", HeaderValue::from_str(key.trim())?);
    }
    headers.insert("user-agent", HeaderValue::from_static("predict-gateway/race"));
    let f3 = Fanout::connect(HOST, 3, headers.clone()).await?;
    let f6 = Fanout::connect(HOST, 6, headers).await?;
    let uri = f3.conns()[0].uri("/v1/orders")?;
    let warm = f3.conns()[0].uri("/v1/auth/message")?;
    for c in f3.conns().iter().chain(f6.conns()) {
        c.get(&warm).await?;
    }

    if args.iter().any(|a| a == "--dedupe") {
        let signed = client.prepare(&real, &order)?;
        println!("\ndedupe: real order {} sent on 3 connections at once", predict_gateway::crypto::to_hex(&signed.hash));
        let set = f3.arm(&uri, signed.body().as_bytes(), Hold::LastByte, true).await?;
        tokio::time::sleep(HOLD).await;
        let copies = fire_all(set).await;
        for (i, (ttfb, r)) in copies.iter().enumerate() {
            match r {
                Ok(r) => println!("  copy {i}: {:.2} ms  {} {}", ms(*ttfb), r.status, String::from_utf8_lossy(&r.body)),
                Err(e) => println!("  copy {i}: error {e}"),
            }
        }
        let removed = client.cancel_by_hash(&[signed.hash]).await?;
        println!("  cancel_by_hash: removed {:?} noop {:?}", removed.removed, removed.noop);
        return Ok(());
    }

    let mut single: Vec<Duration> = Vec::new();
    let mut per_ip: [Vec<Duration>; 3] = Default::default();
    let mut race3: Vec<Duration> = Vec::new();
    let mut race6: Vec<Duration> = Vec::new();
    let mut wins3 = [0usize; 3];
    let mut codes: Vec<String> = Vec::new();

    for round in 0..rounds {
        // single
        let i = round % 3;
        let body = client.prepare(&probe, &order)?.body().to_owned();
        let armed = f3.conns()[i].arm(&uri, body.as_bytes(), Hold::LastByte, true).await?;
        tokio::time::sleep(HOLD).await;
        let t = Instant::now();
        let resp = armed.fire()?.await?;
        let d = t.elapsed();
        let r = presend::read_body(resp).await?;
        guard(&client, &r).await?;
        codes.push(code_of(&r));
        single.push(d);
        per_ip[i].push(d);
        tokio::time::sleep(PACE).await;

        // race3 / race6
        for (fan, out, wins) in [(&f3, &mut race3, Some(&mut wins3)), (&f6, &mut race6, None)] {
            let body = client.prepare(&probe, &order)?.body().to_owned();
            let set = fan.arm(&uri, body.as_bytes(), Hold::LastByte, true).await?;
            tokio::time::sleep(HOLD).await;
            let copies = fire_all(set).await;
            let mut best: Option<(usize, Duration)> = None;
            for (k, (d, r)) in copies.iter().enumerate() {
                if let Ok(r) = r {
                    guard(&client, r).await?;
                    codes.push(code_of(r));
                    if best.map_or(true, |(_, b)| *d < b) {
                        best = Some((k, *d));
                    }
                }
            }
            if let Some((k, d)) = best {
                out.push(d);
                if let Some(w) = wins {
                    w[k] += 1;
                }
            }
            tokio::time::sleep(PACE).await;
        }
        eprint!("\rround {}/{rounds}", round + 1);
    }
    eprintln!();

    println!("\nttfb after the signal (ms){:>8}{:>8}{:>8}{:>8}{:>8}", "min", "p25", "p50", "p90", "max");
    row("single (1 conn)", &mut single);
    row("race3 (first of 3)", &mut race3);
    row("race6 (first of 6)", &mut race6);
    for (i, v) in per_ip.iter_mut().enumerate() {
        row(&format!("  single via edge IP #{i}"), v);
    }
    println!("race3 wins per edge IP: {wins3:?}");
    let mut uniq: Vec<&String> = codes.iter().collect();
    uniq.sort();
    uniq.dedup();
    println!("response codes seen: {uniq:?}");
    Ok(())
}

/// Fire every copy and time each from the same instant.
async fn fire_all(set: ArmedSet) -> Vec<(Duration, Result<RawResponse, Error>)> {
    let t = Instant::now();
    let handles: Vec<_> = set
        .fire()
        .into_iter()
        .map(|f| {
            tokio::spawn(async move {
                let resp = f?.await?;
                let d = t.elapsed();
                Ok::<_, Error>((d, presend::read_body(resp).await?))
            })
        })
        .collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(match h.await.expect("task") {
            Ok((d, r)) => (d, Ok(r)),
            Err(e) => (Duration::MAX, Err(e)),
        });
    }
    out
}

/// Abort if a probe was ever accepted.
async fn guard(client: &Client, r: &RawResponse) -> Result<(), Box<dyn std::error::Error>> {
    if (200..300).contains(&r.status) {
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
        if let Some(id) = v["data"]["orderId"].as_str() {
            client.cancel(&[id]).await?;
        }
        return Err(format!("probe ACCEPTED (cancelled): {}", String::from_utf8_lossy(&r.body)).into());
    }
    Ok(())
}

fn code_of(r: &RawResponse) -> String {
    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
    format!("{} {}", r.status, v["error"].as_str().unwrap_or("?"))
}

fn row(name: &str, v: &mut [Duration]) {
    if v.is_empty() {
        return println!("{name:<26}  (no data)");
    }
    v.sort_unstable();
    let p = |q: f64| format!("{:.2}", ms(v[((v.len() - 1) as f64 * q) as usize]));
    println!("{name:<26}{:>8}{:>8}{:>8}{:>8}{:>8}", p(0.0), p(0.25), p(0.5), p(0.9), p(1.0));
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}
