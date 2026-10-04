//! Live latency breakdown with REAL orders on the current BTC 5-minute market (refuses any other).
//! Reads `.env`.
//!
//!   cargo run --release --example live -- <btc_5m_market_id> [--rounds N]
//!
//! Every sample is ONE real $1 post-only BUY at the lowest tick: it is accepted onto the book (the
//! full accept path), cannot cross, and is cancelled by hash as soon as every copy has answered.
//! Worst case per sample: someone sells into it before the cancel and it fills for $1.
//!
//! Variants, interleaved (start rotates every round). The clock starts at the *signal*:
//!   sign+reqwest        prepare() on the signal, Client::submit           (crate baseline)
//!   ladder+reqwest      pre-signed order from a Ladder, Client::submit
//!   ladder+fire-pool    pre-signed, Client::fire (non-blocking pool), result off the channel
//!   ladder+h2           pre-signed, raw h2 one-shot POST on one connection
//!   ladder+presend      pre-signed, armed (last byte held) on one connection, signal = fire()
//!   ladder+race6        pre-signed, full one-shot POST on 6 connections at once
//!   ladder+presend+race6  pre-signed, armed on 6 connections, signal = fire() on all
//!
//! Columns: ready = signal → order bytes in hand; sent = signal → handed to the transport;
//! accepted = signal → the 201 for the copy that got in; first = signal → first response of any copy.

use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use predict_gateway::crypto::to_hex;
use predict_gateway::presend::{self, Fanout, Hold, RawResponse};
use predict_gateway::{Client, Config, Error, Ladder, LimitOrder, Market, OrderTemplate, Side};

const HOST: &str = "api.predict.fun";
const HOLD: Duration = Duration::from_millis(200);
/// Gap after each sample (incl. its cancel). ~22 requests per round → ~280 req/min, limit is 500.
const PACE: Duration = Duration::from_millis(600);
const RACE: usize = 6;

#[derive(Clone, Copy, PartialEq)]
enum V {
    SignReqwest,
    LadderReqwest,
    LadderFire,
    LadderH2,
    LadderPresend,
    LadderRace,
    Combo,
}

const VARIANTS: [(V, &str); 7] = [
    (V::SignReqwest, "sign+reqwest"),
    (V::LadderReqwest, "ladder+reqwest"),
    (V::LadderFire, "ladder+fire-pool"),
    (V::LadderH2, "ladder+h2"),
    (V::LadderPresend, "ladder+presend"),
    (V::LadderRace, "ladder+race6"),
    (V::Combo, "ladder+presend+race6"),
];

#[derive(Default)]
struct Stats {
    ready: Vec<Duration>,
    sent: Vec<Duration>,
    accepted: Vec<Duration>,
    first: Vec<Duration>,
    errors: Vec<String>,
}

struct Ctx {
    market: Market,
    template: OrderTemplate,
    ladder: Ladder,
    order: LimitOrder,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rounds: usize = args
        .iter()
        .position(|a| a == "--rounds")
        .and_then(|i| args.get(i + 1)?.parse().ok())
        .unwrap_or(20);

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let mut results = client.results().ok_or("results channel already taken")?;

    let mut headers = HeaderMap::new();
    let bearer = client.bearer().ok_or("no jwt")?;
    headers.insert("authorization", HeaderValue::from_str(&bearer)?);
    if let Ok(key) = std::env::var("PREDICT_API_KEY") {
        headers.insert("x-api-key", HeaderValue::from_str(key.trim())?);
    }
    headers.insert("user-agent", HeaderValue::from_static("predict-gateway/live"));
    let fan = Fanout::connect(HOST, RACE, headers).await?;
    let uri = fan.conns()[0].uri("/v1/orders")?;
    let warm = fan.conns()[0].uri("/v1/auth/message")?;
    for c in fan.conns() {
        c.get(&warm).await?;
    }

    let id = args
        .iter()
        .filter_map(|a| a.parse::<u64>().ok())
        .find(|&n| n != rounds as u64)
        .ok_or("usage: live <btc_5m_market_id> [--rounds N]")?;
    let mut ctx = select_market(&client, id).await?;
    let market_uri = fan.conns()[0].uri(&format!("/v1/markets/{id}"))?;
    let mut stats: Vec<Stats> = VARIANTS.iter().map(|_| Stats::default()).collect();
    let mut accepted_total = 0usize;

    for round in 0..rounds {
        // One GET per round: market still trading, and enough rate budget for the round (~25 req).
        let r = fan.conns()[0].get(&market_uri).await?;
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
        if v["data"]["tradingStatus"] != "OPEN" {
            println!("\nmarket {id} stopped trading after {round} rounds");
            break;
        }
        if let Some((left, reset)) = rpm_budget(&r) {
            if left < 60 {
                eprint!("\rrate budget {left} left, waiting {reset}s          ");
                tokio::time::sleep(Duration::from_secs(reset + 1)).await;
            }
        }
        for k in 0..VARIANTS.len() {
            let i = (round + k) % VARIANTS.len();
            let (v, _) = VARIANTS[i];
            let single = &fan.conns()[round % 3];
            let s = &mut stats[i];
            let hash = match sample(&client, &mut results, &fan, single, &uri, &mut ctx, v, s).await {
                Ok(h) => h,
                Err(e) => {
                    s.errors.push(e.to_string());
                    None
                }
            };
            if let Some(h) = hash {
                accepted_total += 1;
                let removed = client.cancel_by_hash(&[h]).await?;
                if removed.removed.is_empty() {
                    let info = client.order(&to_hex(&h)).await?;
                    println!("\nWARNING order {} not removed by cancel: status {}", to_hex(&h), info.status);
                }
            }
            ctx.ladder.refill(&client)?;
            tokio::time::sleep(PACE).await;
        }
        eprint!("\rround {}/{rounds}  (market {})   ", round + 1, ctx.market.id);
    }
    eprintln!();

    println!("\n{accepted_total} real orders accepted and cancelled\n");
    println!(
        "{:<22}{:>3} {:>9}{:>9} | accepted ms: {:>6}{:>7}{:>7}{:>7}{:>7} | first p50  errors",
        "variant", "n", "ready", "sent", "min", "p25", "p50", "p90", "max"
    );
    for ((_, name), s) in VARIANTS.iter().zip(&mut stats) {
        let a = q(&mut s.accepted);
        println!(
            "{name:<22}{:>3} {:>9}{:>9} | {:>19}{:>7}{:>7}{:>7}{:>7} | {:>8}  {}",
            s.accepted.len(),
            us(med(&mut s.ready)),
            us(med(&mut s.sent)),
            a[0],
            a[1],
            a[2],
            a[3],
            a[4],
            q(&mut s.first)[2],
            summary(&s.errors)
        );
    }
    println!("\nready/sent in µs (median); accepted/first in ms.");
    Ok(())
}

/// One sample. Returns the hash of the accepted order, if any.
#[allow(clippy::too_many_arguments)]
async fn sample(
    client: &Client,
    results: &mut tokio::sync::mpsc::UnboundedReceiver<predict_gateway::OrderOutcome>,
    fan: &Fanout,
    single: &presend::H2Conn,
    uri: &http::Uri,
    ctx: &mut Ctx,
    v: V,
    s: &mut Stats,
) -> Result<Option<[u8; 32]>, Box<dyn std::error::Error>> {
    let tick = 1u32; // lowest tick
    match v {
        V::SignReqwest | V::LadderReqwest => {
            let t0 = Instant::now();
            let o = if v == V::SignReqwest {
                client.prepare(&ctx.template, &ctx.order)?
            } else {
                ctx.ladder.take(Side::Buy, tick).ok_or("ladder level empty")?
            };
            let ready = t0.elapsed();
            let hash = o.hash;
            let r = client.submit(o).await;
            let d = t0.elapsed();
            s.ready.push(ready);
            s.sent.push(ready);
            s.first.push(d);
            match r {
                Ok(_) => {
                    s.accepted.push(d);
                    Ok(Some(hash))
                }
                Err(Error::Api { code, .. }) => Err(code.into()),
                Err(e) => Err(e.into()),
            }
        }
        V::LadderFire => {
            let t0 = Instant::now();
            let o = ctx.ladder.take(Side::Buy, tick).ok_or("ladder level empty")?;
            let ready = t0.elapsed();
            let hash = client.fire(o);
            let sent = t0.elapsed();
            let out = loop {
                let out = results.recv().await.ok_or("results channel closed")?;
                if out.hash == hash {
                    break out;
                }
            };
            let d = t0.elapsed();
            s.ready.push(ready);
            s.sent.push(sent);
            s.first.push(d);
            match out.result {
                Ok(_) => {
                    s.accepted.push(d);
                    Ok(Some(hash))
                }
                Err(Error::Api { code, .. }) => Err(code.into()),
                Err(e) => Err(e.into()),
            }
        }
        V::LadderH2 | V::LadderRace => {
            let conns: Vec<&presend::H2Conn> =
                if v == V::LadderH2 { vec![single] } else { fan.conns().iter().collect() };
            let t0 = Instant::now();
            let o = ctx.ladder.take(Side::Buy, tick).ok_or("ladder level empty")?;
            let body = Bytes::copy_from_slice(o.body().as_bytes());
            let ready = t0.elapsed();
            let mut futs = Vec::with_capacity(conns.len());
            for c in conns {
                futs.push(c.post(uri, body.clone()).await);
            }
            let sent = t0.elapsed();
            finish(t0, ready, sent, futs, o.hash, s).await
        }
        V::LadderPresend | V::Combo => {
            let o = ctx.ladder.take(Side::Buy, tick).ok_or("ladder level empty")?;
            let body = o.body().as_bytes();
            let futs_armed = if v == V::LadderPresend {
                vec![single.arm(uri, body, Hold::LastByte, true).await?]
            } else {
                Vec::new()
            };
            let set = if v == V::Combo { Some(fan.arm(uri, body, Hold::LastByte, true).await?) } else { None };
            tokio::time::sleep(HOLD).await;
            let t0 = Instant::now();
            let futs = match set {
                Some(set) => set.fire(),
                None => futs_armed.into_iter().map(|a| a.fire()).collect(),
            };
            let sent = t0.elapsed();
            finish(t0, Duration::ZERO, sent, futs, o.hash, s).await
        }
    }
}

/// Await every copy, timed from `t0`; record the accepted copy and the first response.
async fn finish(
    t0: Instant,
    ready: Duration,
    sent: Duration,
    futs: Vec<predict_gateway::Result<h2::client::ResponseFuture>>,
    hash: [u8; 32],
    s: &mut Stats,
) -> Result<Option<[u8; 32]>, Box<dyn std::error::Error>> {
    let handles: Vec<_> = futs
        .into_iter()
        .map(|f| {
            tokio::spawn(async move {
                let resp = f?.await?;
                let d = t0.elapsed();
                Ok::<(Duration, RawResponse), Error>((d, presend::read_body(resp).await?))
            })
        })
        .collect();
    let mut first = Duration::MAX;
    let mut accepted: Vec<Duration> = Vec::new();
    let mut codes: Vec<String> = Vec::new();
    for h in handles {
        match h.await? {
            Ok((d, r)) => {
                first = first.min(d);
                if (200..300).contains(&r.status) {
                    accepted.push(d);
                } else {
                    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
                    codes.push(v["error"].as_str().unwrap_or("?").to_owned());
                }
            }
            Err(e) => codes.push(e.to_string()),
        }
    }
    s.ready.push(ready);
    s.sent.push(sent);
    if first != Duration::MAX {
        s.first.push(first);
    }
    if accepted.len() > 1 {
        println!("\nWARNING {} copies of one order accepted", accepted.len());
    }
    match accepted.first() {
        Some(&d) => {
            s.accepted.push(d);
            // Losing copies of a race are expected duplicates; anything else is worth seeing.
            for c in codes.iter().filter(|c| c.as_str() != "create_order_duplicate_order") {
                s.errors.push(format!("loser:{c}"));
            }
            Ok(Some(hash))
        }
        None => Err(codes.join("|").into()),
    }
}

/// The given market, refused unless it is an open BTC 5-minute market. Takes an id instead of
/// scanning `open_markets()`, which costs ~125 requests of the 500/min budget.
async fn select_market(client: &Client, id: u64) -> Result<Ctx, Box<dyn std::error::Error>> {
    let market = client.market(id).await?;
    if !market.is_btc_5m() || market.trading_status != "OPEN" {
        return Err(format!("market {id} is not an open BTC 5-minute market: '{}'", market.title).into());
    }
    let template = client.template(&market, 0)?;
    let tick = template.tick();
    let order = LimitOrder::buy(tick, 1.0 / tick).post_only(); // $1 at the lowest tick
    let mut ladder = Ladder::new(template.clone(), 1.0 / tick, 1)?.post_only();
    ladder.recenter(client, 2)?; // ticks 1..=3, both sides; we only use BUY @ tick 1
    eprintln!("\nmarket {} '{}' ({})", market.id, market.title, market.question);
    Ok(Ctx { market, template, ladder, order })
}

/// `(remaining, reset_secs)` of the per-minute window, from
/// `ratelimit: "rps";r=39;t=1, "rpm";r=168;t=48`.
fn rpm_budget(r: &RawResponse) -> Option<(u64, u64)> {
    let h = r.headers.get("ratelimit")?.to_str().ok()?;
    let rpm = h.split(',').find(|p| p.contains("\"rpm\""))?;
    let field = |k: &str| rpm.split(';').find_map(|kv| kv.trim().strip_prefix(k)?.parse().ok());
    Some((field("r=")?, field("t=")?))
}

fn med(v: &mut [Duration]) -> Duration {
    if v.is_empty() {
        return Duration::ZERO;
    }
    v.sort_unstable();
    v[v.len() / 2]
}

fn q(v: &mut [Duration]) -> [String; 5] {
    if v.is_empty() {
        return Default::default();
    }
    v.sort_unstable();
    let p = |x: f64| format!("{:.2}", v[((v.len() - 1) as f64 * x) as usize].as_secs_f64() * 1e3);
    [p(0.0), p(0.25), p(0.5), p(0.9), p(1.0)]
}

fn us(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

fn summary(errors: &[String]) -> String {
    let mut seen: Vec<(&String, usize)> = Vec::new();
    for e in errors {
        match seen.iter_mut().find(|(k, _)| *k == e) {
            Some((_, n)) => *n += 1,
            None => seen.push((e, 1)),
        }
    }
    seen.iter().map(|(k, n)| format!("{k} x{n}")).collect::<Vec<_>>().join(", ")
}
