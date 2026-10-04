//! Does pre-sending an order over HTTP/2 (headers + body sent early, only the tail on the signal)
//! actually cut latency through Cloudflare to predict.fun? Reads `.env`.
//!
//!   cargo run --release --example presend -- [--rounds N] [--holds] [--ladder] [marketId]
//!
//! Every order sent is a *probe* signed with `feeRateBps = 0`, which the server rejects
//! (`create_order_fee_rate_too_low`) after parsing the full body, so nothing can trade. If a probe
//! is ever accepted it is cancelled and the run aborts.
//!
//! Modes, interleaved round-robin on one warm connection (the clock starts at the signal):
//!   reqwest      Client::submit (baseline, as used today)
//!   h2-oneshot   raw h2: headers + body + END_STREAM at the signal
//!   presend-es   whole body pre-sent with content-length; signal = empty DATA + END_STREAM
//!   presend-lb   body minus last byte pre-sent with content-length; signal = last byte + END_STREAM
//!   presend-lb0  same without content-length
//! "early" counts responses that arrived *before* the signal, i.e. the server did not wait.
//!
//! --holds   how long an armed stream survives (1 s … 60 s held open before firing)
//! --ladder  arm 22 streams (a ±5-tick ladder), fire one, cancel 21: does it still help?

use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use predict_gateway::presend::{self, Armed, H2Conn, Hold, RawResponse};
use predict_gateway::{Client, Config, Error, LimitOrder, OrderTemplate};

const HOST: &str = "api.predict.fun";
const HOLD: Duration = Duration::from_millis(200);
/// Gap between requests, to stay well under 240 req/min.
const PACE: Duration = Duration::from_millis(350);

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Reqwest,
    OneShot,
    PresendEs,
    PresendLb,
    PresendLb0,
}

const MODES: [(Mode, &str); 5] = [
    (Mode::Reqwest, "reqwest"),
    (Mode::OneShot, "h2-oneshot"),
    (Mode::PresendEs, "presend-es"),
    (Mode::PresendLb, "presend-lb"),
    (Mode::PresendLb0, "presend-lb0"),
];

#[derive(Default)]
struct Stats {
    ttfb: Vec<Duration>,
    total: Vec<Duration>,
    early: usize,
    errors: Vec<String>,
    codes: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |f: &str| args.iter().any(|a| a == f);
    let rounds: usize = args
        .iter()
        .position(|a| a == "--rounds")
        .and_then(|i| args.get(i + 1)?.parse().ok())
        .unwrap_or(20);
    let market_arg = args.iter().filter_map(|a| a.parse::<u64>().ok()).find(|&n| n != rounds as u64);

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;

    let mut market = match market_arg {
        Some(id) => client.market(id).await?,
        None => client
            .open_markets()
            .await?
            .into_iter()
            .find(|m| m.trading_status == "OPEN" && m.is_btc_5m())
            .ok_or("no open BTC 5-minute market")?,
    };
    println!("market {} ({})  fee {} bps -> probes use 0", market.id, market.title, market.fee_rate_bps);
    market.fee_rate_bps = 0;
    let probe = client.template(&market, 0)?;
    let tick = probe.tick();
    let order = LimitOrder::buy(tick, 1.0 / tick).post_only();

    // Sanity: the probe must be rejected via the normal path before we use it anywhere else.
    match client.submit(client.prepare(&probe, &order)?).await {
        Err(Error::Api { status, code, .. }) => println!("probe check: rejected {status} {code}"),
        Ok(p) => {
            client.cancel(&[&p.order_id]).await?;
            return Err("probe was ACCEPTED (cancelled) - aborting".into());
        }
        Err(e) => return Err(e.into()),
    }

    let mut headers = HeaderMap::new();
    let bearer = client.bearer().ok_or("no jwt")?;
    headers.insert("authorization", HeaderValue::from_str(&*bearer)?);
    if let Ok(key) = std::env::var("PREDICT_API_KEY") {
        headers.insert("x-api-key", HeaderValue::from_str(key.trim())?);
    }
    headers.insert("user-agent", HeaderValue::from_static("predict-gateway/presend"));
    let conn = H2Conn::connect(HOST, None, headers).await?;
    let orders_uri = conn.uri("/v1/orders")?;
    let warm_uri = conn.uri("/v1/auth/message")?;
    for _ in 0..5 {
        conn.get(&warm_uri).await?;
    }

    // ---- main comparison ----
    let mut stats: Vec<Stats> = MODES.iter().map(|_| Stats::default()).collect();
    for round in 0..rounds {
        for (i, (mode, _)) in MODES.iter().enumerate() {
            let body = client.prepare(&probe, &order)?.body().to_owned();
            let s = &mut stats[i];
            match *mode {
                Mode::Reqwest => {
                    let o = client.prepare(&probe, &order)?;
                    let t = Instant::now();
                    let r = client.submit(o).await;
                    s.total.push(t.elapsed());
                    match r {
                        Err(Error::Api { code, .. }) => s.codes.push(code),
                        Ok(p) => abort_accepted(&client, &p.order_id).await?,
                        Err(e) => s.errors.push(e.to_string()),
                    }
                }
                Mode::OneShot => {
                    let t = Instant::now();
                    let fut = conn.post(&orders_uri, Bytes::from(body)).await?;
                    finish(&client, s, t, fut).await?;
                }
                Mode::PresendEs | Mode::PresendLb | Mode::PresendLb0 => {
                    let (hold, cl) = match *mode {
                        Mode::PresendEs => (Hold::EndStream, true),
                        Mode::PresendLb => (Hold::LastByte, true),
                        _ => (Hold::LastByte, false),
                    };
                    let armed = conn.arm(&orders_uri, body.as_bytes(), hold, cl).await?;
                    if let Some(armed) = wait_or_early(&client, s, armed, HOLD).await? {
                        let t = Instant::now();
                        let fut = armed.fire()?;
                        finish(&client, s, t, fut).await?;
                    }
                }
            }
            tokio::time::sleep(PACE).await;
        }
        eprint!("\rround {}/{rounds}", round + 1);
    }
    eprintln!();

    println!("\n{:<13}{:>4}{:>7} | ttfb ms: {:>6}{:>7}{:>7}{:>7} | total p50   codes / errors", "mode", "n", "early", "min", "p25", "p50", "p90");
    for ((_, name), s) in MODES.iter().zip(&mut stats) {
        let ttfb = if s.ttfb.is_empty() { &mut s.total } else { &mut s.ttfb };
        let q = quantiles(ttfb);
        let total = quantiles(&mut s.total);
        println!(
            "{name:<13}{:>4}{:>7} | {:>14}{:>7}{:>7}{:>7} | {:>8}   {}",
            s.total.len(),
            s.early,
            q[0],
            q[1],
            q[2],
            q[3],
            total[2],
            summary(&s.codes, &s.errors)
        );
    }
    println!("(reqwest has no separate ttfb; its row shows total round trip)");

    if flag("--holds") {
        holds(&client, &conn, &orders_uri, &probe, &order).await?;
    }
    if flag("--ladder") {
        ladder(&client, &conn, &orders_uri, &probe, &order, rounds.min(10)).await?;
    }
    Ok(())
}

/// Arm streams held 1–60 s, all at once, and fire each at its time.
async fn holds(
    client: &Client,
    conn: &H2Conn,
    uri: &http::Uri,
    probe: &OrderTemplate,
    order: &LimitOrder,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nhold test (presend-lb, all armed at t=0):");
    let secs = [1u64, 5, 15, 30, 60];
    let mut armed = Vec::new();
    for _ in secs {
        let body = client.prepare(probe, order)?.body().to_owned();
        armed.push(conn.arm(uri, body.as_bytes(), Hold::LastByte, true).await?);
    }
    let start = Instant::now();
    for (sec, mut a) in secs.into_iter().zip(armed) {
        let wait = Duration::from_secs(sec).saturating_sub(start.elapsed());
        match tokio::time::timeout(wait, a.response_mut()).await {
            Ok(r) => {
                let msg = match r {
                    Ok(resp) => describe(&presend::read_body(resp).await?),
                    Err(e) => format!("stream error {e}"),
                };
                println!("  {sec:>3}s  ended EARLY by server: {msg}");
                continue;
            }
            Err(_) => {}
        }
        let t = Instant::now();
        let out = match a.fire() {
            Ok(fut) => match fut.await {
                Ok(resp) => {
                    let ttfb = t.elapsed();
                    let r = presend::read_body(resp).await?;
                    check_accepted(client, &r).await?;
                    format!("ttfb {:.2} ms  {}", ms(ttfb), describe(&r))
                }
                Err(e) => format!("stream error {e}"),
            },
            Err(e) => format!("send error {e}"),
        };
        println!("  {sec:>3}s  {out}");
    }
    Ok(())
}

/// Arm a ±5-tick ladder (22 streams), fire one, cancel the rest.
async fn ladder(
    client: &Client,
    conn: &H2Conn,
    uri: &http::Uri,
    probe: &OrderTemplate,
    order: &LimitOrder,
    rounds: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\narmed-ladder test (22 streams armed, 1 fired, 21 cancelled):");
    let mut ttfb = Vec::new();
    let mut s = Stats::default();
    for _ in 0..rounds {
        let mut armed = Vec::with_capacity(22);
        for _ in 0..22 {
            let body = client.prepare(probe, order)?.body().to_owned();
            armed.push(conn.arm(uri, body.as_bytes(), Hold::LastByte, true).await?);
        }
        tokio::time::sleep(HOLD).await;
        let fire = armed.swap_remove(7);
        let t = Instant::now();
        let fut = fire.fire()?;
        for a in armed {
            a.cancel();
        }
        finish(client, &mut s, t, fut).await?;
        ttfb.extend(s.ttfb.drain(..));
        tokio::time::sleep(Duration::from_secs(2)).await; // 22 streams per round: be gentle
    }
    let q = quantiles(&mut ttfb);
    println!("  ttfb ms min {} p25 {} p50 {} p90 {}   {}", q[0], q[1], q[2], q[3], summary(&s.codes, &s.errors));
    // A follow-up plain request shows whether the cancelled streams tripped the rate limit.
    let body = client.prepare(probe, order)?.body().to_owned();
    let r = presend::read(conn.post(uri, Bytes::from(body)).await?).await?;
    println!("  follow-up request after ladder: {}", describe(&r));
    Ok(())
}

/// Hold an armed stream for `hold`, unless the server answers first (recorded as early).
async fn wait_or_early(
    client: &Client,
    s: &mut Stats,
    mut armed: Armed,
    hold: Duration,
) -> Result<Option<Armed>, Box<dyn std::error::Error>> {
    match tokio::time::timeout(hold, armed.response_mut()).await {
        Err(_) => Ok(Some(armed)),
        Ok(r) => {
            s.early += 1;
            match r {
                Ok(resp) => {
                    let r = presend::read_body(resp).await?;
                    check_accepted(client, &r).await?;
                    s.codes.push(format!("EARLY:{}", code_of(&r)));
                }
                Err(e) => s.errors.push(format!("EARLY:{e}")),
            }
            Ok(None)
        }
    }
}

async fn finish(
    client: &Client,
    s: &mut Stats,
    t: Instant,
    fut: h2::client::ResponseFuture,
) -> Result<(), Box<dyn std::error::Error>> {
    match fut.await {
        Ok(resp) => {
            s.ttfb.push(t.elapsed());
            let r = presend::read_body(resp).await?;
            s.total.push(t.elapsed());
            check_accepted(client, &r).await?;
            s.codes.push(code_of(&r));
        }
        Err(e) => s.errors.push(e.to_string()),
    }
    Ok(())
}

async fn check_accepted(client: &Client, r: &RawResponse) -> Result<(), Box<dyn std::error::Error>> {
    if (200..300).contains(&r.status) {
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
        if let Some(id) = v["data"]["orderId"].as_str() {
            abort_accepted(client, id).await?;
        }
        return Err(format!("probe ACCEPTED: {}", String::from_utf8_lossy(&r.body)).into());
    }
    Ok(())
}

async fn abort_accepted(client: &Client, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    client.cancel(&[id]).await?;
    Err(format!("probe {id} was ACCEPTED (cancelled) - aborting").into())
}

fn code_of(r: &RawResponse) -> String {
    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
    format!("{} {}", r.status, v["error"].as_str().unwrap_or("?"))
}

fn describe(r: &RawResponse) -> String {
    let limit: Vec<String> = r
        .headers
        .iter()
        .filter(|(k, _)| k.as_str().contains("ratelimit") || k.as_str() == "retry-after")
        .map(|(k, v)| format!("{k}={}", v.to_str().unwrap_or("")))
        .collect();
    format!("{} {}", code_of(r), limit.join(" "))
}

fn summary(codes: &[String], errors: &[String]) -> String {
    let mut seen: Vec<(String, usize)> = Vec::new();
    for c in codes.iter().chain(errors) {
        match seen.iter_mut().find(|(k, _)| k == c) {
            Some((_, n)) => *n += 1,
            None => seen.push((c.clone(), 1)),
        }
    }
    seen.iter().map(|(k, n)| format!("{k} x{n}")).collect::<Vec<_>>().join(", ")
}

fn quantiles(v: &mut [Duration]) -> [String; 4] {
    if v.is_empty() {
        return ["-".into(), "-".into(), "-".into(), "-".into()];
    }
    v.sort_unstable();
    let p = |q: f64| format!("{:.2}", ms(v[((v.len() - 1) as f64 * q) as usize]));
    [p(0.0), p(0.25), p(0.5), p(0.9)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}
