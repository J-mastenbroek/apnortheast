//! Is a crossing (taker) order held at the exchange after its `201`? TRADES REAL MONEY: N limit
//! BUYs of ~$1 at the live best ask (alternating YES / NO) on the BTC 5-minute market that is
//! trading right now; positions are left open, anything still OPEN after 2 s is cancelled.
//! Reads `.env`.
//!
//!   cargo run --release --example taker_hold -- [samples=10] [--cancel 0,50,100,150,200]
//!
//! `--cancel`: sample i also sends a cancel-by-hash for its own order at delays[i % len] ms after
//! the order POST was sent (not after the 201), recording the cancel response; the trade feed
//! then shows whether the order executed anyway.
//!
//! Per sample: send time on the server clock, `201` round trip, `removalLockedUntil`, and the
//! order's status polled concurrently every 10 ms to +200 ms (then sparser to +2 s), so the
//! OPEN → FILLED transition is pinned to ~10 ms. Afterwards the public trade feed
//! (`/v1/orders/matches`) is matched against our hashes (`executedAt` has 1 s resolution).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use predict_gateway::crypto::to_hex;
use predict_gateway::{Client, Config, LimitOrder, Market, OrderTemplate};
use serde_json::Value;

const API: &str = "https://api.predict.fun";
const MONTHS: [&str; 12] = [
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December",
];
/// ET = UTC−4 (EDT) until 2026-11-01.
const ET_OFFSET_MIN: i64 = -240;

type Res<T> = Result<T, Box<dyn std::error::Error>>;

fn now_ns() -> i128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i128
}

fn poll_offsets() -> Vec<u64> {
    let mut v: Vec<u64> = (0..=200).step_by(10).collect();
    v.extend([250, 300, 400, 600, 1000, 2000]);
    v
}

#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    key: String,
    bearer: String,
}

impl Api {
    async fn get(&self, path: &str) -> Res<Value> {
        let r = self.http.get(format!("{API}{path}")).header("x-api-key", &self.key).header("authorization", &self.bearer).send().await?;
        Ok(serde_json::from_slice(&r.bytes().await?)?)
    }

    async fn post(&self, path: &str, body: String) -> Res<(u16, Value)> {
        let r = self
            .http
            .post(format!("{API}{path}"))
            .header("x-api-key", &self.key)
            .header("authorization", &self.bearer)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = r.status().as_u16();
        Ok((status, serde_json::from_slice(&r.bytes().await?)?))
    }
}

struct Sample {
    leg: &'static str,
    price: f64,
    hash: String,
    srv_send: i128,
    rtt_ms: f64,
    status: u16,
    lock_ms: Option<f64>,
    last_open_ms: Option<f64>,
    first_filled_ms: Option<f64>,
    final_state: String,
    /// (delay, sent ms, answered ms, http status, summary)
    cancel: Option<(u64, f64, f64, u16, String)>,
}

#[tokio::main]
async fn main() -> Res<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let delays: Vec<u64> = match args.iter().position(|a| a == "--cancel") {
        Some(i) => args.get(i + 1).ok_or("--cancel needs a list")?.split(',').map(str::parse).collect::<Result<_, _>>()?,
        None => Vec::new(),
    };
    let n: usize = args.first().filter(|a| !a.starts_with("--")).map(|s| s.parse()).transpose()?.unwrap_or(10);
    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let api = Api {
        http: reqwest::Client::builder().http2_prior_knowledge().tcp_nodelay(true).build()?,
        key: std::env::var("PREDICT_API_KEY")?,
        bearer: client.bearer().ok_or("no jwt")?,
    };

    let market = live_market(&client).await?;
    let yes = client.template(&market, 0)?;
    let no = client.template(&market, 1)?;
    let tick = yes.tick();

    // Clock offset (server − local) from probe rejections: fee 0, post-only at the lowest tick.
    let mut pm = market.clone();
    pm.fee_rate_bps = 0;
    let probe = client.template(&pm, 0)?;
    let mut best: Option<(i128, i128)> = None;
    for _ in 0..8 {
        let o = client.prepare(&probe, &LimitOrder::buy(tick, 1.0 / tick).post_only())?;
        let (t0, i0) = (now_ns(), Instant::now());
        let (_, v) = api.post("/v1/orders", o.body().to_owned()).await?;
        let rtt = i0.elapsed().as_nanos() as i128;
        if let Some(ts) = v["timestamp"].as_str().and_then(rfc3339_ns) {
            if best.is_none_or(|(r, _)| rtt < r) {
                best = Some((rtt, ts - (t0 + rtt / 2)));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (sync_rtt, offset) = best.ok_or("no server timestamps")?;
    println!("clock: server − local {:+.2} ms (± {:.2} ms)\n", ms(offset), ms(sync_rtt / 2));

    let mut samples = Vec::new();
    for i in 0..n {
        let leg = if i % 2 == 0 { "YES" } else { "NO" };
        let cancel = (!delays.is_empty()).then(|| delays[i % delays.len()]);
        match sample(&client, &api, &market, if leg == "YES" { &yes } else { &no }, leg, tick, offset, cancel).await {
            Ok(s) => samples.push(s),
            Err(e) => println!("sample {} ({leg}) skipped: {e}", i + 1),
        }
        tokio::time::sleep(Duration::from_secs(if delays.is_empty() { 4 } else { 6 })).await;
    }

    // Public trade feed: our hashes as taker (or maker).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let oldest = samples.iter().map(|s| s.srv_send).min().unwrap_or(0) - 2_000_000_000;
    let mut trades: Vec<Value> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..40 {
        let cursor = after.as_deref().map(|c| format!("&after={c}")).unwrap_or_default();
        let page = api.get(&format!("/v1/orders/matches?marketId={}&first=100{cursor}", market.id)).await?;
        let data = page["data"].as_array().cloned().unwrap_or_default();
        let done = data.last().and_then(|t| t["executedAt"].as_str().and_then(rfc3339_ns)).is_none_or(|t| t < oldest);
        trades.extend(data);
        after = page["cursor"].as_str().map(|c| c.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D"));
        if done || after.is_none() {
            break;
        }
    }

    println!("\n=== summary (ms from our send, server clock)");
    println!(
        "{:<4}{:<5}{:>6}{:>9}{:>11}{:>8}{:>12}  {:<34}EXECUTED? (public feed) / status",
        "#", "leg", "price", "201 rtt", "lock−send", "cancel", "cancel sent", "cancel answer"
    );
    for (i, s) in samples.iter().enumerate() {
        let trade = trades.iter().find(|t| t["taker"]["hash"] == s.hash.as_str() || t["makers"].as_array().is_some_and(|m| m.iter().any(|m| m["hash"] == s.hash.as_str())));
        let feed = trade
            .map(|t| {
                let amt = t["taker"]["amount"].as_str().and_then(|a| a.parse::<f64>().ok()).unwrap_or(0.0) / 1e18;
                format!("YES {amt:.2} sh, executedAt−send {:+.0} ms (1 s res.)", t["executedAt"].as_str().and_then(rfc3339_ns).map_or(f64::NAN, |x| ms(x - s.srv_send)))
            })
            .unwrap_or_else(|| "NO".into());
        let (cd, cs, ca) = match &s.cancel {
            Some((d, sent, _, st, sum)) => (format!("{d}"), format!("{sent:.1}"), format!("{st} {sum}")),
            None => ("-".into(), "-".into(), "-".into()),
        };
        println!(
            "{:<4}{:<5}{:>6.2}{:>9.2}{:>11}{:>8}{:>12}  {:<34}{feed} / {}",
            i + 1,
            s.leg,
            s.price,
            s.rtt_ms,
            s.lock_ms.map_or("-".into(), |v| format!("{v:.1}")),
            cd,
            cs,
            ca,
            s.final_state
        );
        let _ = (s.status, s.last_open_ms, s.first_filled_ms);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn sample(
    client: &Client,
    api: &Api,
    market: &Market,
    t: &OrderTemplate,
    leg: &'static str,
    tick: f64,
    offset: i128,
    cancel_after: Option<u64>,
) -> Res<Sample> {
    let book = client.orderbook(market.id).await?;
    let (price, avail) = if leg == "YES" {
        book.asks.iter().map(|a| (a[0], a[1])).fold((f64::INFINITY, 0.0), |b, a| if a.0 < b.0 { a } else { b })
    } else {
        let (bid, sz) = book.bids.iter().map(|b| (b[0], b[1])).fold((0.0, 0.0), |b, a| if a.0 > b.0 { a } else { b });
        (1.0 - bid, sz)
    };
    let price = (price / tick).round() * tick;
    let size = ((1.0 / price) * 100.0).ceil() / 100.0;
    if !(0.05..=0.95).contains(&price) || avail < size {
        return Err(format!("{leg} ask {price} ({avail} shares) out of range or too thin").into());
    }
    let order = client.prepare(t, &LimitOrder::buy(price, size))?;
    let hash = to_hex(&order.hash);
    let body = order.body().to_owned();

    let (t_send, i_send) = (now_ns(), Instant::now());
    // Status polls fire concurrently at fixed offsets from the send, independent of the POST.
    let polls: Vec<_> = poll_offsets()
        .into_iter()
        .map(|off| {
            let (api, path) = (api.clone(), format!("/v1/orders/{hash}"));
            tokio::spawn(async move {
                tokio::time::sleep_until((i_send + Duration::from_millis(off)).into()).await;
                let sent = i_send.elapsed();
                let v = api.get(&path).await.ok();
                (sent, i_send.elapsed(), v)
            })
        })
        .collect();
    // The deliberate cancel of this very order, at a fixed delay after the send.
    let cancel_task = cancel_after.map(|d| {
        let (api, body) = (api.clone(), format!(r#"{{"data":{{"hashes":["{hash}"]}}}}"#));
        tokio::spawn(async move {
            tokio::time::sleep_until((i_send + Duration::from_millis(d)).into()).await;
            let sent = ms(i_send.elapsed().as_nanos() as i128);
            let r = api.post("/v1/orders/remove-by-hash", body).await;
            (d, sent, ms(i_send.elapsed().as_nanos() as i128), r.map_err(|e| e.to_string()))
        })
    });
    let (status, placed) = api.post("/v1/orders", body).await?;
    let rtt_ms = ms(i_send.elapsed().as_nanos() as i128);
    let srv_send = t_send + offset;
    let lock_ms = placed["data"]["removalLockedUntil"].as_str().and_then(rfc3339_ns).map(|l| ms(l - srv_send));

    println!("{leg} BUY {size} @ {price:.2}  {hash}");
    println!("  POST → {status} in {rtt_ms:.2} ms: {}", compact(&placed));
    let cancel = match cancel_task {
        Some(t) => {
            let (d, sent, got, r) = t.await?;
            let (st, sum) = match r {
                Ok((st, v)) if v["removed"].is_array() || v["noop"].is_array() => {
                    let n = |k: &str| v[k].as_array().map_or(0, Vec::len);
                    (st, format!("removed {} noop {}", n("removed"), n("noop")))
                }
                Ok((st, v)) => (st, format!("{} {}", v["error"].as_str().unwrap_or("?"), v["message"].as_str().unwrap_or(""))),
                Err(e) => (0, e),
            };
            println!("  CANCEL @{d} ms: sent +{sent:.1}, answered +{got:.1}: {st} {sum}");
            Some((d, sent, got, st, sum))
        }
        None => None,
    };
    let (mut last_open, mut first_filled, mut prev) = (None, None, String::new());
    for p in polls {
        let (sent, got, v) = p.await?;
        let d = v.as_ref().map(|v| &v["data"]);
        let state = match d {
            Some(d) if d.is_object() => format!("{} {}/{}", d["status"].as_str().unwrap_or("?"), short(&d["amountFilled"]), short(&d["amount"])),
            _ => format!("({})", v.as_ref().and_then(|v| v["error"].as_str()).unwrap_or("no data")),
        };
        let (sent_ms, got_ms) = (ms(sent.as_nanos() as i128), ms(got.as_nanos() as i128));
        if state.starts_with("OPEN") {
            last_open = Some(sent_ms);
        }
        if state.starts_with("FILLED") && first_filled.is_none() {
            first_filled = Some(sent_ms);
        }
        if state != prev {
            println!("  poll sent +{sent_ms:>7.1} (answered +{got_ms:>7.1}): {state}");
            prev = state;
        }
    }
    let mut final_state = prev.clone();
    if prev.starts_with("OPEN") {
        let mut h = [0u8; 32];
        for (i, b) in h.iter_mut().enumerate() {
            *b = u8::from_str_radix(&hash[2 + 2 * i..4 + 2 * i], 16)?;
        }
        let r = client.cancel_by_hash(&[h]).await?;
        let d = api.get(&format!("/v1/orders/{hash}")).await?;
        final_state = format!("cancelled ({:?}) → {} {}", r.removed.len(), d["data"]["status"], short(&d["data"]["amountFilled"]));
        println!("  still OPEN at +2 s → {final_state}");
    }
    Ok(Sample { leg, price, hash, srv_send, rtt_ms, status, lock_ms, last_open_ms: last_open, first_filled_ms: first_filled, final_state, cancel })
}

/// The BTC 5-minute market trading right now (today's ET date, window containing now) with
/// ≥110 s left; otherwise the next window, after waiting for it to open.
async fn live_market(client: &Client) -> Res<Market> {
    let all: Vec<Market> = client.open_markets().await?.into_iter().filter(|m| m.trading_status == "OPEN" && m.is_btc_5m()).collect();
    loop {
        let et = now_ns() / 1_000_000_000 + ET_OFFSET_MIN as i128 * 60;
        let (days, sod) = (et.div_euclid(86_400) as i64, et.rem_euclid(86_400) as i64);
        let (_, mo, d) = civil_from_days(days);
        let date = format!("{} {d},", MONTHS[(mo - 1) as usize]);
        let now_min = sod / 60;
        let left_s = |end: i64| (end * 60 - sod).rem_euclid(86_400);
        let mut best: Option<(&Market, i64)> = None; // (market, seconds until its end)
        for m in &all {
            let Some((start, end)) = window(&m.title) else { continue };
            if !m.title.contains(&date) {
                continue;
            }
            let live = (now_min - start).rem_euclid(1440) < 5;
            let next = (start - now_min).rem_euclid(1440) <= 5 && !live;
            if live && left_s(end) >= 110 {
                println!("market {} '{}' ({} s left)", m.id, m.title, left_s(end));
                return Ok(m.clone());
            }
            if next && best.is_none_or(|(_, l)| left_s(end) < l) {
                best = Some((m, left_s(end)));
            }
        }
        let Some((m, l)) = best else { return Err(format!("no BTC 5-min market for {date} around now").into()) };
        let wait = (l - 300 + 2).max(1) as u64;
        println!("live window has <110 s left; waiting {wait} s for '{}'", m.title);
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }
}

/// `(start, end)` minutes past midnight from "... 2:35PM-2:40PM ET".
fn window(title: &str) -> Option<(i64, i64)> {
    let t = title.to_lowercase();
    t.split_whitespace().find_map(|tok| {
        let (a, b) = tok.split_once('-')?;
        Some((clock(a)?, clock(b)?))
    })
}

fn clock(s: &str) -> Option<i64> {
    let (rest, pm) = match (s.strip_suffix("pm"), s.strip_suffix("am")) {
        (Some(r), _) => (r, true),
        (_, Some(r)) => (r, false),
        _ => return None,
    };
    let (h, m) = match rest.split_once(':') {
        Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
        None => (rest.parse::<i64>().ok()?, 0),
    };
    Some((h % 12 + if pm { 12 } else { 0 }) * 60 + m)
}

fn compact(v: &Value) -> String {
    let d = &v["data"];
    if d.is_object() {
        format!("code {} removalLockedUntil {}", d["code"], d["removalLockedUntil"])
    } else {
        format!("{} {}", v["error"], v["message"])
    }
}

fn short(v: &Value) -> String {
    v.as_str().and_then(|s| s.parse::<f64>().ok()).map_or("?".into(), |x| format!("{:.2}", x / 1e18))
}

fn ms(ns: i128) -> f64 {
    ns as f64 / 1e6
}

/// `YYYY-MM-DDTHH:MM:SS[.f…](Z|±HH:MM)` → unix ns.
fn rfc3339_ns(s: &str) -> Option<i128> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d, h, mi, se) = (n(0..4)?, n(5..7)?, n(8..10)?, n(11..13)?, n(14..16)?, n(17..19)?);
    let mut i = 19;
    let mut frac: i128 = 0;
    if b.get(i) == Some(&b'.') {
        let start = i + 1;
        i = start;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        frac = format!("{:0<9}", &s[start..i])[..9].parse().ok()?;
    }
    let off_min: i64 = match b.get(i)? {
        b'Z' | b'z' => 0,
        c @ (b'+' | b'-') => (if *c == b'+' { 1 } else { -1 }) * (n(i + 1..i + 3)? * 60 + n(i + 4..i + 6)?),
        _ => return None,
    };
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + se - off_min * 60;
    Some(secs as i128 * 1_000_000_000 + frac)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}
