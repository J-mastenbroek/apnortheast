//! What happens to a crossing order inside the taker delay (~165 ms `removalLockedUntil`) when
//! the client disconnects, or sends the same order again? REAL MONEY: each sample is a ~$1 limit
//! BUY at the live best ask (alternating YES / NO) on the BTC 5-minute market trading now, sent
//! on its own fresh raw HTTP/2 connection. Positions are left open; anything still OPEN at +2 s is
//! cancelled. Reads `.env`.
//!
//!   cargo run --release --example play -- disconnect [samples=6] [--at 2,10,20,50,100]
//!   cargo run --release --example play -- dup        [samples=6] [--at 50,100,250]
//!
//! `disconnect`: the whole order is sent, then at +d ms after the send the TCP connection is
//! closed with no `RST_STREAM`, `GOAWAY` or TLS `close_notify` ([`H2Conn::kill`]). The `201`
//! arrives at ~30–40 ms, so small d disconnect before the order is acknowledged, larger d inside
//! the lock. Does the order still execute?
//!
//! `dup`: at +d ms the identical signed body (same order hash) is POSTed again on a second stream
//! of the same connection. What does the second POST get, does it carry its own lock, and does
//! the order execute once, twice or not at all?
//!
//!   cargo run --release --example play -- collateral [samples=3] [--at 10,50,100] [--notional 500]
//!
//! `collateral`: order A is the usual crossing ~$1 BUY and goes into the lock queue. At +d ms,
//! order B is a NON-crossing, post-only BUY resting at ~1¢ (far below the ask, so it can never
//! fill) sized so its reserved collateral (price·size ≈ `--notional`, default $500) is more than
//! the wallet holds. Does the exchange accept an order that reserves more collateral than is
//! available, and does over-committing it starve or otherwise change A while A is in the lock?
//! B rests at 1¢ so nothing can fill it; both orders are cancelled afterwards.
//!
//!   cargo run --release --example play -- lockmap [rounds=2]
//!
//! `lockmap`: per round, fires one of each order variant on the live window and reports which
//! carry the removal lock and which can be cancelled at once — a crossing BUY at the ask (taker),
//! a post-only BUY joining the best bid (maker), and a post-only BUY improving the bid (maker).
//! Resting variants are cancelled immediately; the crossing one is polled to +2 s. This maps the
//! boundary; it is not a way to cancel a matched taker order (the lock prevents that by design).
//!
//! Sample i uses delay `at[i % len]`. Per sample: every response with its round trip and
//! `removalLockedUntil − send` (server clock, offset from probe rejections), status at +2 s, and
//! afterwards the public trade feed (`/v1/orders/matches`) for our hashes.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use h2::client::ResponseFuture;
use predict_gateway::crypto::to_hex;
use predict_gateway::presend::{read_body, H2Conn};
use predict_gateway::{Client, Config, LimitOrder, Market, OrderTemplate};
use serde_json::Value;

const API: &str = "https://api.predict.fun";

type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Disconnect,
    Dup,
    Collateral,
    LockMap,
}

/// One HTTP response: ms from the order send, status, `removalLockedUntil − send` on the server
/// clock, and a one-line summary.
struct Reply {
    at_ms: f64,
    status: u16,
    lock_ms: Option<f64>,
    text: String,
}

struct Sample {
    leg: &'static str,
    price: f64,
    hash: String,
    srv_send: i128,
    delay: u64,
    action_ms: f64,
    first: Result<Reply, String>,
    second: Option<Result<Reply, String>>,
    state: String,
}

#[tokio::main]
async fn main() -> Res<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = match args.first().map(String::as_str) {
        Some("disconnect") => Mode::Disconnect,
        Some("dup") => Mode::Dup,
        Some("collateral") => Mode::Collateral,
        Some("lockmap") => Mode::LockMap,
        _ => return Err("usage: play disconnect|dup|collateral|lockmap [samples] [--at ms,ms,...] [--notional $]".into()),
    };
    let delays: Vec<u64> = match args.iter().position(|a| a == "--at") {
        Some(i) => args.get(i + 1).ok_or("--at needs a list")?.split(',').map(str::parse).collect::<Result<_, _>>()?,
        None if mode == Mode::Disconnect => vec![2, 10, 20, 50, 100],
        None => vec![10, 50, 100],
    };
    let notional: f64 = match args.iter().position(|a| a == "--notional") {
        Some(i) => args.get(i + 1).ok_or("--notional needs a value")?.parse()?,
        None => 500.0,
    };
    let default_n = match mode {
        Mode::Collateral => 3,
        Mode::LockMap => 2,
        _ => 6,
    };
    let n: usize = args.get(1).filter(|a| !a.starts_with("--")).map(|s| s.parse()).transpose()?.unwrap_or(default_n);

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let market = live_market(&client, 4 * n as u32 + 20).await?;
    let yes = client.template(&market, 0)?;
    let no = client.template(&market, 1)?;
    let tick = yes.tick();

    // Clock offset (server − local) from probe rejections: fee 0, post-only at the lowest tick.
    let mut pm = market.clone();
    pm.fee_rate_bps = 0;
    let probe = client.template(&pm, 0)?;
    let conn = fresh(&client).await?;
    let uri = conn.uri("/v1/orders")?;
    let mut best: Option<(i128, i128)> = None;
    for _ in 0..8 {
        let o = client.prepare(&probe, &LimitOrder::buy(tick, 1.0 / tick).post_only())?;
        let (t0, i0) = (now_ns(), Instant::now());
        let r = read_body(conn.post(&uri, Bytes::from(o.body().to_owned())).await?.await?).await?;
        let rtt = i0.elapsed().as_nanos() as i128;
        let v: Value = serde_json::from_slice(&r.body)?;
        if let Some(ts) = v["timestamp"].as_str().and_then(rfc3339_ns) {
            if best.is_none_or(|(r, _)| rtt < r) {
                best = Some((rtt, ts - (t0 + rtt / 2)));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (sync_rtt, offset) = best.ok_or("no server timestamps")?;
    println!("clock: server − local {:+.2} ms (± {:.2} ms)\n", ms(offset), ms(sync_rtt / 2));

    if mode == Mode::Collateral {
        println!("order B: non-crossing post-only BUY resting at ~1¢, ~${notional:.0} collateral (must exceed available)\n");
        let mut cs = Vec::new();
        for i in 0..n {
            let leg = if i % 2 == 0 { "YES" } else { "NO" };
            let t = if leg == "YES" { &yes } else { &no };
            match collateral_sample(&client, &market, t, leg, tick, offset, delays[i % delays.len()], notional).await {
                Ok(s) => cs.push(s),
                Err(e) => println!("sample {} ({leg}) skipped: {e}", i + 1),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let trades = feed(&client, market.id, cs.iter().map(|s| s.a_srv_send).min().unwrap_or(0) - 2_000_000_000).await?;
        let exec = |hash: &str| {
            let mine: Vec<&Value> = trades.iter().filter(|t| t["taker"]["hash"] == hash).collect();
            let sh: f64 = mine.iter().filter_map(|t| t["taker"]["amount"].as_str()?.parse::<f64>().ok()).sum::<f64>() / 1e18;
            if mine.is_empty() { "not executed".to_owned() } else { format!("executed ×{} ({sh:.2} sh)", mine.len()) }
        };
        println!("\n=== summary (A = crossing $1; B = ~${notional:.0} resting at 1¢, more collateral than held)");
        for (i, s) in cs.iter().enumerate() {
            println!("#{} {} | B sent +{:.1} ms (delay {})", i + 1, s.leg, s.action_ms, s.delay);
            println!("    A crossing {:.2}: {} | +2 s {} | feed: {}", s.a_price, show(&s.a_resp), s.a_state, exec(&s.a_hash));
            println!("    B 1¢ ${:.0}: {} | +2 s {} | feed: {}", s.b_req, show(&s.b_resp), s.b_state, exec(&s.b_hash));
        }
        return Ok(());
    }

    if mode == Mode::LockMap {
        // Map which orders carry the removal lock and which can be cancelled at once. For every
        // variant: send, read the 201 + removalLockedUntil, then for a resting (maker) variant
        // immediately cancel-by-hash and report whether it was removed; for the crossing variant
        // poll to +2 s (it cannot be cancelled in the window — see Run 3). Checked against the feed.
        let mut rows: Vec<(String, String, i128, Result<Reply, String>, String, String)> = Vec::new();
        for round in 0..n {
            let book = client.orderbook(market.id).await?;
            let ask = book.asks.iter().map(|a| a[0]).fold(f64::INFINITY, f64::min);
            let bid = book.bids.iter().map(|b| b[0]).fold(0.0, f64::max);
            if !ask.is_finite() || bid <= 0.0 {
                println!("round {}: book one-sided (ask {ask} bid {bid}); skipping", round + 1);
                continue;
            }
            let r = |p: f64| (p / tick).round() * tick;
            // (name, price, post_only, crossing). Resting prices stay strictly below the ask.
            let improve = r(bid + tick);
            let mut variants = vec![
                ("cross_buy@ask", r(ask), false, true),
                ("rest_join_bid", r(bid), true, false),
            ];
            if improve < r(ask) {
                variants.push(("rest_improve_bid", improve, true, false));
            }
            for (name, price, post_only, crossing) in variants {
                if !(0.02..=0.98).contains(&price) {
                    println!("{name}: price {price} out of range; skip");
                    continue;
                }
                let size = ((1.0 / price) * 100.0).ceil() / 100.0;
                let mut o = LimitOrder::buy(price, size);
                if post_only {
                    o = o.post_only();
                }
                let order = client.prepare(&yes, &o)?;
                let hash = to_hex(&order.hash);
                let conn = fresh(&client).await?;
                let uri = conn.uri("/v1/orders")?;
                let (t_send, i_send) = (now_ns(), Instant::now());
                let srv_send = t_send + offset;
                let resp = reply(conn.post(&uri, Bytes::from(order.body().to_owned())).await?, i_send, srv_send).await;
                let accepted = matches!(&resp, Ok(r) if r.status == 201);
                let cancel = if accepted && !crossing {
                    // Resting order: try to remove it immediately — there is no window to beat.
                    match client.cancel_by_hash(&[order.hash]).await {
                        Ok(r) => format!("immediate cancel → removed {} noop {}", r.removed.len(), r.noop.len()),
                        Err(e) => format!("cancel error {e}"),
                    }
                } else {
                    "-".to_owned()
                };
                let state = if accepted {
                    if crossing {
                        tokio::time::sleep_until((i_send + Duration::from_secs(2)).into()).await;
                        settle(&client, &hash, order.hash).await
                    } else {
                        client.order(&hash).await.map(|o| format!("{} {}/{}", o.status, sh(&o.amount_filled), sh(&o.amount))).unwrap_or_else(|e| format!("({e})"))
                    }
                } else {
                    "-".to_owned()
                };
                println!("{name} @{price:.2}: {} | {cancel} | +state {state}", show(&resp));
                rows.push((name.to_owned(), hash, srv_send, resp, cancel, state));
                tokio::time::sleep(Duration::from_millis(800)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let trades = feed(&client, market.id, rows.iter().map(|r| r.2).min().unwrap_or(0) - 2_000_000_000).await?;
        println!("\n=== lock map (which orders are locked, which cancel at once)");
        for (name, hash, _, resp, cancel, state) in &rows {
            let mine: Vec<&Value> = trades.iter().filter(|t| t["taker"]["hash"] == hash.as_str()).collect();
            let feed = if mine.is_empty() { "not executed".to_owned() } else { format!("executed ×{}", mine.len()) };
            let lock = match resp {
                Ok(r) => r.lock_ms.map_or("null".to_owned(), |l| format!("{l:.0} ms")),
                Err(_) => "-".to_owned(),
            };
            println!("{name:<17} lock {lock:<8} | {cancel:<34} | +2 s {state} | feed {feed}");
        }
        return Ok(());
    }

    let mut samples = Vec::new();
    for i in 0..n {
        let leg = if i % 2 == 0 { "YES" } else { "NO" };
        let t = if leg == "YES" { &yes } else { &no };
        match sample(&client, &market, t, leg, tick, offset, mode, delays[i % delays.len()]).await {
            Ok(s) => samples.push(s),
            Err(e) => println!("sample {} ({leg}) skipped: {e}", i + 1),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let trades = feed(&client, market.id, samples.iter().map(|s| s.srv_send).min().unwrap_or(0) - 2_000_000_000).await?;

    let action = if mode == Mode::Disconnect { "kill" } else { "resend" };
    println!("\n=== summary (ms from the order send; lock = removalLockedUntil − send, server clock)");
    for (i, s) in samples.iter().enumerate() {
        let mine: Vec<&Value> = trades.iter().filter(|t| t["taker"]["hash"] == s.hash.as_str()).collect();
        let shares: f64 = mine.iter().filter_map(|t| t["taker"]["amount"].as_str()?.parse::<f64>().ok()).sum::<f64>() / 1e18;
        let feed = if mine.is_empty() { "not executed".to_owned() } else { format!("executed ×{} ({shares:.2} sh)", mine.len()) };
        println!("#{} {} {:.2}  {action} @{} (sent +{:.1})", i + 1, s.leg, s.price, s.delay, s.action_ms);
        println!("    1st POST: {}", show(&s.first));
        if let Some(r) = &s.second {
            println!("    2nd POST: {}", show(r));
        }
        println!("    status +2 s: {} | public feed: {feed}", s.state);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn sample(client: &Client, market: &Market, t: &OrderTemplate, leg: &'static str, tick: f64, offset: i128, mode: Mode, delay: u64) -> Res<Sample> {
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
    let body = Bytes::from(order.body().to_owned());
    let conn = fresh(client).await?;
    let uri = conn.uri("/v1/orders")?;
    println!("{leg} BUY {size} @ {price:.2}  {hash}");

    let (t_send, i_send) = (now_ns(), Instant::now());
    let srv_send = t_send + offset;
    let first = tokio::spawn(reply(conn.post(&uri, body.clone()).await?, i_send, srv_send));
    tokio::time::sleep_until((i_send + Duration::from_millis(delay)).into()).await;
    let action_ms = ms(i_send.elapsed().as_nanos() as i128);
    let second = match mode {
        Mode::Disconnect => {
            conn.kill();
            None
        }
        Mode::Dup => Some(tokio::spawn(reply(conn.post(&uri, body).await?, i_send, srv_send))),
        Mode::Collateral | Mode::LockMap => unreachable!("handled on their own path"),
    };
    let first = first.await?;
    let second = match second {
        Some(h) => Some(h.await?),
        None => None,
    };
    println!("  1st POST: {}", show(&first));
    println!("  {} @{delay} ms (+{action_ms:.1})", if mode == Mode::Disconnect { "kill" } else { "resend" });
    if let Some(r) = &second {
        println!("  2nd POST: {}", show(r));
    }

    tokio::time::sleep_until((i_send + Duration::from_secs(2)).into()).await;
    let mut state = match client.order(&hash).await {
        Ok(o) => format!("{} {}/{}", o.status, sh(&o.amount_filled), sh(&o.amount)),
        Err(e) => format!("({e})"),
    };
    if state.starts_with("OPEN") {
        let r = client.cancel_by_hash(&[order.hash]).await?;
        let after = client.order(&hash).await.map(|o| format!("{} {}", o.status, sh(&o.amount_filled))).unwrap_or_else(|e| e.to_string());
        state = format!("{state} → cancel (removed {}) → {after}", r.removed.len());
    }
    println!("  status +2 s: {state}");
    Ok(Sample { leg, price, hash, srv_send, delay, action_ms, first, second, state })
}

struct CollSample {
    leg: &'static str,
    delay: u64,
    action_ms: f64,
    a_price: f64,
    a_hash: String,
    a_srv_send: i128,
    a_resp: Result<Reply, String>,
    a_state: String,
    b_req: f64,
    b_hash: String,
    b_resp: Result<Reply, String>,
    b_state: String,
}

/// A = crossing ~$1 BUY at the ask (goes into the lock queue). B = post-only BUY resting at ~1¢,
/// sized so `price·size ≈ notional` dollars of collateral — more than the wallet holds — sent
/// `delay` ms after A, on a second stream of the same connection. B rests at 1¢ so it cannot fill.
/// Both are cancelled afterwards.
#[allow(clippy::too_many_arguments)]
async fn collateral_sample(client: &Client, market: &Market, t: &OrderTemplate, leg: &'static str, tick: f64, offset: i128, delay: u64, notional: f64) -> Res<CollSample> {
    let book = client.orderbook(market.id).await?;
    let (a_px, avail) = if leg == "YES" {
        book.asks.iter().map(|a| (a[0], a[1])).fold((f64::INFINITY, 0.0), |b, a| if a.0 < b.0 { a } else { b })
    } else {
        let (bid, sz) = book.bids.iter().map(|b| (b[0], b[1])).fold((0.0, 0.0), |b, a| if a.0 > b.0 { a } else { b });
        (1.0 - bid, sz)
    };
    let a_price = (a_px / tick).round() * tick;
    let a_size = ((1.0 / a_price) * 100.0).ceil() / 100.0;
    if !(0.05..=0.95).contains(&a_price) || avail < a_size {
        return Err(format!("{leg} ask {a_price} ({avail} shares) out of range or too thin").into());
    }
    // B rests at ~1¢ (one tick, or 0.01 if the tick is finer), far below the ask, so it never crosses.
    let b_price = (0.01 / tick).round().max(1.0) * tick;
    if b_price >= a_price {
        return Err(format!("1¢ rest price {b_price} is not below the ask {a_price}; skipping to stay non-crossing").into());
    }
    let b_size = notional / b_price;
    let a = client.prepare(t, &LimitOrder::buy(a_price, a_size))?;
    let b = client.prepare(t, &LimitOrder::buy(b_price, b_size).post_only())?;
    let (a_hash, b_hash) = (to_hex(&a.hash), to_hex(&b.hash));
    let b_req = b_price * b_size; // dollars of collateral B asks the exchange to reserve
    let conn = fresh(client).await?;
    let uri = conn.uri("/v1/orders")?;
    println!("{leg}: A crossing BUY {a_size} @ {a_price:.2}  {a_hash}");
    println!("     B rest BUY {b_size:.0} @ {b_price:.2} (~${b_req:.0} collateral)  {b_hash}");

    let (t_send, i_send) = (now_ns(), Instant::now());
    let a_srv_send = t_send + offset;
    let a_fut = tokio::spawn(reply(conn.post(&uri, Bytes::from(a.body().to_owned())).await?, i_send, a_srv_send));
    tokio::time::sleep_until((i_send + Duration::from_millis(delay)).into()).await;
    let action_ms = ms(i_send.elapsed().as_nanos() as i128);
    let b_fut = tokio::spawn(reply(conn.post(&uri, Bytes::from(b.body().to_owned())).await?, i_send, a_srv_send));
    let a_resp = a_fut.await?;
    let b_resp = b_fut.await?;
    println!("  A POST: {}", show(&a_resp));
    println!("  B POST @{delay} ms (+{action_ms:.1}): {}", show(&b_resp));

    tokio::time::sleep_until((i_send + Duration::from_secs(2)).into()).await;
    let a_state = settle(client, &a_hash, a.hash).await;
    let b_state = settle(client, &b_hash, b.hash).await;
    println!("  A +2 s: {a_state}");
    println!("  B +2 s: {b_state}");
    Ok(CollSample { leg, delay, action_ms, a_price, a_hash, a_srv_send, a_resp, a_state, b_req, b_hash, b_resp, b_state })
}

/// Read an order's status; if it is still OPEN, cancel it by hash and report the result.
async fn settle(client: &Client, hash: &str, h: predict_gateway::crypto::B256) -> String {
    let mut state = match client.order(hash).await {
        Ok(o) => format!("{} {}/{}", o.status, sh(&o.amount_filled), sh(&o.amount)),
        Err(e) => format!("({e})"),
    };
    if state.starts_with("OPEN") {
        match client.cancel_by_hash(&[h]).await {
            Ok(r) => {
                let after = client.order(hash).await.map(|o| format!("{} {}", o.status, sh(&o.amount_filled))).unwrap_or_else(|e| e.to_string());
                state = format!("{state} → cancel (removed {}) → {after}", r.removed.len());
            }
            Err(e) => state = format!("{state} → cancel error {e}"),
        }
    }
    state
}

/// Await a response; errors (e.g. the connection was killed) and a 5 s timeout become `Err`.
async fn reply(f: ResponseFuture, i_send: Instant, srv_send: i128) -> Result<Reply, String> {
    let r = tokio::time::timeout(Duration::from_secs(5), async { read_body(f.await?).await })
        .await
        .map_err(|_| format!("no response after {:.0} ms", ms(i_send.elapsed().as_nanos() as i128)))?
        .map_err(|e| format!("{e} at +{:.1} ms", ms(i_send.elapsed().as_nanos() as i128)))?;
    let at_ms = ms(i_send.elapsed().as_nanos() as i128);
    let v: Value = serde_json::from_slice(&r.body).unwrap_or(Value::Null);
    let d = &v["data"];
    let lock_ms = d["removalLockedUntil"].as_str().and_then(rfc3339_ns).map(|l| ms(l - srv_send));
    let text = if d.is_object() {
        format!("code {} removalLockedUntil {}", d["code"], d["removalLockedUntil"])
    } else {
        format!("{} {}", v["error"], v["message"])
    };
    Ok(Reply { at_ms, status: r.status, lock_ms, text })
}

fn show(r: &Result<Reply, String>) -> String {
    match r {
        Ok(r) => format!("{} at +{:.1}, lock {}: {}", r.status, r.at_ms, r.lock_ms.map_or("-".into(), |l| format!("{l:.1}")), r.text),
        Err(e) => format!("no response: {e}"),
    }
}

async fn fresh(client: &Client) -> Res<H2Conn> {
    Ok(client.fanout(1).await?.into_conns().pop().ok_or("no connection")?)
}

/// The BTC 5-minute market trading now, if it has at least `need` s left; otherwise the next one.
async fn live_market(client: &Client, need: u32) -> Res<Market> {
    loop {
        let (m, left) = client.current_btc_5m().await?;
        if !m.is_btc_5m() {
            return Err("not a BTC 5-minute market".into());
        }
        if left >= need && m.trading_status == "OPEN" {
            println!("market {} '{}' ({left} s left)", m.id, m.title);
            return Ok(m);
        }
        println!("'{}' has {left} s left (< {need}); waiting for the next window", m.title);
        tokio::time::sleep(Duration::from_secs(left as u64 + 3)).await;
    }
}

/// Public trades on `market` back to `oldest` (unix ns).
async fn feed(client: &Client, market: u64, oldest: i128) -> Res<Vec<Value>> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let http = reqwest::Client::new();
    let bearer = client.bearer().ok_or("no jwt")?;
    let key = std::env::var("PREDICT_API_KEY")?;
    let mut trades: Vec<Value> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..40 {
        let cursor = after.as_deref().map(|c| format!("&after={c}")).unwrap_or_default();
        let r = http
            .get(format!("{API}/v1/orders/matches?marketId={market}&first=100{cursor}"))
            .header("x-api-key", &key)
            .header("authorization", &bearer)
            .send()
            .await?;
        let page: Value = serde_json::from_slice(&r.bytes().await?)?;
        let data = page["data"].as_array().cloned().unwrap_or_default();
        let done = data.last().and_then(|t| t["executedAt"].as_str().and_then(rfc3339_ns)).is_none_or(|t| t < oldest);
        trades.extend(data);
        after = page["cursor"].as_str().map(|c| c.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D"));
        if done || after.is_none() {
            break;
        }
    }
    Ok(trades)
}

fn now_ns() -> i128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i128
}

fn sh(wei: &str) -> String {
    wei.parse::<f64>().map_or("?".into(), |x| format!("{:.2}", x / 1e18))
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
