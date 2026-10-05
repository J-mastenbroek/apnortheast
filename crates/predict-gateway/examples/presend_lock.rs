//! Test whether pre-sending an incomplete crossing order consumes the removal-lock timer.
//! Running presend_lock submits real orders. --dry-run prepares without sending.
//! Up to 14 ~$1 YES buys; filled positions remain.
//! Run: py deploy.py --run "cargo run --release --example presend_lock"
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use predict_gateway::crypto::to_hex;
use predict_gateway::presend::read_body;
use predict_gateway::{Client, Config, LimitOrder};
use serde_json::{json, Value};

type Res<T> = Result<T, Box<dyn std::error::Error>>;
const AGES: [u64; 7] = [50, 100, 150, 250, 500, 1000, 4000];

fn now_ns() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i128
}

#[tokio::main]
async fn main() -> Res<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help") {
        println!("presend_lock [market-id] [--dry-run]\nDefault: up to 14 ~$1 crossing YES buys on the current BTC 5-minute market.\nPairs full-body controls with incomplete-body holds of {AGES:?} ms.\nFilled positions remain. --dry-run sends no orders.");
        return Ok(());
    }
    let live = !args.iter().any(|a| a == "--dry-run");
    let positional: Vec<&String> = args.iter().filter(|a| a.as_str() != "--dry-run").collect();
    if positional.len() > 1 {
        return Err("usage: presend_lock [market-id] [--dry-run]".into());
    }
    let requested_id: Option<u64> = positional.first().map(|s| s.parse()).transpose()?;
    dotenvy::dotenv().ok();
    let cfg = Config::from_env()?;
    let base = cfg.chain.api_url();
    let key = cfg.api_key.clone();
    let client = Client::new(cfg)?;
    client.warm().await?;
    client.login().await?;
    let market = match requested_id {
        Some(id) => client.market(id).await?,
        None => client.current_btc_5m().await?.0,
    };
    let id = market.id;
    println!("market {id}: {} | live={live}", market.title);
    if !market.is_btc_5m() || market.trading_status != "OPEN" {
        return Err("requires an open BTC 5-minute market".into());
    }
    let template = client.template(&market, 0)?;
    let mut fan = client.fanout(1).await?;
    let uri = fan.conns()[0].uri("/v1/orders")?;
    let http = reqwest::Client::new();
    let bearer = client.bearer().ok_or("login missing bearer")?;
    let mut hashes = Vec::new();
    let mut samples = Vec::new();
    let mut taker_fills = std::collections::HashSet::new();

    // Control/aged ordering alternates to reduce a simple time-of-window bias.
    for (pair, age) in AGES.into_iter().enumerate() {
        let order = if pair % 2 == 0 { [0, age] } else { [age, 0] };
        for hold_ms in order {
            if client.market(id).await?.trading_status != "OPEN" {
                return Err("market stopped trading; stopping experiment".into());
            }
            let book = client.orderbook(id).await?;
            let best = book
                .asks
                .iter()
                .min_by(|a, b| a[0].total_cmp(&b[0]))
                .ok_or("no YES ask")?;
            // Two ticks of headroom, capped at the highest valid market tick.
            // Refresh the book before releasing an aged request.
            let tick = template.tick();
            let max_ticks = (1.0 / tick).round() - 1.0;
            let ask_ticks = (best[0] / tick).round();
            if !best[0].is_finite()
                || !(1.0..=max_ticks).contains(&ask_ticks)
                || (best[0] / tick - ask_ticks).abs() > 1e-6
            {
                return Err(format!("invalid best ask {} for market tick {tick}", best[0]).into());
            }
            let price = (ask_ticks + 2.0).min(max_ticks) * tick;
            let size = (1.0 / price * 100.0).ceil() / 100.0;
            let signed = client.prepare(&template, &LimitOrder::buy(price, size))?;
            let hash = to_hex(&signed.hash);
            println!(
                "{}",
                json!({"event":"prepared", "pair":pair, "hold_ms":hold_ms,
                "hash":hash, "price":price, "size":size, "notional":price*size, "live":live})
            );
            if !live {
                continue;
            }
            fan.heal().await?;
            let mut held = if hold_ms > 0 {
                Some(fan.arm(&uri, signed.body().as_bytes()).await?)
            } else {
                None
            };
            let armed_at = Instant::now();
            if let Some(set) = held.as_mut() {
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                if set.prune_dead().await != 0 || set.is_empty() {
                    return Err(format!(
                        "held stream ended early for {hash}; outcome unknown; stopping"
                    )
                    .into());
                }
            }
            let fresh = match client.orderbook(id).await {
                Ok(book) => book,
                Err(e) => {
                    if let Some(set) = held {
                        set.cancel();
                    }
                    return Err(e.into());
                }
            };
            let depth: f64 = fresh
                .asks
                .iter()
                .filter(|a| a[0] <= price)
                .map(|a| a[1])
                .sum();
            if depth < size {
                if let Some(set) = held {
                    set.cancel();
                }
                println!(
                    "{}",
                    json!({"event":"skipped", "hash":hash, "reason":"no longer crossing sufficient displayed liquidity"})
                );
                continue;
            }
            let actual_hold_ms = if held.is_some() {
                armed_at.elapsed().as_secs_f64() * 1000.0
            } else {
                0.0
            };
            let fire_ns = now_ns();
            let fired_at = Instant::now();
            // Never send the complete body before the measured fire time.
            let response = match held {
                Some(set) => set.fire().pop().ok_or("empty armed set")??,
                None => {
                    fan.conns()[0]
                        .post(&uri, signed.body().as_bytes().to_vec().into())
                        .await?
                }
            };
            // On transport/decode failure stop: retrying could create additional exposure.
            let raw = tokio::time::timeout(Duration::from_secs(10), async {
                read_body(response.await?).await
            })
            .await??;
            let received_ns = now_ns();
            let v: Value = serde_json::from_slice(&raw.body)?;
            println!(
                "{}",
                json!({"event":"response", "pair":pair, "hold_ms":hold_ms,
                "actual_hold_ms":actual_hold_ms, "hash":hash, "fire_unix_ns":fire_ns.to_string(),
                "received_unix_ns":received_ns.to_string(), "fire_to_response_ms":fired_at.elapsed().as_secs_f64()*1000.0,
                "http":raw.status, "response":v})
            );
            if raw.status == 201 {
                let order_id = v["data"]["orderId"]
                    .as_str()
                    .ok_or("accepted response missing orderId")?;
                hashes.push(hash.clone());
                let lock = v["data"].get("removalLockedUntil");
                let lock_ms = lock
                    .and_then(Value::as_str)
                    .and_then(rfc3339_ns)
                    .map(|until| (until - fire_ns) as f64 / 1e6);
                let lock_state = match lock {
                    Some(Value::Null) => "null",
                    Some(Value::String(_)) if lock_ms.is_some() => "timestamp",
                    _ => "missing or unparseable",
                };
                samples.push((
                    pair,
                    hold_ms,
                    actual_hold_ms,
                    hash.clone(),
                    lock_ms,
                    lock_state,
                ));
                // A null lock is only a candidate. Displayed liquidity is not execution proof.
                println!(
                    "{}",
                    json!({"event":"candidate", "hash":hash,
                    "null_lock":v["data"].get("removalLockedUntil").is_some_and(Value::is_null)})
                );
                // Wait beyond the known removal lock, then remove unfilled remainder.
                tokio::time::sleep(Duration::from_secs(3)).await;
                let cleanup = client.cancel(&[order_id]).await?;
                println!(
                    "{}",
                    json!({"event":"cleanup", "hash":hash, "removed":cleanup.removed, "noop":cleanup.noop})
                );
                match client.order(&hash).await {
                    Ok(info) => println!(
                        "{}",
                        json!({"event":"status", "hash":hash,
                        "status":info.status, "filled":info.amount_filled, "amount":info.amount})
                    ),
                    Err(e) => println!(
                        "{}",
                        json!({"event":"status_error", "hash":hash, "error":e.to_string()})
                    ),
                }
            } else if raw.status >= 500 {
                return Err(
                    format!("server error for {hash}; acceptance uncertain; stopping").into(),
                );
            }
        }
    }
    if live {
        tokio::time::sleep(Duration::from_secs(2)).await;
        // Read up to 20 pages; absence is inconclusive if the history is incomplete or delayed.
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut req = http
                .get(format!("{base}/v1/orders/matches"))
                .bearer_auth(bearer.strip_prefix("Bearer ").unwrap_or(&bearer))
                .query(&[("marketId", id.to_string()), ("first", "100".to_owned())]);
            if let Some(k) = &key {
                req = req.header("x-api-key", k);
            }
            if let Some(c) = &cursor {
                req = req.query(&[("after", c)]);
            }
            let page: Value =
                serde_json::from_slice(&req.send().await?.error_for_status()?.bytes().await?)?;
            let trades = page["data"]
                .as_array()
                .ok_or("unexpected match feed response")?;
            for t in trades {
                for hash in &hashes {
                    let taker = t["taker"]["hash"] == hash.as_str();
                    let maker = t["makers"]
                        .as_array()
                        .is_some_and(|m| m.iter().any(|m| m["hash"] == hash.as_str()));
                    if taker {
                        taker_fills.insert(hash.clone());
                    }
                    if taker || maker {
                        println!(
                            "{}",
                            json!({"event":"execution_evidence", "hash":hash, "taker":taker, "trade":t})
                        );
                    }
                }
            }
            cursor = page["cursor"].as_str().map(str::to_owned);
            if cursor.is_none() || trades.is_empty() {
                break;
            }
        }
    }
    println!("\n=== paired result: aged minus full-send control ===");
    for (pair, age) in AGES.into_iter().enumerate() {
        let control = samples.iter().find(|s| s.0 == pair && s.1 == 0);
        let aged = samples.iter().find(|s| s.0 == pair && s.1 == age);
        match (control, aged) {
            (Some(c), Some(a)) => {
                let delta =
                    c.4.zip(a.4)
                        .map(|(c, a)| format!("{:+.2} ms", a - c))
                        .unwrap_or_else(|| "inconclusive".to_owned());
                println!(
                    "hold {:.1} ms | lock delta {delta} | aged lock {} | taker fill {}",
                    a.2,
                    a.5,
                    if taker_fills.contains(&a.3) {
                        "observed"
                    } else {
                        "not observed (inconclusive)"
                    }
                );
            }
            _ => println!("hold {age} ms: incomplete pair (rejected/skipped/not sent)"),
        }
    }
    println!("Flat deltas: no evidence pre-send consumes the lock. Negative deltas approaching the hold duration: candidate worth repeating.");
    println!("Null lock + taker fill: candidate only. One-second feed timestamps cannot prove sub-150ms matching. Paired differences assume a stable clock offset.");
    Ok(())
}

fn rfc3339_ns(s: &str) -> Option<i128> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d, h, mi, se) = (
        n(0..4)?,
        n(5..7)?,
        n(8..10)?,
        n(11..13)?,
        n(14..16)?,
        n(17..19)?,
    );
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
        c @ (b'+' | b'-') => {
            (if *c == b'+' { 1 } else { -1 }) * (n(i + 1..i + 3)? * 60 + n(i + 4..i + 6)?)
        }
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
