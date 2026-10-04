//! Exercise [`Hitter`] re-arm rotation live with *probe* orders (`feeRateBps = 0`, rejected by the
//! server, cannot trade) on a BTC 5-minute market. Reads `.env`.
//!
//!   cargo run --release --example hitter -- <btc_5m_market_id> [--secs 60]
//!
//! Phase A (40 s): rotation only, two targets armed on 6 connections, re-armed every 4 s. A GET
//!   every 5 s reads the `ratelimit` header: does resetting armed streams cost rate budget?
//! Phase B (--secs): maintain() every 200 ms and a hit at a random moment every 2–4 s, 75% on an
//!   armed target, 25% on an unarmed ladder level (cold path). Reports send time, time to first
//!   response, and how old the armed set was when fired.

use std::time::{Duration, Instant};

use http::{HeaderMap, HeaderValue};
use predict_gateway::presend::{Fanout, RawResponse};
use predict_gateway::{Client, Config, Hitter, Ladder, Side};

const HOST: &str = "api.predict.fun";
const CENTER: u32 = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let secs: u64 = args
        .iter()
        .position(|a| a == "--secs")
        .and_then(|i| args.get(i + 1)?.parse().ok())
        .unwrap_or(60);
    let id = args
        .iter()
        .filter_map(|a| a.parse::<u64>().ok())
        .find(|&n| n != secs)
        .ok_or("usage: hitter <btc_5m_market_id> [--secs N]")?;

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    let mut market = client.market(id).await?;
    if !market.is_btc_5m() || market.trading_status != "OPEN" {
        return Err(format!("market {id} is not an open BTC 5-minute market").into());
    }
    market.fee_rate_bps = 0; // probes: rejected after full validation
    let probe = client.template(&market, 0)?;
    let tick = probe.tick();
    let mut ladder = Ladder::new(probe, 1.0 / tick, 2)?.post_only();
    ladder.recenter(&client, CENTER)?;

    let mut headers = HeaderMap::new();
    let bearer = client.bearer().ok_or("no jwt")?;
    headers.insert("authorization", HeaderValue::from_str(&bearer)?);
    if let Ok(key) = std::env::var("PREDICT_API_KEY") {
        headers.insert("x-api-key", HeaderValue::from_str(key.trim())?);
    }
    headers.insert("user-agent", HeaderValue::from_static("predict-gateway/hitter"));
    let fan = Fanout::connect(HOST, 6, headers).await?;
    let orders = fan.conns()[0].uri("/v1/orders")?;
    let probe_uri = fan.conns()[0].uri("/v1/auth/message")?;
    let budget_conn = predict_gateway::presend::H2Conn::connect(HOST, None, {
        let mut h = HeaderMap::new();
        h.insert("authorization", HeaderValue::from_str(&bearer)?);
        if let Ok(key) = std::env::var("PREDICT_API_KEY") {
            h.insert("x-api-key", HeaderValue::from_str(key.trim())?);
        }
        h
    })
    .await?;

    let targets = [(Side::Buy, CENTER + 1), (Side::Sell, CENTER - 1)];
    let mut hitter = Hitter::new(fan, orders, ladder);
    hitter.set_targets(&targets);

    // ---- phase A: rotation only, watch the rate budget ----
    println!("phase A: rotation only, 2 targets x 6 connections, max age 4 s, 40 s");
    let start = Instant::now();
    let mut next_probe = Instant::now();
    let mut samples: Vec<(f64, u64, u64)> = Vec::new(); // (t, remaining, reset)
    while start.elapsed() < Duration::from_secs(40) {
        hitter.maintain(&client).await?;
        if Instant::now() >= next_probe {
            let r = budget_conn.get(&probe_uri).await?;
            if let Some((left, reset)) = rpm_budget(&r) {
                samples.push((start.elapsed().as_secs_f64(), left, reset));
            }
            next_probe += Duration::from_secs(5);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let a = hitter.stats();
    println!("  arms {}  rotations {}  dead copies {}  reconnects {}", a.arms, a.rotations, a.dead_copies, a.reconnects);
    let mut extra = 0i64;
    let mut gets = 0i64;
    for w in samples.windows(2) {
        let ((_, r0, t0), (_, r1, t1)) = (w[0], w[1]);
        if t1 < t0 {
            // same 60 s window: drop = our GET + anything else that counted
            extra += r0 as i64 - r1 as i64 - 1;
            gets += 1;
        }
    }
    println!(
        "  rate budget: {} same-window intervals, {} requests consumed beyond our own GETs \
         while {} armed copies were reset",
        gets,
        extra,
        a.rotations * 6
    );

    // ---- phase B: hits at random moments ----
    println!("\nphase B: maintain every 200 ms, a hit every 2-4 s, {secs} s");
    let mut rng = 0x2545f4914f6cdd1du64 ^ start.elapsed().as_nanos() as u64;
    let mut rand = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut warm: Vec<(Duration, Duration, Duration)> = Vec::new(); // (send, first, age)
    let mut cold: Vec<(Duration, Duration)> = Vec::new();
    let mut codes: Vec<String> = Vec::new();
    let b_start = Instant::now();
    let mut next_hit = Instant::now() + Duration::from_millis(2000 + rand() % 2000);
    while b_start.elapsed() < Duration::from_secs(secs) {
        if Instant::now() >= next_hit {
            let armed_target = rand() % 4 != 0;
            let (side, t) = if armed_target {
                targets[(rand() % 2) as usize]
            } else {
                (Side::Buy, CENTER - 2) // signed in the ladder, never armed
            };
            let shot = hitter.hit(side, t).await?;
            let (armed, age, send) = (shot.armed, shot.armed_age, shot.send);
            let s = shot.settle().await;
            for r in &s.rejected {
                codes.push(code_of(r));
            }
            codes.extend(s.errors.iter().cloned());
            if s.accepted.is_some() {
                return Err("probe ACCEPTED - aborting (cancel it manually)".into());
            }
            if let Some(first) = s.first {
                if armed {
                    warm.push((send, first, age.unwrap_or_default()));
                } else {
                    cold.push((send, first));
                }
            }
            eprint!("\rhits {}  ", warm.len() + cold.len());
            next_hit = Instant::now() + Duration::from_millis(2000 + rand() % 2000);
        }
        hitter.maintain(&client).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!();

    let b = hitter.stats();
    println!(
        "  arms {}  rotations {}  dead copies {}  reconnects {}  hits armed {}  cold {}",
        b.arms, b.rotations, b.dead_copies, b.reconnects, b.hits_armed, b.hits_cold
    );
    println!("\n{:<28}{:>4}{:>11}{:>9}{:>9}{:>9}", "first response (ms)", "n", "send µs", "min", "p50", "p90");
    let mut w_first: Vec<Duration> = warm.iter().map(|x| x.1).collect();
    let mut w_send: Vec<Duration> = warm.iter().map(|x| x.0).collect();
    row("armed hit (1 byte x6)", &mut w_send, &mut w_first);
    let mut c_first: Vec<Duration> = cold.iter().map(|x| x.1).collect();
    let mut c_send: Vec<Duration> = cold.iter().map(|x| x.0).collect();
    row("cold hit (full body x6)", &mut c_send, &mut c_first);
    for (lo, hi) in [(0.0, 1.0), (1.0, 2.5), (2.5, 4.5)] {
        let mut f: Vec<Duration> = warm
            .iter()
            .filter(|x| (lo..hi).contains(&x.2.as_secs_f64()))
            .map(|x| x.1)
            .collect();
        let mut s: Vec<Duration> = vec![Duration::ZERO; f.len()];
        row(&format!("  armed, set age {lo}-{hi} s"), &mut s, &mut f);
    }
    codes.sort();
    codes.dedup();
    println!("\nresponse codes: {codes:?}");
    Ok(())
}

fn row(name: &str, send: &mut [Duration], first: &mut [Duration]) {
    if first.is_empty() {
        return println!("{name:<28}{:>4}", 0);
    }
    send.sort_unstable();
    first.sort_unstable();
    let p = |v: &[Duration], q: f64| v[((v.len() - 1) as f64 * q) as usize];
    let ms = |d: Duration| format!("{:.2}", d.as_secs_f64() * 1e3);
    println!(
        "{name:<28}{:>4}{:>11.1}{:>9}{:>9}{:>9}",
        first.len(),
        p(send, 0.5).as_secs_f64() * 1e6,
        ms(p(first, 0.0)),
        ms(p(first, 0.5)),
        ms(p(first, 0.9))
    );
}

fn code_of(r: &RawResponse) -> String {
    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
    format!("{} {}", r.status, v["error"].as_str().unwrap_or("?"))
}

/// `(remaining, reset_secs)` of the per-minute window.
fn rpm_budget(r: &RawResponse) -> Option<(u64, u64)> {
    let h = r.headers.get("ratelimit")?.to_str().ok()?;
    let rpm = h.split(',').find(|p| p.contains("\"rpm\""))?;
    let field = |k: &str| rpm.split(';').find_map(|kv| kv.trim().strip_prefix(k)?.parse().ok());
    Some((field("r=")?, field("t=")?))
}
