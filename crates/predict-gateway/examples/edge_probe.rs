//! Latency map for the order path: per Cloudflare edge IP, the round trip to the edge
//! (`/cdn-cgi/trace`, which the PoP answers) and the round trip through to predict.fun's origin
//! (a light authenticated GET). Read-only — no orders, no money. Shows where the signal→accepted
//! budget goes and which edge IPs to pin a [`Fanout`] to.
//!
//!   cargo run --release --example edge_probe -- [samples_per_ip=15]
//!
//! `colo` is the Cloudflare PoP (e.g. NRT = Tokyo). `edge` is the PoP round trip; `origin` is the
//! round trip of a request the PoP proxies to predict.fun; `origin − edge` estimates how far the
//! origin sits behind the PoP (a few ms ≈ same metro).

use std::net::SocketAddr;
use std::time::Instant;

use http::header::{AUTHORIZATION, USER_AGENT};
use http::{HeaderMap, HeaderValue};
use predict_gateway::presend::H2Conn;
use predict_gateway::{Client, Config};

type Res<T> = Result<T, Box<dyn std::error::Error>>;
const HOST: &str = "api.predict.fun";

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

#[tokio::main]
async fn main() -> Res<()> {
    dotenvy::dotenv().ok();
    let k: usize = std::env::args().nth(1).map(|s| s.parse()).transpose()?.unwrap_or(15);
    let client = Client::new(Config::from_env()?)?;
    client.login().await?;

    let mut headers = HeaderMap::new();
    if let Ok(key) = std::env::var("PREDICT_API_KEY") {
        headers.insert("x-api-key", HeaderValue::from_str(&key)?);
    }
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&client.bearer().ok_or("no jwt")?)?);
    headers.insert(USER_AGENT, HeaderValue::from_static("predict-gateway-edge-probe"));

    let mut ips: Vec<SocketAddr> = tokio::net::lookup_host((HOST, 443)).await?.filter(SocketAddr::is_ipv4).collect();
    ips.sort();
    ips.dedup();
    println!("{HOST}: {} edge IPs, {k} samples each\n", ips.len());
    println!("{:<16} {:<5} {:>8} {:>8} {:>8} {:>8} {:>8}", "ip", "colo", "edge p50", "edge p90", "orig p50", "orig p90", "o−e p50");

    let mut rows = Vec::new();
    for ip in ips {
        let conn = match H2Conn::connect(HOST, ip, headers.clone()).await {
            Ok(c) => c,
            Err(e) => {
                println!("{:<16} connect failed: {e}", ip.ip().to_string());
                continue;
            }
        };
        let turi = conn.uri("/cdn-cgi/trace")?;
        let ouri = conn.uri("/v1/markets?status=OPEN&first=1")?;
        // Warm the connection (TLS + first stream) before timing.
        let _ = conn.get(&turi).await;
        let mut colo = String::from("?");
        let mut edge = Vec::new();
        let mut orig = Vec::new();
        for _ in 0..k {
            let t = Instant::now();
            if let Ok(r) = conn.get(&turi).await {
                edge.push(t.elapsed().as_secs_f64() * 1000.0);
                if colo == "?" {
                    if let Ok(body) = std::str::from_utf8(&r.body) {
                        if let Some(c) = body.lines().find_map(|l| l.strip_prefix("colo=")) {
                            colo = c.to_owned();
                        }
                    }
                }
            }
            let t = Instant::now();
            if conn.get(&ouri).await.is_ok() {
                orig.push(t.elapsed().as_secs_f64() * 1000.0);
            }
        }
        edge.sort_by(f64::total_cmp);
        orig.sort_by(f64::total_cmp);
        let (ep50, ep90, op50, op90) = (pct(&edge, 0.5), pct(&edge, 0.9), pct(&orig, 0.5), pct(&orig, 0.9));
        println!("{:<16} {colo:<5} {ep50:>8.2} {ep90:>8.2} {op50:>8.2} {op90:>8.2} {:>8.2}", ip.ip().to_string(), op50 - ep50);
        rows.push((ip, colo, ep50, op50));
    }

    rows.sort_by(|a, b| a.3.total_cmp(&b.3));
    println!("\nfastest origin RTT first (pin a Fanout to the top IPs):");
    for (ip, colo, ep50, op50) in rows.iter().take(6) {
        println!("  {} ({colo})  edge {ep50:.2}  origin {op50:.2}", ip.ip());
    }
    if let Some((_, colo, ep50, op50)) = rows.first() {
        println!("\norigin sits ~{:.1} ms RTT behind the {colo} PoP; edge RTT ~{:.1} ms. A region whose nearest PoP is closer to the origin, or to the signal source, is the next lever — measure from a box there with this same tool.", op50 - ep50, ep50);
    }
    Ok(())
}
