//! Read-only: raw order book + outcomes for markets, this wallet's OPEN orders, and open
//! short-dated crypto markets. Sends no orders. Reads `.env`.
//!
//!   cargo run --release --example inspect -- [market_id ...]

use predict_gateway::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let cfg = Config::from_env()?;
    let api_key = cfg.api_key.clone().unwrap_or_default();
    let base = cfg.chain.api_url();
    let client = Client::new(cfg)?;
    client.warm().await?;
    client.login().await?;
    let bearer = client.bearer().ok_or("no token")?;
    let http = reqwest::Client::new();
    let get = |path: String| {
        http.get(format!("{base}{path}"))
            .header("x-api-key", &api_key)
            .header("authorization", &bearer)
            .send()
    };

    for id in std::env::args().skip(1) {
        let m = client.market(id.parse()?).await?;
        println!("=== market {} '{}' neg_risk {} dp {}", m.id, m.title, m.is_neg_risk, m.decimal_precision);
        for (i, o) in m.outcomes.iter().enumerate() {
            println!("  outcome {i}: {}", o.name);
        }
        let raw = get(format!("/v1/markets/{id}/orderbook")).await?.text().await?;
        let v: serde_json::Value = serde_json::from_str(&raw)?;
        let d = &v["data"];
        for side in ["asks", "bids"] {
            let lv: Vec<String> = d[side].as_array().into_iter().flatten().take(5).map(|x| x.to_string()).collect();
            println!("  {side} (first 5 as returned): {}", lv.join(" "));
        }
    }

    let raw = get("/v1/orders?status=OPEN&first=100".into()).await?.text().await?;
    let v: serde_json::Value = serde_json::from_str(&raw)?;
    let open = v["data"].as_array().cloned().unwrap_or_default();
    println!("\n=== my OPEN orders: {}", open.len());
    for o in &open {
        println!(
            "  id {} market {} side {} filled {}/{} hash {}",
            o["id"], o["marketId"], o["order"]["side"], o["amountFilled"], o["amount"], o["order"]["hash"]
        );
    }

    println!("\n=== open short-dated crypto markets");
    for m in client.open_markets().await? {
        let t = format!("{} {}", m.title, m.question).to_lowercase();
        if t.contains("up or down") || (t.contains("btc") || t.contains("bitcoin")) && (t.contains(":") || t.contains("am") || t.contains("pm")) {
            println!("  {:>8} {:<8} {} | {}", m.id, m.trading_status, m.title, m.question);
        }
    }
    Ok(())
}
