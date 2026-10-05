//! Probe whether order-amend routes exist. Sends `{}` (or nothing) to candidate paths with auth
//! and prints status + body; a body of `{}` cannot create or modify an order. Reads `.env`.
//!   cargo run --release --example route_probe
use predict_gateway::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let client = Client::new(Config::from_env()?)?;
    client.login().await?;
    let key = std::env::var("PREDICT_API_KEY")?;
    let jwt = client.bearer().ok_or("no jwt")?;
    let h = format!("0x{}", "0".repeat(64));
    let paths = [
        "/v1/zzz-not-a-route".to_string(),
        "/v1/orders/zzz-not-a-route".to_string(),
        "/v1/orders".to_string(),
        format!("/v1/orders/{h}"),
        "/v1/orders/amend".to_string(),
        "/v1/orders/modify".to_string(),
        "/v1/orders/replace".to_string(),
        "/v1/orders/edit".to_string(),
        "/v1/orders/update".to_string(),
        "/v1/orders/cancel-replace".to_string(),
        format!("/v1/orders/{h}/amend"),
        format!("/v1/orders/{h}/replace"),
    ];
    let http = reqwest::Client::new();
    for p in &paths {
        for m in ["PATCH", "PUT", "POST"] {
            if m == "POST" && (p == "/v1/orders" || p.ends_with(&h)) { continue; }
            let r = http
                .request(m.parse()?, format!("https://api.predict.fun{p}"))
                .header("x-api-key", &key)
                .header("authorization", &jwt)
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await?;
            let s = r.status();
            let t = r.text().await?;
            println!("{m:5} {p:<90} {s} {}", t.chars().take(160).collect::<String>());
        }
    }
    Ok(())
}
