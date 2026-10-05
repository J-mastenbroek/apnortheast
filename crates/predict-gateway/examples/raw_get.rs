//! GET an API path with auth and print the raw JSON. Reads `.env`.
//!   cargo run --release --example raw_get -- "/v1/orders?first=5"
use predict_gateway::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let client = Client::new(Config::from_env()?)?;
    client.login().await?;
    let key = std::env::var("PREDICT_API_KEY")?;
    for path in std::env::args().skip(1) {
        let r = reqwest::Client::new()
            .get(format!("https://api.predict.fun{path}"))
            .header("x-api-key", &key)
            .header("authorization", client.bearer().ok_or("no jwt")?)
            .send()
            .await?;
        println!("== {path} -> {}\n{}", r.status(), r.text().await?);
    }
    Ok(())
}
