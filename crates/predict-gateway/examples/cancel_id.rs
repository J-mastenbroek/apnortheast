//! Cancel orders by id and print their final state. Reads `.env`.
//!   cargo run --release --example cancel_id -- <order_id>...
use predict_gateway::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let client = Client::new(Config::from_env()?)?;
    client.login().await?;
    let ids: Vec<String> = std::env::args().skip(1).collect();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let r = client.cancel(&refs).await?;
    println!("removed {:?} noop {:?}", r.removed, r.noop);
    Ok(())
}
