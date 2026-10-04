//! Smoke test: ONE real $1 post-only BUY at the lowest tick (rests, cannot cross), then cancel.
//! Reads `.env`.
//!
//!   cargo run --release --example order_once            # list open BTC 5-minute markets (~125 requests)
//!   cargo run --release --example order_once -- <id>    # place + cancel on that market

use std::time::Instant;

use predict_gateway::{Client, Config, LimitOrder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;

    let Some(id) = std::env::args().nth(1) else {
        for m in client.open_markets().await?.iter().filter(|m| m.is_btc_5m()) {
            println!("{:>8}  {:<8} {}", m.id, m.trading_status, m.title);
        }
        return Ok(());
    };

    let market = client.market(id.parse()?).await?;
    if !market.is_btc_5m() {
        return Err(format!("market {} is not a BTC 5-minute market: '{}'", market.id, market.title).into());
    }
    let template = client.template(&market, 0)?;
    let tick = template.tick();
    let t = Instant::now();
    let order = client.prepare(&template, &LimitOrder::buy(tick, 1.0 / tick).post_only())?;
    let sign = t.elapsed();
    let hash = order.hash;
    let placed = client.submit(order).await?;
    let t = Instant::now();
    let removed = client.cancel_by_hash(&[hash]).await?;
    let cancel = t.elapsed();

    println!("market {} '{}': order {} removed {:?}", market.id, market.title, placed.order_id, removed.removed);
    println!("sign {sign:?}  place {:?}  cancel {cancel:?}", placed.round_trip);
    Ok(())
}
