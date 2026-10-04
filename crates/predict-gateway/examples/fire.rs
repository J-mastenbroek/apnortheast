//! Demonstrate and measure the zero-blocker fire path. Reads `.env`.
//!
//!   cargo run --release --example fire -- <marketId>
//!
//! Pre-signs a $1 post-only BUY at the lowest tick (cannot fill), then `fire()`s it and measures
//! how long the *hot path* takes to return (no await, no lock) versus the network round trip that
//! completes in the background and arrives on the results channel. The order is cancelled after.

use std::time::Instant;

use predict_gateway::{Client, Config, LimitOrder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let market_id: u64 = std::env::args().nth(1).ok_or("usage: fire <marketId>")?.parse()?;

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?; // open read + cancel + the whole fire pool
    client.login().await?;
    client.spawn_keepalive(std::time::Duration::from_secs(20));
    let mut results = client.results().expect("results receiver");

    let market = client.market(market_id).await?;
    if !market.is_btc_5m() {
        return Err(format!("refusing: market {} is not a BTC 5-minute market ('{}')", market.id, market.title).into());
    }
    let template = client.template(&market, 0)?;
    let tick = template.tick();

    // Pre-sign OFF the hot path. On the signal you only call fire().
    let order = client.prepare(&template, &LimitOrder::buy(tick, 1.0 / tick).post_only())?;
    let signed_in = order.timings.total();

    // Hot path: time only the fire() return.
    let t = Instant::now();
    let hash = client.fire(order);
    let fire_return = t.elapsed();

    // The network result arrives here, off the hot path.
    let outcome = results.recv().await.ok_or("channel closed")?;
    assert_eq!(outcome.hash, hash);

    println!("market {} '{}'", market.id, market.title);
    println!("pre-sign (off hot path)   {signed_in:?}");
    println!("fire() return (HOT PATH)  {fire_return:?}   <- what the algo waits on");
    println!("network round trip (bg)   {:?}", outcome.round_trip);
    match &outcome.result {
        Ok(p) => {
            println!("placed: id {} code {}", p.order_id, p.code);
            let removed = client.cancel(&[&p.order_id]).await?;
            println!("cancelled {:?}", removed.removed);
        }
        Err(e) => println!("rejected: {e}"),
    }
    Ok(())
}
