//! Place ONE real $1 post-only BUY at the lowest tick, then cancel it, and print timings.
//!
//!   cargo run --release --example order_once            # list open BTC markets
//!   cargo run --release --example order_once -- <id>    # trade outcome 0 of market <id>
//!
//! Reads `.env`. Compare with `sdk-bench/` (official TypeScript SDK) on the same market.

use std::time::Instant;

use predict_gateway::{Client, Config, LimitOrder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let client = Client::new(Config::from_env()?)?;

    let t = Instant::now();
    client.warm().await?;
    let connect = t.elapsed();
    let t = Instant::now();
    client.login().await?;
    let login = t.elapsed();

    let Some(id) = std::env::args().nth(1) else {
        for m in client.open_markets().await? {
            let text = format!("{} {}", m.title, m.question).to_lowercase();
            if text.contains("btc") || text.contains("bitcoin") {
                println!("{:>8}  {:<12} {} | {}", m.id, m.trading_status, m.title, m.question);
            }
        }
        return Ok(());
    };

    let market = client.market(id.parse()?).await?;
    if !market.is_btc_5m() {
        return Err(format!("refusing: market {} is not a BTC 5-minute market ('{}')", market.id, market.title).into());
    }
    let template = client.template(&market, 0)?;
    let tick = template.tick();
    let order = LimitOrder::buy(tick, 1.0 / tick).post_only(); // $1 notional, can't fill
    println!(
        "market {} '{}'  outcome '{}'  BUY {} @ {} post-only",
        market.id, market.title, market.outcomes[0].name, order.size, order.price
    );

    let t = Instant::now();
    let signed = client.prepare(&template, &order)?;
    let prepare = t.elapsed();
    let placed = client.submit(signed).await?;
    let place_total = t.elapsed();

    let t = Instant::now();
    let removed = client.cancel(&[&placed.order_id]).await?;
    let cancel = t.elapsed();

    let p = placed.timings.prepare;
    println!("order {}  removed {:?}", placed.order_id, removed.removed);
    println!("connect          {connect:>12.3?}");
    println!("login            {login:>12.3?}");
    println!("prepare          {prepare:>12.3?}  (hash {:?}, sign {:?}, encode {:?})", p.hash, p.sign, p.encode);
    println!("POST round trip  {:>12.3?}", placed.timings.round_trip);
    println!("place total      {place_total:>12.3?}");
    println!("cancel           {cancel:>12.3?}");
    Ok(())
}
