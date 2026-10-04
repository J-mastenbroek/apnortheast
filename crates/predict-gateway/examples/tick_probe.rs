//! Probe predict.fun's price-precision and tick rules with live orders. Reads `.env`.
//!
//!   cargo run --release --example tick_probe -- <marketId>
//!
//! Places post-only BUY orders FAR below the market (they rest, never cross, never fill) at a
//! ladder of price precisions, and reports which the server accepts or rejects and with what
//! error. For each accepted price it re-reads the aggregated book to show whether that price is
//! its own level (so a sub-cent bid is a genuinely better price) or collapsed into a coarser
//! tick. All accepted orders are cancelled at the end.

use predict_gateway::{Client, Config, Error, RawOrder, Side};

const ONE: u128 = 1_000_000_000_000_000_000; // 1e18

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let market_id: u64 = std::env::args().nth(1).ok_or("usage: tick_probe <marketId>")?.parse()?;

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;

    let market = client.market(market_id).await?;
    if !market.is_btc_5m() {
        return Err(format!("refusing: market {} is not a BTC 5-minute market ('{}')", market.id, market.title).into());
    }
    let template = client.template(&market, 0)?;
    let book = client.orderbook(market_id).await?;
    let best_bid = book.bids.first().map(|l| l[0]).unwrap_or(0.0);
    let best_ask = book.asks.first().map(|l| l[0]).unwrap_or(1.0);
    println!(
        "market {} '{}'  decimalPrecision={}  feeRateBps={}",
        market.id, market.title, market.decimal_precision, market.fee_rate_bps
    );
    println!("book: best bid {best_bid}  best ask {best_ask}  ({} bid levels)\n", book.bids.len());

    // Prices well below best bid, so every probe rests and cannot fill.
    let ladder = ["0.05", "0.051", "0.0511", "0.05111", "0.001", "0.0001", "0.00001"];
    println!("{:<12}{:<22}{:<12}{}", "price", "price_wei", "result", "detail");

    let mut placed: Vec<(String, f64)> = Vec::new();
    for p in ladder {
        let price_wei = parse_price_wei(p);
        // keep makerAmount (collateral) ~ $2 and a clean multiple of 1e10; taker = qty shares
        let qty = round_to(2 * ONE * ONE / price_wei, ONE / 100); // multiple of 1e16 shares
        let maker = price_wei * qty / ONE;
        let order = RawOrder {
            side: Side::Buy,
            price_wei,
            maker_amount: maker,
            taker_amount: qty,
            post_only: true,
            expiration: predict_gateway::NO_EXPIRY,
        };
        let signed = client.prepare_raw(&template, &order);
        match client.submit(signed).await {
            Ok(pl) => {
                println!("{p:<12}{price_wei:<22}{:<12}id {}", "ACCEPTED", pl.order_id);
                placed.push((pl.order_id, price_wei as f64 / ONE as f64));
            }
            Err(Error::Api { code, message, .. }) => {
                println!("{p:<12}{price_wei:<22}{:<12}{code}: {message}", "rejected");
            }
            Err(e) => println!("{p:<12}{price_wei:<22}{:<12}{e}", "error"),
        }
    }

    // Did accepted prices create distinct book levels?
    if !placed.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let book = client.orderbook(market_id).await?;
        println!("\nbid levels now present at/near our probes:");
        for (_, price) in &placed {
            let lvl = book.bids.iter().find(|l| (l[0] - price).abs() < 1e-9);
            match lvl {
                Some(l) => println!("  {price:<10} -> distinct level, size {}", l[1]),
                None => {
                    let coll = book.bids.iter().find(|l| (l[0] - round2(*price)).abs() < 1e-9);
                    println!(
                        "  {price:<10} -> NOT its own level{}",
                        coll.map(|l| format!(" (folded into {} , size {})", l[0], l[1])).unwrap_or_default()
                    );
                }
            }
        }
    }

    // Cleanup.
    if !placed.is_empty() {
        let ids: Vec<&str> = placed.iter().map(|(id, _)| id.as_str()).collect();
        let removed = client.cancel(&ids).await?;
        println!("\ncancelled {} order(s): {:?}", removed.removed.len(), removed.removed);
    }
    Ok(())
}

/// Parse a decimal price string like "0.0511" into 1e18 wei exactly.
fn parse_price_wei(s: &str) -> u128 {
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    let mut wei: u128 = int.parse::<u128>().unwrap_or(0) * ONE;
    let mut scale = ONE / 10;
    for d in frac.bytes() {
        wei += (d - b'0') as u128 * scale;
        scale /= 10;
    }
    wei
}

fn round_to(v: u128, step: u128) -> u128 {
    ((v + step - 1) / step) * step
}

fn round2(p: f64) -> f64 {
    (p * 100.0).round() / 100.0
}
