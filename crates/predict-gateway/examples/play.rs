//! Hand-play orders on a BTC 5-minute market. Edit the STEPS block in `main`, then run:
//!
//!   py deploy.py --run "cargo run --release --example play"          # the BTC 5-min market trading now
//!   py deploy.py --run "cargo run --release --example play -- <id>"  # or a specific one
//!
//! Finding the current market scans every open market (~125 of the 500 requests/min).
//!
//! REAL MONEY. Every buy is a limit BUY at the live best ask of that side (YES ask = lowest
//! ask; NO ask = 1 − highest bid), sized to roughly the given dollar amount.

use std::time::Duration;

use predict_gateway::crypto::{to_hex, B256};
use predict_gateway::{Client, Config, LimitOrder, Market, OrderTemplate};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Res<()> {
    let id: Option<u64> = std::env::args().nth(1).map(|a| a.parse()).transpose()?;
    let mut p = Play::new(id).await?;

    // ===================== STEPS: edit freely =====================
    
    // instant
    p.buy_yes(1.0).await?; // ~$1 of YES at the ask
    p.cancel_yes().await?;

    // 50 ms
    p.buy_yes(1.0).await?; // ~$1 of YES at the ask
    p.wait(50).await; 
    p.cancel_yes().await?;

    // 100ms
    p.buy_yes(1.0).await?;
    p.wait(100).await;
    p.cancel_yes().await?;
    
    p.buy_no(1.0).await?;
    p.buy_no(1.0).await?;
    p.buy_no(1.0).await?;
    // ~$1 of NO at the ask
    // p.wait(500).await;      // pause 500 ms
    // p.cancel_yes().await?;  // cancel every YES order this run placed (only removes unfilled remainder)
    // p.cancel_no().await?;   // same for NO

    // A loop: 3 rounds of YES, YES, NO with 2 s between rounds.
    // for _ in 0..3 {
    //     p.buy_yes(1.0).await?;
    //     p.buy_yes(1.0).await?;
    //     p.buy_no(1.0).await?;
    //     p.wait(2000).await;
    // }
    // ===================== end ====================================

    p.report();
    Ok(())
}

struct Play {
    client: Client,
    market: Market,
    yes: OrderTemplate,
    no: OrderTemplate,
    yes_orders: Vec<B256>,
    no_orders: Vec<B256>,
}

#[allow(dead_code)] // steps you comment out in main shouldn't warn
impl Play {
    async fn new(id: Option<u64>) -> Res<Self> {
        dotenvy::dotenv().ok();
        let client = Client::new(Config::from_env()?)?;
        client.warm().await?;
        client.login().await?;
        let market = match id {
            Some(id) => client.market(id).await?,
            None => {
                let (m, left) = client.current_btc_5m().await?;
                println!("current window, {left} s left");
                m
            }
        };
        if !market.is_btc_5m() || market.trading_status != "OPEN" {
            return Err(format!("market {} is not an open BTC 5-minute market: '{}'", market.id, market.title).into());
        }
        println!("market {} '{}'", market.id, market.title);
        let (yes, no) = (client.template(&market, 0)?, client.template(&market, 1)?);
        Ok(Self { client, market, yes, no, yes_orders: Vec::new(), no_orders: Vec::new() })
    }

    async fn buy_yes(&mut self, usd: f64) -> Res<()> {
        self.buy(true, usd).await
    }

    async fn buy_no(&mut self, usd: f64) -> Res<()> {
        self.buy(false, usd).await
    }

    async fn cancel_yes(&mut self) -> Res<()> {
        let hashes = std::mem::take(&mut self.yes_orders);
        self.cancel("YES", &hashes).await
    }

    async fn cancel_no(&mut self) -> Res<()> {
        let hashes = std::mem::take(&mut self.no_orders);
        self.cancel("NO", &hashes).await
    }

    async fn wait(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    async fn buy(&mut self, yes: bool, usd: f64) -> Res<()> {
        let book = self.client.orderbook(self.market.id).await?;
        let ask = if yes {
            book.asks.iter().map(|a| a[0]).reduce(f64::min)
        } else {
            book.bids.iter().map(|b| b[0]).reduce(f64::max).map(|bid| 1.0 - bid)
        };
        let leg = if yes { "YES" } else { "NO" };
        let ask = ask.ok_or(format!("no {leg} ask in the book"))?;
        let template = if yes { &self.yes } else { &self.no };
        let price = (ask / template.tick()).round() * template.tick();
        let size = ((usd / price) * 100.0).ceil() / 100.0;

        let order = self.client.prepare(template, &LimitOrder::buy(price, size))?;
        let hash = order.hash;
        match self.client.submit(order).await {
            Ok(p) => println!("BUY {leg} {size} @ {price:.2}  → {} in {:.1} ms  {}", p.code, p.round_trip.as_secs_f64() * 1e3, to_hex(&hash)),
            Err(e) => println!("BUY {leg} {size} @ {price:.2}  → rejected: {e}"),
        }
        if yes { &mut self.yes_orders } else { &mut self.no_orders }.push(hash);
        Ok(())
    }

    async fn cancel(&self, leg: &str, hashes: &[B256]) -> Res<()> {
        if hashes.is_empty() {
            println!("cancel {leg}: nothing to cancel");
            return Ok(());
        }
        let r = self.client.cancel_by_hash(hashes).await?;
        println!("cancel {leg}: removed {} noop {}", r.removed.len(), r.noop.len());
        Ok(())
    }

    fn report(&self) {
        println!(
            "\nsent {} YES and {} NO order(s) not cancelled. Fills: check positions, not order status (it lags):\n  \
             py deploy.py --run \"./target/release/examples/raw_get '/v1/positions?first=50'\"",
            self.yes_orders.len(),
            self.no_orders.len()
        );
    }
}
