//! Minimal, low-latency order client for predict.fun.
//!
//! ```no_run
//! # async fn run() -> predict_gateway::Result<()> {
//! use predict_gateway::{Client, Config, LimitOrder};
//!
//! let client = Client::new(Config::from_env()?)?;
//! client.warm().await?;                             // open every connection
//! client.login().await?;
//!
//! let market = client.market(123).await?;
//! let yes = client.template(&market, 0)?;           // pre-encode once per outcome
//!
//! let order = client.prepare(&yes, &LimitOrder::buy(0.33, 10.0).post_only())?; // sign ahead
//! let hash = client.fire(order);                    // returns immediately, no await
//! let mut results = client.results().unwrap();
//! let outcome = results.recv().await.unwrap();      // placed/rejected + round trip
//! assert_eq!(outcome.hash, hash);
//! # Ok(()) }
//! ```

mod chain;
mod client;
pub mod crypto;
mod error;
mod hitter;
mod ladder;
mod order;
pub mod presend;

pub use chain::Chain;
pub use client::{
    prepare_cancel, CancelOrder, Client, Config, Market, OrderBook, OrderInfo, OrderOutcome, Outcome,
    Placed, Removed, Timings,
};
pub use error::{Error, Result};
pub use hitter::{Hitter, HitterStats, Settled, Shot, DEFAULT_MAX_AGE};
pub use ladder::Ladder;
pub use order::{LimitOrder, OrderTemplate, PrepareTimings, RawOrder, Side, SignedOrder, NO_EXPIRY};
