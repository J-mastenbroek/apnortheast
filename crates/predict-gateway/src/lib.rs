//! Low-latency order client for predict.fun.
//!
//! ```no_run
//! # async fn run() -> predict_gateway::Result<()> {
//! use predict_gateway::{Client, Config, Hitter, Ladder, Side};
//!
//! let client = Client::new(Config::from_env()?)?;
//! client.login().await?;
//!
//! let market = client.market(123).await?;
//! let ladder = Ladder::new(client.template(&market, 0)?, 10.0, 5)?.fill_or_kill();
//! let mut yes = Hitter::new(client.fanout(6).await?, ladder);
//! yes.ladder_mut().recenter(&client, 50)?;       // pre-sign ±5 ticks around 0.50
//! yes.set_targets(&[(Side::Buy, 51)]);           // keep BUY @ 0.51 pre-sent
//! yes.maintain(&client).await?;                  // call every ~200 ms
//!
//! let shot = yes.hit(Side::Buy, 51).await?;      // on the signal: 1 byte per connection
//! let settled = shot.settle().await;             // off the hot path
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
pub use client::{prepare_cancel, CancelOrder, Client, Config, Market, OrderBook, OrderInfo, Outcome, Placed, Removed};
pub use error::{Error, Result};
pub use hitter::{Hitter, HitterStats, Settled, Shot, DEFAULT_MAX_AGE};
pub use ladder::Ladder;
pub use order::{LimitOrder, OrderTemplate, Side, SignedOrder, NO_EXPIRY};
