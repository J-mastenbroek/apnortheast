//! Pre-signed price ladder: signed, serialised orders for every tick within `depth` of a centre
//! price, on both sides, so that on a signal the hot path is an array lookup instead of
//! hash + ECDSA + JSON. Each order is single-use (its salt and hash are unique), so a taken level
//! is re-signed by the next [`Ladder::recenter`] / [`Ladder::refill`], off the hot path.

use crate::order::{LimitOrder, OrderTemplate, Side, SignedOrder, NO_EXPIRY};
use crate::{Client, Error, Result};

pub struct Ladder {
    template: OrderTemplate,
    size: f64,
    depth: u32,
    fill_or_kill: bool,
    post_only: bool,
    expiration: u64,
    /// Ticks per 1.0 USD (100 for a 2-decimal market).
    scale: u32,
    /// Current signed window, inclusive tick range.
    window: Option<(u32, u32)>,
    /// Indexed by tick.
    buys: Vec<Option<SignedOrder>>,
    sells: Vec<Option<SignedOrder>>,
}

impl Ladder {
    /// `size` shares per order, `depth` ticks either side of the centre.
    pub fn new(template: OrderTemplate, size: f64, depth: u32) -> Result<Self> {
        let scale = (1.0 / template.tick()).round() as u32;
        if scale > 10_000 {
            return Err(Error::InvalidOrder("ladder supports up to 4 price decimals"));
        }
        let n = scale as usize;
        Ok(Self {
            template,
            size,
            depth,
            fill_or_kill: false,
            post_only: false,
            expiration: NO_EXPIRY,
            scale,
            window: None,
            buys: (0..n).map(|_| None).collect(),
            sells: (0..n).map(|_| None).collect(),
        })
    }

    /// Sign every level as fill-or-kill (taker: fills completely or not at all).
    pub fn fill_or_kill(mut self) -> Self {
        self.fill_or_kill = true;
        self
    }

    pub fn post_only(mut self) -> Self {
        self.post_only = true;
        self
    }

    pub fn expires_at(mut self, unix_secs: u64) -> Self {
        self.expiration = unix_secs;
        self
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Nearest tick for a price.
    pub fn tick_of(&self, price: f64) -> u32 {
        (price * self.scale as f64).round() as u32
    }

    pub fn window(&self) -> Option<(u32, u32)> {
        self.window
    }

    /// Move the window to `center ± depth` ticks: drop levels that fell out, sign levels that are
    /// missing (new or taken). Returns the number of orders signed.
    pub fn recenter(&mut self, client: &Client, center: u32) -> Result<usize> {
        let lo = center.saturating_sub(self.depth).max(1);
        let hi = center.saturating_add(self.depth).min(self.scale - 1);
        if let Some((old_lo, old_hi)) = self.window {
            for t in old_lo..=old_hi {
                if t < lo || t > hi {
                    self.buys[t as usize] = None;
                    self.sells[t as usize] = None;
                }
            }
        }
        self.window = Some((lo, hi));
        self.refill(client)
    }

    /// Sign any missing levels in the current window. Returns the number of orders signed.
    pub fn refill(&mut self, client: &Client) -> Result<usize> {
        let Some((lo, hi)) = self.window else { return Ok(0) };
        let mut signed = 0;
        for t in lo..=hi {
            for side in [Side::Buy, Side::Sell] {
                let slot = match side {
                    Side::Buy => &mut self.buys[t as usize],
                    Side::Sell => &mut self.sells[t as usize],
                };
                if slot.is_none() {
                    let mut o = LimitOrder::new(side, t as f64 / self.scale as f64, self.size)
                        .expires_at(self.expiration);
                    o.fill_or_kill = self.fill_or_kill;
                    o.post_only = self.post_only;
                    *slot = Some(client.prepare(&self.template, &o)?);
                    signed += 1;
                }
            }
        }
        Ok(signed)
    }

    /// Hot path: take the pre-signed order for `side` at `tick`, if that level is signed.
    #[inline]
    pub fn take(&mut self, side: Side, tick: u32) -> Option<SignedOrder> {
        let book = match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        };
        book.get_mut(tick as usize)?.take()
    }

    /// Borrow without taking (e.g. to arm a pre-sent HTTP/2 stream with its body).
    #[inline]
    pub fn get(&self, side: Side, tick: u32) -> Option<&SignedOrder> {
        let book = match side {
            Side::Buy => &self.buys,
            Side::Sell => &self.sells,
        };
        book.get(tick as usize)?.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Market, Outcome};

    fn setup() -> (Client, OrderTemplate) {
        let client = Client::new(Config {
            chain: crate::Chain::Testnet,
            api_key: None,
            private_key: "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
            predict_account: None,
        })
        .unwrap();
        let market = Market {
            id: 1,
            title: String::new(),
            question: String::new(),
            fee_rate_bps: 200,
            decimal_precision: 2,
            is_neg_risk: false,
            is_yield_bearing: false,
            trading_status: "OPEN".into(),
            outcomes: vec![Outcome { name: "Yes".into(), on_chain_id: "12345".into() }],
        };
        let t = client.template(&market, 0).unwrap();
        (client, t)
    }

    #[test]
    fn signs_window_and_takes_exact_price() {
        let (client, t) = setup();
        let mut l = Ladder::new(t, 10.0, 5).unwrap().fill_or_kill();
        assert_eq!(l.recenter(&client, 50).unwrap(), 22); // 11 ticks × 2 sides
        let o = l.take(Side::Buy, 53).unwrap();
        assert!(o.body().contains(r#""pricePerShare":"530000000000000000""#));
        assert!(o.body().contains(r#""isFillOrKill":true"#));
        assert!(l.take(Side::Buy, 53).is_none()); // single use
        assert!(l.take(Side::Buy, 56).is_none()); // outside window
    }

    #[test]
    fn recenter_only_signs_new_and_taken_levels() {
        let (client, t) = setup();
        let mut l = Ladder::new(t, 10.0, 5).unwrap();
        l.recenter(&client, 50).unwrap();
        let kept = l.get(Side::Sell, 52).unwrap().hash;
        l.take(Side::Sell, 51);
        assert_eq!(l.recenter(&client, 51).unwrap(), 2 + 1); // tick 56 both sides + refilled 51
        assert_eq!(l.get(Side::Sell, 52).unwrap().hash, kept);
        assert!(l.get(Side::Buy, 45).is_none());
    }

    #[test]
    fn clamps_at_price_bounds() {
        let (client, t) = setup();
        let mut l = Ladder::new(t, 10.0, 5).unwrap();
        l.recenter(&client, 2).unwrap();
        assert_eq!(l.window(), Some((1, 7)));
        l.recenter(&client, 98).unwrap();
        assert_eq!(l.window(), Some((93, 99)));
    }
}
