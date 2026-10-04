//! Order construction. All static parts of an order (EIP-712 domain, typehash, maker, token, fee,
//! the static JSON fields) are encoded once per outcome in an [`OrderTemplate`]; building an order
//! then patches five 32-byte words, hashes 416 bytes, signs, and writes the JSON body.

use std::fmt::Write;
use std::time::{Duration, Instant};

use crate::crypto::{eip712_digest, keccak256, push_hex, Address, SignMode, Signer, B256};
use crate::{Error, Result};

/// Default expiration used by the official SDK for limit orders: 2100-01-01T00:00:00Z.
pub const NO_EXPIRY: u64 = 4_102_444_800;

const ORDER_TYPE: &[u8] = b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";

// Word offsets in the encoded struct (typehash at 0).
const W_SALT: usize = 32;
const W_MAKER_AMOUNT: usize = 192;
const W_TAKER_AMOUNT: usize = 224;
const W_EXPIRATION: usize = 256;
const W_SIDE: usize = 352;
const STRUCT_LEN: usize = 416;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

/// A limit order. `price` is per share in USD (e.g. `0.33`) and must sit on the market's tick;
/// `size` is in shares and is truncated to the finest size the API accepts.
#[derive(Debug, Clone, Copy)]
pub struct LimitOrder {
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub post_only: bool,
    pub fill_or_kill: bool,
    /// Unix seconds.
    pub expiration: u64,
}

impl LimitOrder {
    pub fn buy(price: f64, size: f64) -> Self {
        Self::new(Side::Buy, price, size)
    }

    pub fn sell(price: f64, size: f64) -> Self {
        Self::new(Side::Sell, price, size)
    }

    pub fn new(side: Side, price: f64, size: f64) -> Self {
        Self { side, price, size, post_only: false, fill_or_kill: false, expiration: NO_EXPIRY }
    }

    pub fn post_only(mut self) -> Self {
        self.post_only = true;
        self
    }

    pub fn fill_or_kill(mut self) -> Self {
        self.fill_or_kill = true;
        self
    }

    pub fn expires_at(mut self, unix_secs: u64) -> Self {
        self.expiration = unix_secs;
        self
    }
}

/// Pre-encoded, signing-ready state for one outcome token. Build with [`crate::Client::template`].
#[derive(Clone)]
pub struct OrderTemplate {
    domain_separator: B256,
    words: [u8; STRUCT_LEN],
    json_static: String,
    decimal_precision: u32,
}

impl OrderTemplate {
    pub(crate) fn new(
        domain_separator: B256,
        maker: &Address,
        token_id: &B256,
        token_id_dec: &str,
        fee_rate_bps: u32,
        decimal_precision: u32,
    ) -> Result<Self> {
        if decimal_precision == 0 || decimal_precision > 8 {
            return Err(Error::InvalidOrder("unsupported decimal precision"));
        }
        let mut words = [0u8; STRUCT_LEN];
        words[0..32].copy_from_slice(&keccak256(ORDER_TYPE));
        words[76..96].copy_from_slice(maker); // maker
        words[108..128].copy_from_slice(maker); // signer
        // taker (128) = zero address, nonce (288) = 0, signatureType (384) = 0 (EOA / EIP-1271)
        words[160..192].copy_from_slice(token_id);
        words[344..352].copy_from_slice(&(fee_rate_bps as u64).to_be_bytes());

        let mut json_static = String::with_capacity(256);
        json_static.push_str(r#""maker":""#);
        push_hex(&mut json_static, maker);
        json_static.push_str(r#"","signer":""#);
        push_hex(&mut json_static, maker);
        write!(
            json_static,
            r#"","taker":"0x0000000000000000000000000000000000000000","tokenId":"{token_id_dec}","nonce":"0","feeRateBps":"{fee_rate_bps}","signatureType":0"#
        )
        .unwrap();

        Ok(Self { domain_separator, words, json_static, decimal_precision })
    }

    /// Price tick, e.g. `0.01` for a market with 2 decimals.
    pub fn tick(&self) -> f64 {
        10f64.powi(-(self.decimal_precision as i32))
    }

    pub(crate) fn sign(
        &self,
        signer: &Signer,
        mode: &SignMode,
        salt: u64,
        o: &LimitOrder,
    ) -> Result<SignedOrder> {
        let a = amounts(o, self.decimal_precision)?;
        Ok(self.sign_amounts(signer, mode, salt, o.side, o.post_only, o.fill_or_kill, o.expiration, &a))
    }

    /// Sign with explicit wei amounts, bypassing tick/precision rounding. For probing the
    /// server's own validation; a normal strategy uses [`crate::Client::prepare`].
    pub(crate) fn sign_raw(
        &self,
        signer: &Signer,
        mode: &SignMode,
        salt: u64,
        r: &RawOrder,
    ) -> SignedOrder {
        let a = Amounts { price_wei: r.price_wei, maker: r.maker_amount, taker: r.taker_amount };
        self.sign_amounts(signer, mode, salt, r.side, r.post_only, false, r.expiration, &a)
    }

    #[allow(clippy::too_many_arguments)]
    fn sign_amounts(
        &self,
        signer: &Signer,
        mode: &SignMode,
        salt: u64,
        side: Side,
        post_only: bool,
        fill_or_kill: bool,
        expiration: u64,
        a: &Amounts,
    ) -> SignedOrder {
        let t0 = Instant::now();
        let mut words = self.words;
        put_u64(&mut words, W_SALT, salt);
        put_u128(&mut words, W_MAKER_AMOUNT, a.maker);
        put_u128(&mut words, W_TAKER_AMOUNT, a.taker);
        put_u64(&mut words, W_EXPIRATION, expiration);
        words[W_SIDE + 31] = side as u8;
        let hash = eip712_digest(&self.domain_separator, &keccak256(&words));
        let t1 = Instant::now();

        let sig = mode.sign(signer, &hash);
        let t2 = Instant::now();

        let mut body = String::with_capacity(self.json_static.len() + 512);
        write!(
            body,
            r#"{{"data":{{"pricePerShare":"{}","strategy":"LIMIT","isFillOrKill":{},"isPostOnly":{},"order":{{"hash":""#,
            a.price_wei, fill_or_kill, post_only
        )
        .unwrap();
        push_hex(&mut body, &hash);
        write!(
            body,
            r#"","salt":"{salt}","makerAmount":"{}","takerAmount":"{}","expiration":{},"side":{},"signature":""#,
            a.maker, a.taker, expiration, side as u8
        )
        .unwrap();
        push_hex(&mut body, sig.as_bytes());
        body.push_str("\",");
        body.push_str(&self.json_static);
        body.push_str("}}}");
        let t3 = Instant::now();

        SignedOrder {
            hash,
            body,
            timings: PrepareTimings { hash: t1 - t0, sign: t2 - t1, encode: t3 - t2 },
        }
    }
}

/// An order with hand-specified wei amounts, for probing server-side price/precision rules.
#[derive(Debug, Clone, Copy)]
pub struct RawOrder {
    pub side: Side,
    /// `pricePerShare`, in 1e18 wei (e.g. 0.001 → 1_000_000_000_000_000).
    pub price_wei: u128,
    pub maker_amount: u128,
    pub taker_amount: u128,
    pub post_only: bool,
    pub expiration: u64,
}

/// A fully signed, serialised order, ready to send with [`crate::Client::submit`].
pub struct SignedOrder {
    pub hash: B256,
    pub(crate) body: String,
    pub timings: PrepareTimings,
}

impl SignedOrder {
    pub fn body(&self) -> &str {
        &self.body
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PrepareTimings {
    /// Amount conversion + EIP-712 hashing.
    pub hash: Duration,
    /// ECDSA signature.
    pub sign: Duration,
    /// JSON body serialisation.
    pub encode: Duration,
}

impl PrepareTimings {
    pub fn total(&self) -> Duration {
        self.hash + self.sign + self.encode
    }
}

struct Amounts {
    price_wei: u128,
    maker: u128,
    taker: u128,
}

const fn pow10(n: u32) -> u128 {
    10u128.pow(n)
}

/// With `dp` price decimals: price = ticks·10^(18−dp) wei, qty = lots·10^(10+dp) wei, so the
/// collateral leg price·qty/1e18 = ticks·lots·1e10 is exact and a multiple of 1e10 as the API requires.
fn amounts(o: &LimitOrder, dp: u32) -> Result<Amounts> {
    let scale = pow10(dp) as f64;
    let ticks_f = o.price * scale;
    let ticks = ticks_f.round();
    if (ticks_f - ticks).abs() > 1e-6 {
        return Err(Error::InvalidOrder("price not on tick"));
    }
    if ticks < 1.0 || ticks >= scale {
        return Err(Error::InvalidOrder("price out of range"));
    }
    if !(o.size > 0.0) {
        return Err(Error::InvalidOrder("size must be positive"));
    }
    let lots = (o.size * pow10(8 - dp) as f64 + 1e-6).floor() as u128;
    if lots == 0 {
        return Err(Error::InvalidOrder("size below minimum increment"));
    }
    let ticks = ticks as u128;
    let qty = lots * pow10(10 + dp);
    let value = ticks * lots * pow10(10);
    let (maker, taker) = match o.side {
        Side::Buy => (value, qty),
        Side::Sell => (qty, value),
    };
    Ok(Amounts { price_wei: ticks * pow10(18 - dp), maker, taker })
}

#[inline]
fn put_u64(words: &mut [u8; STRUCT_LEN], at: usize, v: u64) {
    words[at + 24..at + 32].copy_from_slice(&v.to_be_bytes());
}

#[inline]
fn put_u128(words: &mut [u8; STRUCT_LEN], at: usize, v: u128) {
    words[at + 16..at + 32].copy_from_slice(&v.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_amounts() {
        let a = amounts(&LimitOrder::buy(0.33, 10.0), 2).unwrap();
        assert_eq!(a.price_wei, 330_000_000_000_000_000);
        assert_eq!(a.maker, 3_300_000_000_000_000_000);
        assert_eq!(a.taker, 10_000_000_000_000_000_000);
    }

    #[test]
    fn sell_amounts_and_granularity() {
        let a = amounts(&LimitOrder::sell(0.57, 1.234567891), 2).unwrap();
        assert_eq!(a.maker, 1_234_567_000_000_000_000); // truncated to 1e-6 shares
        assert_eq!(a.taker, 57 * 1_234_567 * 10_000_000_000);
        assert_eq!(a.taker % 10_000_000_000, 0);
    }

    #[test]
    fn rejects_off_tick() {
        assert!(amounts(&LimitOrder::buy(0.335, 10.0), 2).is_err());
        assert!(amounts(&LimitOrder::buy(1.0, 10.0), 2).is_err());
        assert!(amounts(&LimitOrder::buy(0.5, 0.0), 2).is_err());
    }
}
