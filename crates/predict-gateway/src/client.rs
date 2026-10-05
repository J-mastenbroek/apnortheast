use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Response, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::chain::{exchange_index, Chain};
use crate::crypto::{
    domain_separator, eip191_hash, parse_address, parse_u256_dec, push_hex, to_hex, Address,
    SignMode, Signer, B256,
};
use crate::order::{LimitOrder, OrderTemplate, SignedOrder};
use crate::presend::Fanout;
use crate::{Error, Result};

const JSON: HeaderValue = HeaderValue::from_static("application/json");
const UA: &str = concat!("predict-gateway/", env!("CARGO_PKG_VERSION"));

pub struct Config {
    pub chain: Chain,
    pub api_key: Option<String>,
    /// Hex private key of the signing wallet (for a Predict account: the exported Privy key).
    pub private_key: String,
    /// Predict account (smart wallet / deposit) address. `None` trades as a plain EOA.
    pub predict_account: Option<String>,
}

impl Config {
    /// Reads `PREDICT_CHAIN` (`mainnet` | `testnet`, default mainnet), `PREDICT_API_KEY`,
    /// `PREDICT_PRIVATE_KEY` and `PREDICT_ACCOUNT`.
    pub fn from_env() -> Result<Self> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.trim().is_empty());
        Ok(Self {
            chain: match var("PREDICT_CHAIN").as_deref() {
                Some("testnet") => Chain::Testnet,
                _ => Chain::Mainnet,
            },
            api_key: var("PREDICT_API_KEY"),
            private_key: var("PREDICT_PRIVATE_KEY").ok_or(Error::MissingEnv("PREDICT_PRIVATE_KEY"))?,
            predict_account: var("PREDICT_ACCOUNT"),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub id: u64,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub question: String,
    pub fee_rate_bps: u32,
    pub decimal_precision: u32,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    #[serde(default)]
    pub trading_status: String,
    pub outcomes: Vec<Outcome>,
}

impl Market {
    /// BTC 5-minute up/down market: a Bitcoin title whose time window spans exactly 5 minutes,
    /// e.g. "Bitcoin Up or Down - October 4, 8:45PM-8:50PM ET" (15-min and hourly markets fail).
    /// The only market type anything in this repo should send orders to.
    pub fn is_btc_5m(&self) -> bool {
        let text = format!("{} {}", self.title, self.question).to_lowercase();
        if !(text.contains("btc") || text.contains("bitcoin")) {
            return false;
        }
        self.window_et().is_some()
    }

    /// `(start, end)` of a 5-minute title window in ET minutes past midnight, e.g. 2:35PM-2:40PM →
    /// `(875, 880)`.
    fn window_et(&self) -> Option<(u32, u32)> {
        let text = format!("{} {}", self.title, self.question).to_lowercase();
        text.split_whitespace().find_map(|tok| {
            let tok = tok.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != ':' && c != '-');
            let (a, b) = tok.split_once('-')?;
            let (start, end) = (clock_minutes(a)?, clock_minutes(b)?);
            ((end + 1440 - start) % 1440 == 5).then_some((start, end)) // wraps midnight
        })
    }
}

const MONTHS: [&str; 12] = [
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December",
];

/// US Eastern time now: (`"October 5,"` as in market titles, minutes past midnight, seconds past
/// the minute).
fn eastern_now() -> (String, u32, u32) {
    let utc = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let local = |offset_h: i64| utc + offset_h * 3600;
    // EDT (UTC−4) from the 2nd Sunday of March 02:00 to the 1st Sunday of November 02:00, else EST.
    let (y, _, _) = civil_from_days(local(-5).div_euclid(86_400));
    let nth_sunday = |m: i64, n: i64| {
        let first = days_from_civil(y, m, 1);
        first + (3 - first).rem_euclid(7) + 7 * (n - 1) // 1970-01-04 (day 3) was a Sunday
    };
    let est = local(-5);
    let dst = est >= nth_sunday(3, 2) * 86_400 + 2 * 3600 && est < nth_sunday(11, 1) * 86_400 + 3600;
    let et = local(if dst { -4 } else { -5 });
    let (_, m, d) = civil_from_days(et.div_euclid(86_400));
    let sod = et.rem_euclid(86_400) as u32;
    (format!("{} {d},", MONTHS[(m - 1) as usize]), sod / 60, sod % 60)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Minutes past midnight for "8:45pm" or "8pm".
fn clock_minutes(s: &str) -> Option<u32> {
    let (rest, pm) = match (s.strip_suffix("pm"), s.strip_suffix("am")) {
        (Some(r), _) => (r, true),
        (_, Some(r)) => (r, false),
        _ => return None,
    };
    let (h, m) = match rest.split_once(':') {
        Some((h, m)) => (h.parse::<u32>().ok()?, m.parse::<u32>().ok()?),
        None => (rest.parse::<u32>().ok()?, 0),
    };
    if !(1..=12).contains(&h) || m > 59 {
        return None;
    }
    Some((h % 12 + if pm { 12 } else { 0 }) * 60 + m)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    pub name: String,
    /// ERC-1155 token id (decimal).
    pub on_chain_id: String,
}

#[derive(Debug, Clone)]
pub struct Placed {
    pub order_id: String,
    pub order_hash: String,
    /// Server result code for the create (e.g. whether it matched or rested).
    pub code: String,
    /// RFC 3339 time before which the server refuses to remove this order.
    pub removal_locked_until: Option<String>,
    /// Request written → response body read.
    pub round_trip: Duration,
}

/// Server-side state of one order, from `GET /v1/orders/{hash}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfo {
    pub id: String,
    /// `OPEN`, `FILLED`, `EXPIRED`, `CANCELLED` or `INVALIDATED`.
    pub status: String,
    /// Wei, 1e18 = one share.
    pub amount: String,
    pub amount_filled: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBook {
    #[serde(rename = "marketId")]
    pub market_id: u64,
    #[serde(rename = "updateTimestampMs")]
    pub update_timestamp_ms: u64,
    /// `[price, size]`, ascending (best = lowest ask first).
    pub asks: Vec<[f64; 2]>,
    /// `[price, size]`, descending (best = highest bid first).
    pub bids: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Removed {
    #[serde(default)]
    pub removed: Vec<String>,
    #[serde(default)]
    pub noop: Vec<String>,
}

struct Urls {
    auth_message: Url,
    auth: Url,
    orders: Url,
    remove: Url,
    remove_by_hash: Url,
}

/// predict.fun REST client: login, markets, signing, and the plain order/cancel path. Two warm
/// HTTP/2 connections: one for reads and orders, one reserved for cancels so a cancel never
/// queues behind order traffic. The JWT sits behind an atomic swap, so a refresh never pauses
/// order flow. For the fastest order path, build a [`crate::Hitter`] on [`Client::fanout`].
pub struct Client {
    http: reqwest::Client,
    cancel_http: reqwest::Client,
    chain: Chain,
    base: Url,
    urls: Urls,
    signer: Arc<Signer>,
    mode: Arc<SignMode>,
    maker: Address,
    domains: [B256; 4],
    api_key: Option<HeaderValue>,
    auth: Arc<ArcSwapOption<HeaderValue>>,
    salt: AtomicU64,
}

impl Client {
    pub fn new(cfg: Config) -> Result<Self> {
        let signer = Signer::from_hex(&cfg.private_key)?;
        let chain = cfg.chain;
        let (maker, mode) = match &cfg.predict_account {
            Some(a) => {
                let account = parse_address(a)?;
                (account, SignMode::kernel(chain.id(), &account))
            }
            None => (signer.address(), SignMode::Eoa),
        };

        let api_key = match &cfg.api_key {
            Some(key) => {
                let mut v = HeaderValue::from_str(key).map_err(|_| Error::MissingEnv("PREDICT_API_KEY"))?;
                v.set_sensitive(true);
                Some(v)
            }
            None => None,
        };
        let mut headers = HeaderMap::new();
        if let Some(v) = &api_key {
            headers.insert("x-api-key", v.clone());
        }

        let base = Url::parse(chain.api_url()).expect("static url");
        let url = |p: &str| base.join(p).expect("static path");
        let urls = Urls {
            auth_message: url("/v1/auth/message"),
            auth: url("/v1/auth"),
            orders: url("/v1/orders"),
            remove: url("/v1/orders/remove"),
            remove_by_hash: url("/v1/orders/remove-by-hash"),
        };

        // Pre-hash the EIP-712 domain of every exchange once.
        let domains = chain
            .exchanges()
            .map(|ex| domain_separator("predict.fun CTF Exchange", "1", chain.id(), &ex));

        let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        Ok(Self {
            // Separate builds give separate pools, hence separate TCP connections.
            http: connection(headers.clone())?,
            cancel_http: connection(headers)?,
            chain,
            base,
            urls,
            signer: Arc::new(signer),
            mode: Arc::new(mode),
            maker,
            domains,
            api_key,
            auth: Arc::new(ArcSwapOption::empty()),
            salt: AtomicU64::new(seed >> 1),
        })
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    /// Address that owns orders: the Predict account if configured, otherwise the signer.
    pub fn maker(&self) -> String {
        to_hex(&self.maker)
    }

    /// Open both connections (or check they are alive). Returns the read connection's round trip.
    pub async fn warm(&self) -> Result<Duration> {
        let rtt = ping(&self.http, &self.urls.auth_message).await?;
        ping(&self.cancel_http, &self.urls.auth_message).await?;
        Ok(rtt)
    }

    /// Ping both connections every `every` (20–30 s is plenty) so idle TCP timeouts never bite.
    pub fn spawn_keepalive(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let conns = [self.http.clone(), self.cancel_http.clone()];
        let url = self.urls.auth_message.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                for http in &conns {
                    let _ = ping(http, &url).await;
                }
            }
        })
    }

    /// Fetch and sign the login message and store the JWT (valid 24 h).
    pub async fn login(&self) -> Result<()> {
        let v = authenticate(&self.http, &self.urls.auth_message, &self.urls.auth, &self.signer, &self.mode, &self.maker())
            .await?;
        self.auth.store(Some(Arc::new(v)));
        Ok(())
    }

    /// Re-login every `every` in the background so the 24 h JWT never lapses. On failure it keeps
    /// the current token and retries next tick.
    pub fn spawn_token_refresh(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let http = self.http.clone();
        let (msg_url, auth_url) = (self.urls.auth_message.clone(), self.urls.auth.clone());
        let (signer, mode, maker, auth) = (self.signer.clone(), self.mode.clone(), self.maker(), self.auth.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // the immediate first tick; initial login is explicit
            loop {
                tick.tick().await;
                if let Ok(v) = authenticate(&http, &msg_url, &auth_url, &signer, &mode, &maker).await {
                    auth.store(Some(Arc::new(v)));
                }
            }
        })
    }

    /// `n` raw HTTP/2 connections for a [`crate::Hitter`], spread over the API's edge IPs,
    /// carrying the API key and the current JWT. Call after [`Client::login`].
    pub async fn fanout(&self, n: usize) -> Result<Fanout> {
        let mut h = HeaderMap::new();
        if let Some(k) = &self.api_key {
            h.insert("x-api-key", k.clone());
        }
        if let Some(a) = self.auth_header() {
            h.insert(AUTHORIZATION, (*a).clone());
        }
        h.insert(USER_AGENT, HeaderValue::from_static(UA));
        Fanout::connect(self.base.host_str().expect("static url"), n, h).await
    }

    /// Like [`Client::fanout`], but rank the edge IPs by origin round trip and build the
    /// connections over the fastest ones (see [`Fanout::connect_ranked`]).
    pub async fn fanout_ranked(&self, n: usize) -> Result<Fanout> {
        let mut h = HeaderMap::new();
        if let Some(k) = &self.api_key {
            h.insert("x-api-key", k.clone());
        }
        if let Some(a) = self.auth_header() {
            h.insert(AUTHORIZATION, (*a).clone());
        }
        h.insert(USER_AGENT, HeaderValue::from_static(UA));
        Fanout::connect_ranked(self.base.host_str().expect("static url"), n, h, "/v1/markets?status=OPEN&first=1", 10).await
    }

    pub async fn market(&self, id: u64) -> Result<Market> {
        let url = self.base.join(&format!("/v1/markets/{id}")).expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    /// All markets with status OPEN, 100 per request. Expensive: ~125 requests of the 500/min
    /// budget. Prefer a known market id.
    pub async fn open_markets(&self) -> Result<Vec<Market>> {
        #[derive(Deserialize)]
        struct Page {
            data: Vec<Market>,
            cursor: Option<String>,
        }
        let mut all = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut url = self.base.join("/v1/markets").expect("static path");
            url.query_pairs_mut().append_pair("first", "100").append_pair("status", "OPEN");
            if let Some(c) = &after {
                url.query_pairs_mut().append_pair("after", c);
            }
            let page: Page = read_raw(self.authed(self.http.get(url)).send().await?).await?;
            let done = page.data.is_empty() || page.cursor.is_none();
            all.extend(page.data);
            if done {
                return Ok(all);
            }
            after = page.cursor;
        }
    }

    /// The BTC 5-minute market whose ET title window contains the current time, and the seconds
    /// left in it. Scans every open market: ~125 requests of the 500/min budget.
    pub async fn current_btc_5m(&self) -> Result<(Market, u32)> {
        let markets = self.open_markets().await?;
        let (date, now_min, now_sec) = eastern_now();
        markets
            .into_iter()
            .filter(|m| m.trading_status == "OPEN" && m.is_btc_5m() && m.title.contains(&date))
            .find_map(|m| {
                let (start, _) = m.window_et()?;
                let into = (now_min + 1440 - start) % 1440; // minutes since the window opened
                (into < 5).then(|| (m, (5 - into) * 60 - now_sec))
            })
            .ok_or(Error::InvalidOrder("no BTC 5-minute market open for the current ET time"))
    }

    /// Current aggregated order book for a market. `bids`/`asks` are `[price, size]`, best first.
    pub async fn orderbook(&self, market_id: u64) -> Result<OrderBook> {
        let url = self.base.join(&format!("/v1/markets/{market_id}/orderbook")).expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    /// Pre-encode everything static about orders on `market.outcomes[outcome]`.
    pub fn template(&self, market: &Market, outcome: usize) -> Result<OrderTemplate> {
        let o = market.outcomes.get(outcome).ok_or(Error::InvalidOrder("no such outcome"))?;
        let token_id = parse_u256_dec(&o.on_chain_id).ok_or(Error::InvalidOrder("bad token id"))?;
        OrderTemplate::new(
            self.domains[exchange_index(market.is_neg_risk, market.is_yield_bearing)],
            &self.maker,
            &token_id,
            &o.on_chain_id,
            market.fee_rate_bps,
            market.decimal_precision,
        )
    }

    /// Hash, sign and serialise an order without sending it. Pure CPU, no I/O.
    pub fn prepare(&self, t: &OrderTemplate, o: &LimitOrder) -> Result<SignedOrder> {
        let salt = self.salt.fetch_add(1, Ordering::Relaxed) & (i64::MAX as u64);
        t.sign(&self.signer, &self.mode, salt, o)
    }

    /// Send a prepared order and await the result.
    pub async fn submit(&self, order: SignedOrder) -> Result<Placed> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Created {
            order_id: String,
            order_hash: String,
            #[serde(default)]
            code: String,
            removal_locked_until: Option<String>,
        }
        let req = self.authed(self.http.post(self.urls.orders.clone())).header(CONTENT_TYPE, JSON).body(order.body);
        let t = Instant::now();
        let created: Result<Created> = async { read(req.send().await?).await }.await;
        let round_trip = t.elapsed();
        let c = created?;
        Ok(Placed {
            order_id: c.order_id,
            order_hash: c.order_hash,
            code: c.code,
            removal_locked_until: c.removal_locked_until,
            round_trip,
        })
    }

    /// Server-side status and fill of one order, by its `0x` hash.
    pub async fn order(&self, hash: &str) -> Result<OrderInfo> {
        let url = self.base.join(&format!("/v1/orders/{hash}")).expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    /// Remove orders from the book by id (max 100). Off-chain only.
    pub async fn cancel(&self, ids: &[&str]) -> Result<Removed> {
        let body = serde_json::json!({ "data": { "ids": ids } }).to_string();
        self.post_raw(&self.urls.remove, body).await
    }

    /// Remove orders from the book by hash (max 100). Off-chain only. The hash is known from
    /// [`SignedOrder::hash`] before the order is even sent, so this needs no order id.
    pub async fn cancel_by_hash(&self, hashes: &[B256]) -> Result<Removed> {
        self.submit_cancel(prepare_cancel(hashes)).await
    }

    /// Send a cancel built ahead of time with [`prepare_cancel`].
    pub async fn submit_cancel(&self, c: CancelOrder) -> Result<Removed> {
        self.post_raw(&self.urls.remove_by_hash, c.body).await
    }

    async fn post_raw<T: DeserializeOwned>(&self, url: &Url, body: String) -> Result<T> {
        let req = self.authed(self.cancel_http.post(url.clone())).header(CONTENT_TYPE, JSON).body(body);
        read_raw(req.send().await?).await
    }

    /// Current JWT (`Bearer <jwt>`) after [`Client::login`].
    pub fn bearer(&self) -> Option<String> {
        self.auth_header().map(|v| v.to_str().unwrap_or_default().to_owned())
    }

    pub(crate) fn auth_header(&self) -> Option<Arc<HeaderValue>> {
        self.auth.load_full()
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.auth_header() {
            Some(v) => req.header(AUTHORIZATION, (*v).clone()),
            None => req,
        }
    }
}

/// GET the login message, sign it, POST it, and return the `Bearer <jwt>` header value.
async fn authenticate(
    http: &reqwest::Client,
    msg_url: &Url,
    auth_url: &Url,
    signer: &Signer,
    mode: &SignMode,
    maker_hex: &str,
) -> Result<HeaderValue> {
    #[derive(Deserialize)]
    struct Msg {
        message: String,
    }
    #[derive(Deserialize)]
    struct Token {
        token: String,
    }
    let msg: Msg = read(http.get(msg_url.clone()).send().await?).await?;
    let sig = mode.sign(signer, &eip191_hash(msg.message.as_bytes()));
    let body = serde_json::json!({
        "signer": maker_hex,
        "message": msg.message,
        "signature": to_hex(sig.as_bytes()),
    });
    let resp = http.post(auth_url.clone()).header(CONTENT_TYPE, JSON).body(body.to_string()).send().await?;
    let token: Token = read(resp).await?;
    let mut v = HeaderValue::from_str(&format!("Bearer {}", token.token)).map_err(|_| Error::InvalidOrder("bad token"))?;
    v.set_sensitive(true);
    Ok(v)
}

/// A serialised cancel-by-hash request, ready to send with [`Client::submit_cancel`].
pub struct CancelOrder {
    body: String,
}

/// Build a cancel-by-hash body (max 100 hashes) without sending it. Pure CPU, no I/O.
pub fn prepare_cancel(hashes: &[B256]) -> CancelOrder {
    let mut body = String::with_capacity(32 + hashes.len() * 69);
    body.push_str(r#"{"data":{"hashes":["#);
    for (i, h) in hashes.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push('"');
        push_hex(&mut body, h);
        body.push('"');
    }
    body.push_str("]}}");
    CancelOrder { body }
}

fn connection(headers: HeaderMap) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .use_rustls_tls()
        .http2_prior_knowledge()
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(15))
        .pool_idle_timeout(None)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        .user_agent(UA)
        .default_headers(headers)
        .build()
}

async fn ping(http: &reqwest::Client, url: &Url) -> Result<Duration> {
    let t = Instant::now();
    http.get(url.clone()).send().await?.bytes().await?;
    Ok(t.elapsed())
}

/// Decode `{"success":true,"data":T}` or turn the error body into [`Error::Api`].
async fn read<T: DeserializeOwned>(resp: Response) -> Result<T> {
    #[derive(Deserialize)]
    struct Envelope<T> {
        data: T,
    }
    Ok(read_raw::<Envelope<T>>(resp).await?.data)
}

/// Decode a JSON body as-is, or turn the error body into [`Error::Api`].
async fn read_raw<T: DeserializeOwned>(resp: Response) -> Result<T> {
    #[derive(Deserialize, Default)]
    struct E {
        error: Option<String>,
        message: Option<String>,
    }
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if !status.is_success() {
        let e: E = serde_json::from_slice(&bytes).unwrap_or_default();
        return Err(Error::Api {
            status: status.as_u16(),
            code: e.error.unwrap_or_default(),
            message: e.message.unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned()),
        });
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::Market;

    fn market(title: &str) -> Market {
        Market {
            id: 0,
            title: title.into(),
            question: String::new(),
            fee_rate_bps: 0,
            decimal_precision: 2,
            is_neg_risk: false,
            is_yield_bearing: false,
            trading_status: "OPEN".into(),
            outcomes: vec![],
        }
    }

    #[test]
    fn btc_5m_from_title() {
        assert!(market("Bitcoin Up or Down - October 4, 8:45PM-8:50PM ET").is_btc_5m());
        assert!(market("Bitcoin Up or Down - October 4, 11:55PM-12:00AM ET").is_btc_5m());
        assert!(market("BTC Up or Down - October 4, 9:55AM-10:00AM ET").is_btc_5m());
        assert!(!market("Bitcoin Up or Down - October 4, 8:45PM-9:00PM ET").is_btc_5m());
        assert!(!market("Bitcoin Up or Down - October 4, 8PM ET").is_btc_5m());
        assert!(!market("Ethereum Up or Down - October 4, 8:45PM-8:50PM ET").is_btc_5m());
        assert!(!market("Will Ethereum hit $1,000 or $3,000 first?").is_btc_5m());
    }

    #[test]
    fn window_and_calendar() {
        assert_eq!(market("Bitcoin Up or Down - October 5, 2:35PM-2:40PM ET").window_et(), Some((875, 880)));
        assert_eq!(market("Bitcoin Up or Down - October 5, 11:55PM-12AM ET").window_et(), Some((1435, 0)));
        for day in [0, 3, 20_731, 20_731 + 366] {
            let (y, m, d) = super::civil_from_days(day);
            assert_eq!(super::days_from_civil(y, m, d), day);
        }
        assert_eq!(super::civil_from_days(20_731), (2026, 10, 5));
    }
}
