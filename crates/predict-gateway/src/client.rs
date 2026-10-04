use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Response, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::sync::mpsc;

/// Independent warm order connections for [`Client::fire`], so one stalled TCP connection cannot
/// head-of-line block the next order. predict.fun's measured limits are 40 req/s and 500 req/min.
const FIRE_CONNECTIONS: usize = 4;

use crate::chain::{exchange_index, Chain};
use crate::crypto::{
    domain_separator, eip191_hash, parse_address, parse_u256_dec, push_hex, to_hex, Address,
    SignMode, Signer, B256,
};
use crate::order::{LimitOrder, OrderTemplate, PrepareTimings, RawOrder, SignedOrder};
use crate::{Error, Result};

const JSON: HeaderValue = HeaderValue::from_static("application/json");

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
        text.split_whitespace().any(|tok| {
            let tok = tok.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != ':' && c != '-');
            match tok.split_once('-').map(|(a, b)| (clock_minutes(a), clock_minutes(b))) {
                Some((Some(start), Some(end))) => (end + 1440 - start) % 1440 == 5, // wraps midnight
                _ => false,
            }
        })
    }
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
    pub timings: Timings,
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

#[derive(Debug, Clone, Copy)]
pub struct Timings {
    pub prepare: PrepareTimings,
    /// Request written → response body read.
    pub round_trip: Duration,
}

/// Outcome of a [`Client::fire`] order, delivered on the results channel. `round_trip` is always
/// set (request written → response read); `result` is the placed order or the error.
#[derive(Debug)]
pub struct OrderOutcome {
    pub hash: B256,
    pub round_trip: Duration,
    pub result: Result<Placed>,
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

/// predict.fun order client. Holds warm HTTP/2 connections: one for reads and the awaited
/// `submit`, one reserved for cancels so a cancel never queues behind order traffic, and a small
/// pool for the fire-and-forget [`Client::fire`] path. Every method takes `&self` — nothing on the
/// order path locks — and the JWT is swapped atomically so login/refresh never pauses order flow.
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
    auth: Arc<ArcSwapOption<HeaderValue>>,
    salt: AtomicU64,
    fire_pool: Vec<reqwest::Client>,
    fire_idx: AtomicU64,
    results_tx: mpsc::UnboundedSender<OrderOutcome>,
    results_rx: Mutex<Option<mpsc::UnboundedReceiver<OrderOutcome>>>,
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

        let mut headers = HeaderMap::new();
        if let Some(key) = &cfg.api_key {
            let mut v = HeaderValue::from_str(key).map_err(|_| Error::MissingEnv("PREDICT_API_KEY"))?;
            v.set_sensitive(true);
            headers.insert("x-api-key", v);
        }
        // Separate builds give separate pools, hence separate TCP connections.
        let http = connection(headers.clone())?;
        let cancel_http = connection(headers.clone())?;
        let fire_pool = (0..FIRE_CONNECTIONS)
            .map(|_| connection(headers.clone()))
            .collect::<reqwest::Result<Vec<_>>>()?;

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
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        Ok(Self {
            http,
            cancel_http,
            chain,
            base,
            urls,
            signer: Arc::new(signer),
            mode: Arc::new(mode),
            maker,
            domains,
            auth: Arc::new(ArcSwapOption::empty()),
            salt: AtomicU64::new(seed >> 1),
            fire_pool,
            fire_idx: AtomicU64::new(0),
            results_tx,
            results_rx: Mutex::new(Some(results_rx)),
        })
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    /// Address that owns orders: the Predict account if configured, otherwise the signer.
    pub fn maker(&self) -> String {
        to_hex(&self.maker)
    }

    /// Open every TLS + HTTP/2 connection (read, cancel and the whole fire pool) or check they are
    /// alive. Returns the round trip of the read connection. Call once at startup so the first
    /// order never pays the ~37 ms cold-connect cost.
    pub async fn warm(&self) -> Result<Duration> {
        let rtt = ping(&self.http, &self.urls.auth_message).await?;
        ping(&self.cancel_http, &self.urls.auth_message).await?;
        for http in &self.fire_pool {
            ping(http, &self.urls.auth_message).await?;
        }
        Ok(rtt)
    }

    /// Keep every connection hot by pinging each one every `every` (20–30 s is plenty, well within
    /// the 500 req/min limit). HTTP/2 keep-alive pings run regardless; this also defeats idle TCP
    /// timeouts and keeps the fire pool warm.
    pub fn spawn_keepalive(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let mut conns = vec![self.http.clone(), self.cancel_http.clone()];
        conns.extend(self.fire_pool.iter().cloned());
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

    /// Fetch and sign the login message and store the JWT (valid 24 h). Takes `&self`: the token
    /// lives behind an atomic swap, so a refresh never pauses in-flight orders. Call again, or use
    /// [`Client::spawn_token_refresh`], to renew.
    pub async fn login(&self) -> Result<()> {
        let v = authenticate(
            &self.http,
            &self.urls.auth_message,
            &self.urls.auth,
            &self.signer,
            &self.mode,
            &self.maker(),
        )
        .await?;
        self.auth.store(Some(Arc::new(v)));
        Ok(())
    }

    /// Re-login every `every` in the background so the 24 h JWT never lapses mid-session. On
    /// failure it keeps the current token and retries next tick. Lockless: the fresh bearer is
    /// swapped into the atomic that `fire`/`submit`/`cancel` read.
    pub fn spawn_token_refresh(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let http = self.http.clone();
        let msg_url = self.urls.auth_message.clone();
        let auth_url = self.urls.auth.clone();
        let signer = self.signer.clone();
        let mode = self.mode.clone();
        let maker_hex = self.maker();
        let auth = self.auth.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // the immediate first tick; initial login is explicit
            loop {
                tick.tick().await;
                if let Ok(v) = authenticate(&http, &msg_url, &auth_url, &signer, &mode, &maker_hex).await {
                    auth.store(Some(Arc::new(v)));
                }
            }
        })
    }

    pub async fn market(&self, id: u64) -> Result<Market> {
        let url = self.base.join(&format!("/v1/markets/{id}")).expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    /// All markets with status OPEN (pages through the cursor, 100 per request).
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
            let resp = self.authed(self.http.get(url)).send().await?;
            let status = resp.status();
            let bytes = resp.bytes().await?;
            if !status.is_success() {
                return Err(api_error(status.as_u16(), &bytes));
            }
            let page: Page = serde_json::from_slice(&bytes)?;
            let done = page.data.is_empty() || page.cursor.is_none();
            all.extend(page.data);
            if done {
                return Ok(all);
            }
            after = page.cursor;
        }
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

    /// Fire an order without awaiting the response. Returns its hash at once; the HTTP round trip
    /// runs on the runtime and the outcome arrives on [`Client::results`]. The only work on the
    /// calling task is an atomic bump, a few cheap clones and a spawn — no await, no lock. Pre-sign
    /// with [`Client::prepare`] off the hot path so this moves only bytes.
    pub fn fire(&self, order: SignedOrder) -> B256 {
        let hash = order.hash;
        let i = self.fire_idx.fetch_add(1, Ordering::Relaxed) as usize % self.fire_pool.len();
        let http = self.fire_pool[i].clone();
        let auth = self.auth.load_full();
        let url = self.urls.orders.clone();
        let tx = self.results_tx.clone();
        let prepare = order.timings;
        let body = order.body;
        tokio::spawn(async move {
            let mut req = http.post(url).header(CONTENT_TYPE, JSON).body(body);
            if let Some(a) = &auth {
                req = req.header(AUTHORIZATION, (**a).clone());
            }
            let t = Instant::now();
            let result = send_order(req, prepare).await;
            let round_trip = t.elapsed();
            let _ = tx.send(OrderOutcome { hash, round_trip, result });
        });
        hash
    }

    /// Take the results receiver (available once). Every [`Client::fire`] pushes one
    /// [`OrderOutcome`] here, in completion order.
    pub fn results(&self) -> Option<mpsc::UnboundedReceiver<OrderOutcome>> {
        self.results_rx.lock().unwrap().take()
    }

    /// Sign an order with hand-specified wei amounts, skipping tick rounding. For probing the
    /// server's price/precision rules; see the `tick_probe` example.
    pub fn prepare_raw(&self, t: &OrderTemplate, r: &RawOrder) -> SignedOrder {
        let salt = self.salt.fetch_add(1, Ordering::Relaxed) & (i64::MAX as u64);
        t.sign_raw(&self.signer, &self.mode, salt, r)
    }

    /// Send a prepared order and await the result.
    pub async fn submit(&self, order: SignedOrder) -> Result<Placed> {
        let prepare = order.timings;
        let req = self
            .authed(self.http.post(self.urls.orders.clone()))
            .header(CONTENT_TYPE, JSON)
            .body(order.body);
        send_order(req, prepare).await
    }

    /// Server-side status and fill of one order, by its `0x` hash.
    pub async fn order(&self, hash: &str) -> Result<OrderInfo> {
        let url = self.base.join(&format!("/v1/orders/{hash}")).expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    pub async fn place(&self, t: &OrderTemplate, o: &LimitOrder) -> Result<Placed> {
        let order = self.prepare(t, o)?;
        self.submit(order).await
    }

    /// Current aggregated order book for a market. `bids`/`asks` are `[price, size]`, best first.
    pub async fn orderbook(&self, market_id: u64) -> Result<OrderBook> {
        let url = self
            .base
            .join(&format!("/v1/markets/{market_id}/orderbook"))
            .expect("valid path");
        read(self.authed(self.http.get(url)).send().await?).await
    }

    /// Remove orders from the book by id (max 100). Off-chain only.
    pub async fn cancel(&self, ids: &[&str]) -> Result<Removed> {
        let body = serde_json::json!({ "data": { "ids": ids } }).to_string();
        self.post_raw(&self.cancel_http, &self.urls.remove, body).await
    }

    /// Remove orders from the book by hash (max 100). Off-chain only. The hash is known from
    /// [`SignedOrder::hash`] before the order is even sent, so this needs no order id.
    pub async fn cancel_by_hash(&self, hashes: &[B256]) -> Result<Removed> {
        self.submit_cancel(prepare_cancel(hashes)).await
    }

    /// Send a cancel built ahead of time with [`prepare_cancel`].
    pub async fn submit_cancel(&self, c: CancelOrder) -> Result<Removed> {
        self.post_raw(&self.cancel_http, &self.urls.remove_by_hash, c.body).await
    }

    async fn post_raw<T: DeserializeOwned>(&self, http: &reqwest::Client, url: &Url, body: String) -> Result<T> {
        let resp = self
            .authed(http.post(url.clone()))
            .header(CONTENT_TYPE, JSON)
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            return Err(api_error(status.as_u16(), &bytes));
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Current JWT (`Bearer <jwt>`) after [`Client::login`], as an owned string. Returns `None`
    /// before login.
    pub fn bearer(&self) -> Option<String> {
        self.auth.load_full().map(|v| v.to_str().unwrap_or_default().to_owned())
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.auth.load_full() {
            Some(v) => req.header(AUTHORIZATION, (*v).clone()),
            None => req,
        }
    }
}

/// GET the login message, sign it, POST it, and return the `Bearer <jwt>` header value. Free
/// function so the background refresh task can call it with cloned handles (no `&self`).
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
    let resp = http
        .post(auth_url.clone())
        .header(CONTENT_TYPE, JSON)
        .body(body.to_string())
        .send()
        .await?;
    let token: Token = read(resp).await?;
    let mut v = HeaderValue::from_str(&format!("Bearer {}", token.token))
        .map_err(|_| Error::InvalidOrder("bad token"))?;
    v.set_sensitive(true);
    Ok(v)
}

/// POST a prepared order body and parse the `Placed` result, timing the round trip.
async fn send_order(req: reqwest::RequestBuilder, prepare: PrepareTimings) -> Result<Placed> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Created {
        order_id: String,
        order_hash: String,
        #[serde(default)]
        code: String,
        removal_locked_until: Option<String>,
    }
    let t = Instant::now();
    let created: Result<Created> = async {
        let resp = req.send().await?;
        read(resp).await
    }
    .await;
    let round_trip = t.elapsed();
    let c = created?;
    Ok(Placed {
        order_id: c.order_id,
        order_hash: c.order_hash,
        code: c.code,
        removal_locked_until: c.removal_locked_until,
        timings: Timings { prepare, round_trip },
    })
}

/// A serialised cancel-by-hash request, ready to send with [`Client::submit_cancel`].
pub struct CancelOrder {
    body: String,
}

/// Build a cancel-by-hash body (max 100 hashes) without sending it. Pure CPU, no I/O: build it
/// when the order is prepared so the cancel path is only the write.
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
        .user_agent(concat!("predict-gateway/", env!("CARGO_PKG_VERSION")))
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
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if !status.is_success() {
        return Err(api_error(status.as_u16(), &bytes));
    }
    Ok(serde_json::from_slice::<Envelope<T>>(&bytes)?.data)
}

fn api_error(status: u16, body: &[u8]) -> Error {
    #[derive(Deserialize, Default)]
    struct E {
        error: Option<String>,
        message: Option<String>,
    }
    let e: E = serde_json::from_slice(body).unwrap_or_default();
    Error::Api {
        status,
        code: e.error.unwrap_or_default(),
        message: e.message.unwrap_or_else(|| String::from_utf8_lossy(body).into_owned()),
    }
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
}
