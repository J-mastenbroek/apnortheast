# predict.fun hitter

The fastest client-side way to get an order into predict.fun. It's written in Rust (`crates/predict-gateway`) and runs from AWS Tokyo.

Signal to accepted order, with real orders on a BTC 5-minute market (Oct 2026):

| path | median | p90 |
|---|---|---|
| official `@predictdotfun/sdk` + `fetch` | 37.7–39.9 ms | ~52 ms |
| `Client::submit` (sign on the signal) | 27.5 ms | 32.8 ms |
| **`Hitter::hit`, armed** | **24.6 ms** | **30.1 ms** |

About 25 ms of that is predict.fun's own work to accept the order, and no client can remove it. The hitter strips out everything else.

## How it works

Three tricks stack:

1. **Ladder.** Orders for every price near the market are signed before the signal: about 40 µs of ECDSA per level, done off the hot path. On the signal, fetching a signed order is an array lookup, under 100 ns.
2. **Pre-send** (the HTTP/2 form of the CME/Eurex "all but the last byte" trick). The headers and the whole body except its last byte go out early, on an open stream. On the signal, only that byte plus `END_STREAM` is sent. Cloudflare forwards the unfinished request to predict.fun, so the per-request work happens before the signal.
3. **Fan-out.** The same signed order is armed on 6 connections, spread over Cloudflare's 3 edge IPs. Every copy has the same order hash, so the first to arrive executes and the rest come back as `create_order_duplicate_order`. That cuts the slow tail.

`Hitter` combines all three. It keeps your target prices armed and re-arms them every 4 s: the new set goes up before the old one is reset. It reconnects dead connections and picks up a refreshed login token.

## Setup

1. **Server.** Use an EC2 instance in `ap-northeast-1` (Tokyo); predict.fun's API is served from Tokyo. Install Rust (`rustup`), and Node 22 if you want the SDK baseline. Then put your SSH target in `.env` (step 2).
2. **`.env`** in the repo root. It's gitignored and gets uploaded by `deploy.py`.
   ```
   PREDICT_API_KEY=      # https://developers.predict.fun
   PREDICT_PRIVATE_KEY=  # Privy wallet key, exported at predict.fun/account/settings
   PREDICT_ACCOUNT=      # Predict account (deposit) address; empty for a plain EOA
   PREDICT_CHAIN=        # mainnet (default) | testnet
   DEPLOY_HOST=          # ssh target, e.g. ubuntu@1.2.3.4
   ```
3. **Deploy and build** (`py deploy.py`). It uploads the repo and runs `cargo build --release --examples` on the server.
4. **Smoke test.** This places one real $1 post-only order that cannot cross, then cancels it:
   ```bash
   py deploy.py --run "cargo run --release --example order_once"        # list open BTC 5-min markets (~125 requests!)
   py deploy.py --run "cargo run --release --example order_once -- <id>"
   ```
5. **Benchmark on your box:**
   ```bash
   py deploy.py --run "cargo run --release --example hitter -- <id>"          # probe orders, nothing trades
   py deploy.py --run "cargo run --release --example hitter -- <id> --real"   # 60 real $1 resting orders, cancelled
   py deploy.py --run "cd sdk-bench && npm i && node loop.mjs <id> 20"          # official SDK baseline
   ```

## Writing a hitter

```rust
use std::time::Duration;
use predict_gateway::{Client, Config, Hitter, Ladder, Side};

let client = Client::new(Config::from_env()?)?;
client.login().await?;
client.spawn_token_refresh(Duration::from_secs(12 * 3600)); // the JWT lasts 24 h

let market = client.market(id).await?;                      // a BTC 5-minute market
assert!(market.is_btc_5m());

// One hitter per outcome token: outcomes[0] = YES, outcomes[1] = NO.
let mut yes = Hitter::new(client.fanout(6).await?, Ladder::new(client.template(&market, 0)?, size, 5)?.fill_or_kill());
let mut no  = Hitter::new(client.fanout(6).await?, Ladder::new(client.template(&market, 1)?, size, 5)?.fill_or_kill());

loop {
    // Off the hot path, every ~200 ms and whenever the book moves:
    yes.ladder_mut().recenter(&client, yes_ask_tick)?;      // re-signs only levels that changed
    yes.set_targets(&[(Side::Buy, yes_ask_tick)]);          // 1–2 levels, no more
    yes.maintain(&client).await?;                           // same for `no` (NO ask = 1 − YES bid)

    // On the signal:
    let shot = yes.hit(Side::Buy, yes_ask_tick).await?;     // 1 byte × 6 connections, ~25 µs
    tokio::spawn(async move {
        let s = shot.settle().await;
        // s.accepted = the copy that got in; s.duplicates = the copies that lost the race.
    });
}
```

A tick is a price in market units: `tick = round(price / template.tick())`, so 51 means 0.51 on a 2-decimal market. Hitting a level that isn't armed still works. It sends the pre-signed order in full on all 6 connections, about 2.6 ms slower than an armed hit.

## Rules (all measured)

- **Hold back the last byte, never the full body.** If the whole body is sent with `content-length`, predict.fun executes the order the moment the body is complete, without waiting for `END_STREAM`. That means it fires before your signal. `presend` only ever holds back the last byte.
- **An armed stream lives at least 5 s and is gone by 15 s**, so the hitter re-arms every 4 s. Resetting an unused armed stream costs no rate budget: in a test, 108 resets used about 4 requests' worth.
- **Arm 1–2 levels per hitter.** Arming all 22 levels of a ±5 ladder at once erased the gain.
- **Duplicates are expected.** On a real order raced ×3, exactly one copy got `201`. The `201` may arrive *after* the duplicate rejections, which come back at about 18 ms. That's an upper bound on when the order's hash was registered, not a time-to-queue.
- **Rate limit: 500 requests/min and 40 requests/s per key.** A hit costs one request per connection, so 6 for a 6-way fan-out. `Client::open_markets()` pages through every open market and costs about 125 requests, so pass market ids instead.
- **Prices must sit on the market's tick** (`decimalPrecision`: 0.01 on BTC 5-minute markets). Prices finer than the tick are rejected. `tick_probe` in commit `2d2a33e` re-checks this.
- **Only BTC 5-minute markets.** Every example that sends orders refuses any other market (`Market::is_btc_5m`).
- **Going on-chain wouldn't be faster.** Matching happens off-chain at predict.fun; the BNB Chain exchange only settles. On the Polymarket CTF exchange that predict.fun forked, the fill functions are operator-only (not yet checked on predict.fun's own contracts). A block also takes 0.75–3 s.

## Still up to you

- **The signal.** At ~25 ms per order, the edge is in seeing the event first, e.g. a Binance BTC feed from the same Tokyo box.
- **Rolling markets.** A BTC 5-minute market closes every 5 minutes. Build new templates, ladders and hitters for the next one before it opens.
- **Risk limits and position tracking.** Check `Settled` and `Client::order(hash)` for fills.
- **Taker behaviour.** `examples/taker_delay.rs` measures real fill-or-kill fills. It spends real money (about $9 per run).

## Layout

```
crates/predict-gateway/src/
  client.rs   REST: login, markets, order book, sign, submit, cancel, order status, fanout()
  hitter.rs   Hitter: ladder + armed fan-out + rotation
  presend.rs  raw HTTP/2: H2Conn (arm/fire), Fanout, ArmedSet
  ladder.rs   pre-signed price ladder
  order.rs    order encoding: EIP-712 template, amounts, JSON body
  crypto.rs   keccak, secp256k1, EIP-712/191, Predict-account (Kernel) signatures
examples/     order_once (smoke test), hitter (benchmark), taker_delay (real taker fills)
sdk-bench/    official-SDK baseline (loop.mjs)
deploy.py     upload + build on DEPLOY_HOST
```
