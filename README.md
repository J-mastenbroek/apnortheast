# predict.fun order gateway: measurements

Rust order client (`crates/predict-gateway`) against the official TypeScript SDK (`@predictdotfun/sdk` 1.3.8). Everything was measured from AWS `ap-northeast-1` (Tokyo) in October 2026, on BTC 5-minute markets. All times are in ms unless marked otherwise.

## Signal → accepted order (`201`), real orders

Each sample: a $1 post-only BUY at the lowest tick, accepted onto the book, then cancelled.

| path | n | min | p25 | p50 | p90 | max |
|---|---|---|---|---|---|---|
| official SDK + `fetch`, run A | 20 | 33.11 | 36.76 | 39.93 | 52.33 | 1701 |
| official SDK + `fetch`, run B | 20 | 30.33 | 35.63 | 37.74 | 51.95 | 241.5 |
| `Client::submit`, signed on signal | 20 | 23.12 | 25.99 | 27.49 | 32.77 | 34.26 |
| `Hitter::hit`, unarmed level | 20 | 24.36 | 25.44 | 27.25 | 31.47 | 684.3 |
| `Hitter::hit`, armed level | 20 | 21.28 | 23.33 | 24.60 | 30.09 | 32.26 |

## Why the hitter is faster

Where the time goes, signal → accepted order (p50):

| stage | official SDK | `Client::submit` | `Hitter::hit`, armed |
|---|---|---|---|
| build + sign the order | 4.1–4.8 ms (ethers) | 40.9 µs (secp256k1) | 29 ns (pre-signed, `Ladder::take`) |
| hand bytes to the transport | — | 62.5 µs | 24.9 µs (1 byte × 6 connections) |
| bytes on the wire at the signal | whole request | whole request (~1.3 KB body) | 1 byte + `END_STREAM` per connection |
| HTTP stack | Node `fetch` (POST round trip p50 33.4–36.1) | reqwest, one HTTP/2 connection | raw `h2`, 6 connections over 3 Cloudflare edge IPs |
| signal → `201` | 37.7–39.9 | 27.49 | 24.60 |

What each mechanism does:

1. **Ladder.** Orders for every price near the market are signed before the signal (`Ladder::recenter`: 81.8 µs per 1-tick move), so on the signal there is no hashing or ECDSA.
2. **Pre-send.** Headers and the body minus its last byte are sent early on an open HTTP/2 stream; the signal sends the last byte plus `END_STREAM`. Cloudflare forwards the unfinished request to predict.fun (see the exchange-behaviour table).
3. **Fan-out.** The same signed order is armed on 6 connections. All copies share one order hash; the first to arrive executes and the rest are rejected as duplicates. This cuts the tail: max 33.8 ms against 104.5 ms for fan-out without pre-send.

`Hitter` keeps target levels armed, re-arms every 4 s (new set before the old one is reset), reconnects dead connections and picks up a refreshed login token.

## Hitter experiments

### Per technique, real orders

Signal → `201`, 20 per variant, interleaved. `ready` = signal → order bytes in hand (p50).

| variant | ready | p25 | p50 | p90 | max |
|---|---|---|---|---|---|
| sign + reqwest | 66 µs | 26.73 | 28.53 | 33.37 | 145.9 |
| pre-signed + reqwest | 0.4 µs | 27.65 | 29.21 | 40.86 | 54.4 |
| pre-signed + non-blocking fire pool | 0.4 µs | 28.91 | 31.12 | 37.22 | 67.9 |
| pre-signed + raw HTTP/2 | 0.4 µs | 28.52 | 31.26 | 34.90 | 45.4 |
| pre-signed + pre-sent (last byte held) | 0 | 26.76 | 29.44 | 34.04 | 34.2 |
| pre-signed + same order on 6 connections | 0.4 µs | 27.33 | 31.78 | 45.66 | 104.5 |
| pre-signed + pre-sent + 6 connections | 0 | 25.82 | 27.21 | 30.56 | 33.8 |

In the 6-connection variants, duplicate rejections of the losing copies arrived at a p50 of 17.87–19.95 ms.

### Hitter, probe orders

`examples/hitter`, 15 rounds, signal → first response:

| path | send µs (p50) | min | p25 | p50 | p90 | max |
|---|---|---|---|---|---|---|
| `Client::submit` | 65.1 | 7.97 | 8.59 | 9.43 | 11.72 | 31.67 |
| `Hitter::hit`, unarmed | 54.6 | 7.88 | 8.47 | 8.76 | 10.20 | 14.79 |
| `Hitter::hit`, armed | 23.5 | 6.78 | 7.66 | 7.76 | 8.38 | 9.13 |

### Order preparation

| | p50 | p99 |
|---|---|---|
| official SDK: amounts + build + typed data + sign + hash + JSON | 4.1–4.8 ms | — |
| `Client::prepare` (EIP-712 hash + ECDSA + JSON) | 40.9 µs | 56.1 µs |
| `Ladder::take` (pre-signed) | 29 ns | 89 ns |
| `Ladder::recenter` by 1 tick (2 new signatures) | 81.8 µs | 97.8 µs |

### Network path, probe orders

Probe orders are signed with fee 0, so the server rejects them (`create_order_fee_rate_too_low`) after full validation. Times are signal → first response byte.

| | n | p25 | p50 | p90 | max |
|---|---|---|---|---|---|
| `Client::submit` (reqwest) | 60 | 9.72 | 10.38 | 13.80 | — |
| raw HTTP/2, full send | 60 | 9.91 | 10.61 | 13.14 | — |
| pre-sent, last byte held | 60 | 9.06 | 10.02 | 11.84 | — |
| pre-sent, 1 connection | 40 | 8.01 | 8.75 | 11.99 | 35.00 |
| pre-sent, first of 3 connections | 40 | 7.46 | 8.38 | 10.01 | 32.50 |
| pre-sent, first of 6 connections | 40 | 7.39 | 8.06 | 9.39 | 15.98 |
| pre-sent on all 22 ladder levels, 1 fired | 10 | 11.87 | 13.45 | 16.15 | — |

| path segment | time |
|---|---|
| Tokyo box → Cloudflare edge (NRT), ICMP round trip | 2.2–2.5 ms |
| Cloudflare edge only (`/cdn-cgi/trace`), warm | ~3.6 ms |
| through to predict.fun, unauthenticated `401` | ~7.0 ms |

### Exchange behaviour observed

| test | result |
|---|---|
| full body + `content-length` sent, `END_STREAM` held | server responded before `END_STREAM`, 60/60 |
| last byte held | no early responses, 60/60 |
| armed stream held 1 s / 5 s | completed normally |
| armed stream held 15 s / 30 s / 60 s | reset by the server (`PROTOCOL_ERROR`) |
| re-arm every 4 s for 40 s, 2 levels × 6 connections | 18 re-arms, 0 dead streams |
| 108 armed streams reset, unused | ~4 requests of rate budget consumed |
| one real order sent on 3 connections at once | one `201`; two `400 create_order_duplicate_order` |
| rate limit (`ratelimit` header) | 500 req/min, 40 req/s |
| `GET /v1/markets?status=OPEN`, all pages | 125 requests |
| order acceptance, real vs probe (p50) | ~25 ms vs ~9 ms |

## Taker delay on BTC 5-minute markets

Real orders on live BTC 5-minute windows, 2026-10-05. Clock: the server − local offset was taken from the nanosecond `timestamp` of probe rejections (+1.3 to +1.5 ms, ±4.3–5.3 ms), and the host is chrony-synced (~19 ns off NTP). "Lock" = the order's `removalLockedUntil` minus our send time, both on the server clock. Fills were checked against the public trade feed (`GET /v1/orders/matches?marketId=`) and against positions.

### Run 1: order types on a live window

Market 2934510 (2:35–2:40 PM ET), 18:36 UTC.

| order | response | time |
|---|---|---|
| post-only BUY at the lowest tick (rests) | `201`, `removalLockedUntil: null` | 101.6 (same order on a pre-listed market: ~25 p50) |
| post-only BUY YES at the best ask (0.36) | `201` accepted; seconds later not open, no fill | 80.6 |
| FOK limit BUY at the ask | `400 create_order_fill_or_kill_not_supported` ("only supported for MARKET orders") | 10.3 |

### Run 2: 10 crossing limit BUYs at the ask

Market 2934663 (2:45–2:50 PM ET), 18:47:29–18:48:23 UTC. ~$1 each, alternating YES/NO; status polled every 10 ms to +200 ms, then at +250/300/400/600/1000/2000 ms; still-`OPEN` orders cancelled at +2 s (`examples/taker_hold`).

| # | leg | price | `201` | lock | status at +2 s | public feed |
|---|---|---|---|---|---|---|
| 1 | YES | 0.49 | 42.44 | 178.5 | FILLED | executed |
| 2 | NO | 0.52 | 36.21 | 167.7 | FILLED | executed |
| 3 | YES | 0.49 | 40.20 | 176.5 | OPEN 0 filled → CANCELLED | executed |
| 4 | NO | 0.52 | 36.03 | 172.9 | OPEN 0 filled → CANCELLED | not executed |
| 5 | YES | 0.47 | 39.70 | 173.3 | OPEN 0 filled → CANCELLED | executed |
| 6 | NO | 0.55 | 32.60 | 164.3 | OPEN 0 filled → CANCELLED | executed |
| 7 | YES | 0.46 | 38.43 | 173.5 | OPEN 0 filled → CANCELLED | executed |
| 8 | NO | 0.49 | 39.04 | 168.1 | FILLED | executed |
| 9 | YES | 0.49 | 30.85 | 166.0 | FILLED | executed |
| 10 | NO | 0.53 | 38.42 | 167.3 | OPEN 0 filled → CANCELLED | executed |

- **Lock on crossing orders:** 164.3–178.5 ms after send on 10/10. Resting post-only orders: `null`.
- **Status visibility:** `not_found` until +11–17 ms; `OPEN 0 filled` from +30–41 ms. Where status showed FILLED, it first appeared between the +1000 and +2000 ms polls.
- **Status lag:** 5 orders read `OPEN 0 filled` at +2 s, and a cancel then marked them `CANCELLED`, yet the feed lists them as executed. Position afterwards: Up 10.27 shares = the 5 YES fills (10.46) minus the fee taken in shares.
- **Feed resolution:** `executedAt` has 1 s resolution, and each trade carries an on-chain `transactionHash`.

### Run 3: crossing order + cancel of the same order

Market 2934797 (2:55–3:00 PM ET), 18:57:00–18:58:13 UTC. 10 crossing ~$1 limit BUYs at the ask; each was followed by a cancel-by-hash of that order, sent at a fixed delay after the order was sent.

| # | leg | price | `201` | lock | cancel sent | cancel response | public feed |
|---|---|---|---|---|---|---|---|
| 1 | YES | 0.71 | 42.09 | 173.6 | +1.1 | `401` "one or more orders do not belong to this wallet" | executed |
| 2 | NO | 0.37 | 36.86 | 169.2 | +50.4 | `200` | executed |
| 3 | YES | 0.62 | 30.10 | 164.8 | +101.8 | `200` | executed |
| 4 | NO | 0.41 | 32.06 | 167.5 | +150.8 | `200` | executed |
| 5 | YES | 0.65 | 28.98 | 165.9 | +201.8 | `200`; status `CANCELLED` at +251 | executed |
| 6 | NO | 0.22 | 33.92 | 166.6 | +1.4 | `401` (same message) | executed |
| 7 | YES | 0.77 | 35.32 | 167.2 | +51.1 | `200` | executed |
| 8 | NO | 0.23 | 31.51 | 165.1 | +100.9 | `200` | executed |
| 9 | YES | 0.76 | 34.15 | 166.0 | +151.8 | `200` | not executed |
| 10 | NO | 0.22 | 33.90 | 166.7 | +200.8 | `200`; status `CANCELLED` at +251 | executed |

- **Cancel round trip:** 10–17 ms (sent → answered), except #1 (35 ms).
- **Executions:** 9/10 executed. Positions afterwards: Down 18.27 shares (5 NO fills = 18.60 − fee), Up 5.82 (4 YES fills = 5.87 − fee). #5 and #10 were marked `CANCELLED` and still filled.
- **Feed around #9** (server send ≈ 18:58:04.53, lock end ≈ 04.70): Up takers traded at 0.76 through 18:58:04 (last 4.08 shares at :04), then only at ≥ 0.77 from 18:58:05.

### Cancel sent directly after the `201`

`examples/play`, market 2934876 (3:10–3:15 PM ET): BUY YES 1.05 @ 0.96 followed immediately by `cancel_yes()`, ×3. `201` in 28.4–31.5 ms; each cancel answered `removed 0, noop 0`.

## Reproduce

`.env` needs `PREDICT_API_KEY`, `PREDICT_PRIVATE_KEY`, `PREDICT_ACCOUNT` and `DEPLOY_HOST=user@host`. Every example that sends orders refuses anything but a BTC 5-minute market.

```bash
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id>"           # probe orders
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id> --real"    # real $1 resting orders, cancelled
py deploy.py --run "cd sdk-bench && npm i && node loop.mjs <btc_5m_id> 20"           # official SDK
py deploy.py --run "cargo run --release --example taker_hold -- 10"                 # REAL: 10 crossing ~$1 BUYs, live window
py deploy.py --run "cargo run --release --example play"                             # REAL: hand-edited steps, live window
py deploy.py --run "./target/release/examples/raw_get '/v1/positions?first=50'"     # positions
```

The per-technique, network-path and exchange-behaviour experiments are in commit `2d2a33e` (`examples/live.rs`, `presend.rs`, `race.rs`, `ladder.rs`).
