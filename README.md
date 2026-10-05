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

SDK breakdown, p50: signing 4.13 / 4.75 ms; POST round trip 36.09 / 33.41 ms (runs A / B).

## Per technique, real orders

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

## Order preparation

| | p50 | p99 |
|---|---|---|
| official SDK: amounts + build + typed data + sign + hash + JSON | 4.1–4.8 ms | — |
| `Client::prepare` (EIP-712 hash + ECDSA + JSON) | 40.9 µs | 56.1 µs |
| `Ladder::take` (pre-signed) | 29 ns | 89 ns |
| `Ladder::recenter` by 1 tick (2 new signatures) | 81.8 µs | 97.8 µs |

## Network path, probe orders

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

## Exchange behaviour observed

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

## Reproduce

`.env` needs `PREDICT_API_KEY`, `PREDICT_PRIVATE_KEY`, `PREDICT_ACCOUNT` and `DEPLOY_HOST=user@host`.

```bash
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id>"          # probe orders
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id> --real"   # real $1 resting orders, cancelled
py deploy.py --run "cd sdk-bench && npm i && node loop.mjs <btc_5m_id> 20"          # official SDK
```

The per-technique, network-path and exchange-behaviour experiments are in commit `2d2a33e` (`examples/live.rs`, `presend.rs`, `race.rs`, `ladder.rs`).
