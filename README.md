# predict.fun order gateway

A low-latency predict.fun order client in Rust (`crates/predict-gateway`), measured against the official TypeScript SDK (`@predictdotfun/sdk`). All measurements are from AWS Tokyo (`ap-northeast-1`) in October 2026, using real orders on BTC 5-minute markets.

## Results

Signal to accepted order (`201`), real orders. Each sample was a $1 post-only BUY at the lowest tick, accepted onto the book and then cancelled.

| path | min | p25 | median | p90 | max |
|---|---|---|---|---|---|
| official SDK + `fetch`, run A | 33.11 | 36.76 | 39.93 | 52.33 | 1701 |
| official SDK + `fetch`, run B | 30.33 | 35.63 | 37.74 | 51.95 | 241.5 |
| gateway `Client::submit` (sign on signal) | 23.12 | 25.99 | 27.49 | 32.77 | 34.26 |
| gateway `Hitter::hit`, unarmed level | 24.36 | 25.44 | 27.25 | 31.47 | 684.3 |
| **gateway `Hitter::hit`, armed level** | **21.28** | **23.33** | **24.60** | **30.09** | **32.26** |

All times in ms. Gateway rows: 20 interleaved rounds (`examples/hitter --real`). SDK: 20 samples per run, run before and after the Rust runs (`sdk-bench/loop.mjs`).

- **The gateway is 13–15 ms faster than the SDK at the median.** The SDK spends 4.1–4.8 ms signing (ethers), and Node's `fetch` adds about 5 ms more.
- **About 25 ms is predict.fun's own work to accept the order.** No client can remove it; the rest is what the gateway cuts.

### Per technique

Signal to `201`, real orders, 20 per variant, interleaved (`ready` = signal → order bytes in hand):

| variant | ready | p25 | median | p90 | max |
|---|---|---|---|---|---|
| sign + reqwest | 66 µs | 26.73 | 28.53 | 33.37 | 145.9 |
| ladder + reqwest | 0.4 µs | 27.65 | 29.21 | 40.86 | 54.4 |
| ladder + non-blocking fire pool | 0.4 µs | 28.91 | 31.12 | 37.22 | 67.9 |
| ladder + raw HTTP/2 | 0.4 µs | 28.52 | 31.26 | 34.90 | 45.4 |
| ladder + pre-send | 0 | 26.76 | 29.44 | 34.04 | 34.2 |
| ladder + 6-way fan-out | 0.4 µs | 27.33 | 31.78 | 45.66 | 104.5 |
| **ladder + pre-send + 6-way fan-out** | **0** | **25.82** | **27.21** | **30.56** | **33.8** |

Individually, the techniques are within noise of each other at n=20. Combined, they give the best median and by far the tightest tail. That combination is what `Hitter` implements.

### Order preparation

| | p50 | p99 |
|---|---|---|
| official SDK: build + sign + hash + JSON | 4.1–4.8 ms | — |
| gateway: sign on signal (`Client::prepare`) | 40.9 µs | 56.1 µs |
| gateway: pre-signed (`Ladder::take`) | 29 ns | 89 ns |

### Network and front-end path

Signal to first response byte, using probe orders: signed with fee 0, so the server rejects them after full validation and nothing trades. 40 rounds:

| | p25 | median | p90 | max |
|---|---|---|---|---|
| pre-send, 1 connection | 8.01 | 8.75 | 11.99 | 35.00 |
| pre-send, 3-way fan-out | 7.46 | 8.38 | 10.01 | 32.50 |
| pre-send, 6-way fan-out | 7.39 | 8.06 | 9.39 | 15.98 |

From the Tokyo box, the round trip to Cloudflare's Tokyo edge (NRT) is about 2.3 ms; Cloudflare to predict.fun and back adds about 3.5 ms.

## How the gateway is fast

1. **Ladder.** Orders for every price near the market are signed before the signal, so on the signal fetching one is an array lookup.
2. **Pre-send** (the HTTP/2 form of the CME/Eurex "all but the last byte" trick). The headers and the whole body except its last byte are sent early; the signal sends that byte plus `END_STREAM`. Cloudflare forwards the unfinished request, so the per-request work happens before the signal.
3. **Fan-out.** The same signed order is armed on 6 connections, spread over Cloudflare's 3 edge IPs. Every copy has the same order hash, so the first to arrive executes and the rest are rejected as duplicates.

`Hitter` combines all three. It keeps target prices armed and re-arms them every 4 s, with the new set up before the old one is reset. It also reconnects dead connections and picks up a refreshed login token.

## Findings

- **Holding back the last byte is safe; holding back only `END_STREAM` is not.** With `content-length` set, predict.fun executes the order as soon as the body is complete, without waiting for `END_STREAM`: 60 of 60 executed before the signal.
- **An armed stream lives at least 5 s and is gone by 15 s.** Rotation over 40 s gave 18 re-arms and 0 dead copies. The 108 resets cost about 4 requests of rate budget.
- **Arm 1–2 levels at a time.** Arming all 22 levels of a ±5 ladder at once erased the gain (median 13.45 ms against 10.86 ms for one armed stream, probe orders).
- **Fan-out never fills twice.** On a real order raced ×3, exactly one copy got `201`; the others got `create_order_duplicate_order`. Duplicate rejections arrive at about 18 ms, so the winning hash is registered by then at the latest.
- **Rate limit: 500 requests/min and 40 requests/s per key.** One `open_markets()` scan costs about 125 requests.
- **Prices must sit on the market's tick** (0.01 on BTC 5-minute markets).

## Reproduce

Put `PREDICT_API_KEY`, `PREDICT_PRIVATE_KEY`, `PREDICT_ACCOUNT` and `DEPLOY_HOST=user@host` in `.env`, then:

```bash
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id>"          # probe orders, nothing trades
py deploy.py --run "cargo run --release --example hitter -- <btc_5m_id> --real"   # 60 real $1 resting orders, cancelled
py deploy.py --run "cd sdk-bench && npm i && node loop.mjs <btc_5m_id> 20"          # official SDK baseline
py deploy.py --run "cargo run --release --example order_once -- <btc_5m_id>"      # one real order + cancel
```

The per-technique variants and the network-path experiments are in commit `2d2a33e` (`examples/live.rs`, `presend.rs`, `race.rs`, `ladder.rs`). Every example that sends orders refuses anything but a BTC 5-minute market.
