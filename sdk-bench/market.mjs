// Do MARKET orders (and MARKET + FOK) get the same ~165 ms removalLockedUntil taker-delay lock as
// crossing LIMIT orders, and does a fast cancel-by-hash in that window stop them? Built and signed
// with @predictdotfun/sdk (the same path as loop.mjs), sent with fetch. TRADES REAL MONEY: each
// sample is a ~$1 MARKET BUY of Up on the live BTC 5-minute market; filled positions remain.
//
//   node market.mjs [market_id] [samples=4] [--cancel <ms>] [--value <usd>]
//
// Samples alternate isFillOrKill true / false. --cancel also fires a remove-by-hash for the order
// at <ms> after the POST was sent. Per sample: 201 round trip, removalLockedUntil − send, the
// cancel response, status polled to +2 s, and the public trade feed (/v1/orders/matches).
import { config } from "dotenv";
import { Wallet } from "ethers";
import { OrderBuilder, ChainId, Side } from "@predictdotfun/sdk";

config({ path: "../.env", quiet: true });
const API = "https://api.predict.fun";
const { PREDICT_API_KEY, PREDICT_PRIVATE_KEY, PREDICT_ACCOUNT } = process.env;

const pos = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const flag = (name) => {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
};
let marketId = flag("--market") ?? undefined;
const N = Number(pos[0] ?? 4);
const CANCEL_MS = flag("--cancel") !== undefined ? Number(flag("--cancel")) : null;
const VALUE = Number(flag("--value") ?? 1); // USD to spend per market buy (>= 1)
const ET_OFFSET_MIN = -240; // EDT until 2026-11-01, as in taker_hold.rs

const headers = { "x-api-key": PREDICT_API_KEY, "content-type": "application/json" };
let budget = { left: 500, reset: 0 };
const call = async (path, init = {}) => {
  const r = await fetch(API + path, { ...init, headers: { ...headers, ...init.headers } });
  const rpm = (r.headers.get("ratelimit") ?? "").split(",").find((p) => p.includes('"rpm"'));
  if (rpm) budget = { left: Number(/r=(\d+)/.exec(rpm)?.[1]), reset: Number(/t=(\d+)/.exec(rpm)?.[1]) };
  const j = await r.json().catch(() => ({}));
  return { ok: r.ok, status: r.status, j };
};
const must = async (path, init) => {
  const { ok, status, j } = await call(path, init);
  if (!ok) throw new Error(`${status} ${j.error ?? JSON.stringify(j)}`);
  return j;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const minutes = (s) => {
  const m = /^(\d{1,2})(?::(\d{2}))?(am|pm)$/.exec(s);
  if (!m || +m[1] < 1 || +m[1] > 12) return null;
  return ((+m[1] % 12) + (m[3] === "pm" ? 12 : 0)) * 60 + +(m[2] ?? 0);
};
const window = (title) => {
  for (const tok of title.toLowerCase().split(/\s+/)) {
    const [a, b, ...rest] = tok.replace(/^[^a-z0-9:-]+|[^a-z0-9:-]+$/g, "").split("-");
    if (b === undefined || rest.length) continue;
    const s = minutes(a), e = minutes(b);
    if (s !== null && e !== null && (e + 1440 - s) % 1440 === 5) return [s, e];
  }
  return null;
};
const isBtc5m = (m) => {
  const t = `${m.title} ${m.question}`.toLowerCase();
  return (t.includes("btc") || t.includes("bitcoin")) && window(t) !== null;
};

// --- auth ---
const signer = new Wallet(PREDICT_PRIVATE_KEY);
const builder = await OrderBuilder.make(ChainId.BnbMainnet, signer, PREDICT_ACCOUNT ? { predictAccount: PREDICT_ACCOUNT } : {});
const { data: { message } } = await must("/v1/auth/message");
const signature = PREDICT_ACCOUNT ? await builder.signPredictAccountMessage(message) : await signer.signMessage(message);
const { data: { token } } = await must("/v1/auth", { method: "POST", body: JSON.stringify({ signer: PREDICT_ACCOUNT ?? signer.address, message, signature }) });
headers.authorization = `Bearer ${token}`;

// --- pick the BTC 5-minute market trading now, with enough time left (or wait for the next) ---
async function pickMarket(needS) {
  for (;;) {
    if (marketId) {
      const { data: m } = await must(`/v1/markets/${marketId}`);
      if (!isBtc5m(m) || m.tradingStatus !== "OPEN") throw new Error(`market ${marketId} is not an open BTC 5-minute market: '${m.title}'`);
      return m;
    }
    const et = Math.floor(Date.now() / 1000) + ET_OFFSET_MIN * 60;
    const sod = ((et % 86400) + 86400) % 86400;
    const nowMin = Math.floor(sod / 60);
    const leftS = (end) => (((end * 60 - sod) % 86400) + 86400) % 86400;
    let pages = [], after;
    for (let i = 0; i < 200; i++) {
      const q = after ? `&after=${encodeURIComponent(after)}` : "";
      const p = await must(`/v1/markets?status=OPEN&first=100${q}`);
      pages.push(...(p.data ?? []));
      after = p.cursor;
      if (!after || !(p.data ?? []).length) break;
    }
    const btc = pages.filter((m) => isBtc5m(m) && m.tradingStatus === "OPEN");
    if (process.argv.includes("--scan")) {
      const open = pages.filter((m) => m.tradingStatus === "OPEN");
      console.error(`fetched ${pages.length} markets, ${open.length} OPEN, ${btc.length} btc5m OPEN`);
      console.error("sample titles:", pages.slice(0, 5).map((m) => m.title));
      const anyBtc = pages.filter((m) => `${m.title} ${m.question}`.toLowerCase().includes("bitcoin"));
      console.error(`markets mentioning bitcoin: ${anyBtc.length}`);
      anyBtc.slice(0, 6).forEach((m) => console.error(`  status=${m.tradingStatus} window=${JSON.stringify(window(`${m.title} ${m.question}`.toLowerCase()))} title='${m.title}'`));
      process.exit(0);
    }
    let best = null, liveShort = null;
    for (const m of btc) {
      const w = window(`${m.title} ${m.question}`.toLowerCase());
      if (!w) continue;
      const [start, end] = w;
      const live = (((nowMin - start) % 1440) + 1440) % 1440 < 5;
      if (live && leftS(end) >= needS) return m;
      if (live && (!liveShort || leftS(end) < liveShort.l)) liveShort = { m, l: leftS(end) };
      const next = (((start - nowMin) % 1440) + 1440) % 1440 <= 5 && !live;
      if (next && (!best || leftS(end) < best.l)) best = { m, l: leftS(end) };
    }
    if (best) {
      const wait = Math.max(1, best.l - 300 + 2);
      console.error(`waiting ${wait}s for next window '${best.m.title}'`);
      await sleep(wait * 1000);
    } else if (liveShort) {
      console.error(`live window '${liveShort.m.title}' has ${liveShort.l}s left (< ${needS}); waiting ${liveShort.l + 4}s for the next`);
      await sleep((liveShort.l + 4) * 1000);
    } else {
      console.error(`no BTC 5-minute market live or imminent among ${btc.length} open; retrying in 10s`);
      await sleep(10000);
    }
  }
}

const market = await pickMarket(4 * N + 20);
marketId = market.id;
const outcome = market.outcomes[0]; // Up
console.error(`market ${market.id} '${market.title}', ${CANCEL_MS === null ? "no cancel" : `cancel @${CANCEL_MS}ms`}, value $${VALUE}\n`);

const orderbook = async () => {
  const { data } = await must(`/v1/markets/${marketId}/orderbook`);
  return { marketId, asks: (data.asks ?? []).map(([p, q]) => [p, q]), bids: (data.bids ?? []).map(([p, q]) => [p, q]) };
};

const results = [];
for (let i = 0; i < N; i++) {
  if (budget.left < 30) await sleep((budget.reset + 1) * 1000);
  const fok = i % 2 === 0;
  const book = await orderbook();
  if (!book.asks.length) { console.error(`sample ${i + 1}: empty ask book, skip`); continue; }
  let amounts;
  try {
    amounts = builder.getMarketOrderAmounts({ side: Side.BUY, valueWei: BigInt(Math.round(VALUE * 1e18)), slippageBps: 300n, isMinAmountOut: false }, book);
  } catch (e) { console.error(`sample ${i + 1}: amounts ${e.message}, skip`); continue; }
  const order = builder.buildOrder("MARKET", { side: Side.BUY, tokenId: outcome.onChainId, makerAmount: amounts.makerAmount, takerAmount: amounts.takerAmount, nonce: 0n, feeRateBps: market.feeRateBps });
  const typed = builder.buildTypedData(order, { isNegRisk: market.isNegRisk, isYieldBearing: market.isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed);
  const hash = builder.buildTypedDataHash(typed);
  const body = JSON.stringify({ data: { order: { ...signed, hash }, pricePerShare: String(amounts.pricePerShare), strategy: "MARKET", isFillOrKill: fok, isMinAmountOut: false } });

  console.error(`#${i + 1} MARKET BUY Up fok=${fok} ~${Number(amounts.amount) / 1e18} sh @~${Number(amounts.pricePerShare) / 1e18}  ${hash}`);
  const sent = Date.now();
  const t0 = performance.now();
  let cancel = null;
  if (CANCEL_MS !== null) {
    cancel = (async () => {
      await sleep(CANCEL_MS);
      const at = performance.now() - t0;
      const r = await call("/v1/orders/remove-by-hash", { method: "POST", body: JSON.stringify({ data: { hashes: [hash] } }) });
      return { at, status: r.status, body: r.j };
    })();
  }
  const { ok, status, j } = await call("/v1/orders", { method: "POST", body });
  const rtt = performance.now() - t0;
  const lock = j?.data?.removalLockedUntil;
  const lockMs = typeof lock === "string" ? Date.parse(lock) - sent : null;
  console.error(`  POST ${status} in ${rtt.toFixed(1)} ms  ${ok ? `code ${j.data.code} lock ${lock ?? "null"}${lockMs !== null ? ` (${lockMs.toFixed(0)} ms)` : ""}` : `${j.error} ${j.message ?? ""}`}`);
  let cancelInfo = null;
  if (cancel) {
    const c = await cancel;
    const removed = Array.isArray(c.body?.removed) ? c.body.removed.length : null;
    const noop = Array.isArray(c.body?.noop) ? c.body.noop.length : null;
    cancelInfo = `@${c.at.toFixed(1)}ms ${c.status} ${removed !== null ? `removed ${removed} noop ${noop}` : `${c.body?.error ?? ""} ${c.body?.message ?? ""}`}`;
    console.error(`  CANCEL ${cancelInfo}`);
  }

  // status to +2 s
  let state = "(no 201)";
  if (ok) {
    for (const off of [50, 150, 400, 1000, 2000]) {
      await sleep(off - (Date.now() - sent));
      const s = await call(`/v1/orders/${hash}`);
      const d = s.j?.data;
      state = d && d.status ? `${d.status} ${(Number(d.amountFilled) / 1e18).toFixed(2)}/${(Number(d.amount) / 1e18).toFixed(2)}` : `(${s.j?.error ?? "no data"})`;
    }
    if (state.startsWith("OPEN")) {
      const r = await call("/v1/orders/remove-by-hash", { method: "POST", body: JSON.stringify({ data: { hashes: [hash] } }) });
      state += ` → cleanup ${r.status}`;
    }
  }
  console.error(`  status +2 s: ${state}`);
  results.push({ i: i + 1, fok, hash, sent, rtt, status, lock: lock ?? null, lockMs, ok, err: ok ? null : `${j.error} ${j.message ?? ""}`, cancel: cancelInfo, state });
  await sleep(CANCEL_MS !== null ? 4000 : 2500);
}

// --- public trade feed for our hashes ---
await sleep(2000);
const oldest = Math.min(...results.map((r) => r.sent)) - 2000;
let trades = [], after;
for (let i = 0; i < 40; i++) {
  const q = after ? `&after=${encodeURIComponent(after)}` : "";
  const p = await call(`/v1/orders/matches?marketId=${marketId}&first=100${q}`);
  const data = p.j?.data ?? [];
  trades.push(...data);
  const last = data[data.length - 1]?.executedAt;
  after = p.j?.cursor;
  if (!after || (last && Date.parse(last) < oldest)) break;
}
const execOf = (hash) => {
  const mine = trades.filter((t) => t?.taker?.hash === hash);
  const sh = mine.reduce((a, t) => a + Number(t?.taker?.amount ?? 0) / 1e18, 0);
  return mine.length ? `executed ×${mine.length} (${sh.toFixed(2)} sh)` : "not executed";
};

console.log("\n=== MARKET orders: lock, cancel, fill ===");
for (const r of results) {
  console.log(`#${r.i} fok=${r.fok} | POST ${r.status} ${r.ok ? `lock ${r.lock ?? "null"}${r.lockMs !== null ? ` (+${r.lockMs} ms)` : ""}` : `REJECTED ${r.err}`}`);
  if (r.cancel) console.log(`     cancel ${r.cancel}`);
  console.log(`     status +2 s: ${r.state} | public feed: ${execOf(r.hash)}`);
}
