// Official-SDK baseline for `examples/live.rs`: the same real $1 post-only BUY at the lowest tick on
// a BTC 5-minute market, signed with @predictdotfun/sdk on the signal and POSTed with fetch, then
// cancelled. Run from sdk-bench/ on the server (needs its node_modules).
//   node loop.mjs <btc_5m_market_id> [samples]
import { config } from "dotenv";
import { Wallet } from "ethers";
import { OrderBuilder, ChainId, Side } from "@predictdotfun/sdk";

config({ path: "../.env", quiet: true });
const API = "https://api.predict.fun";
const { PREDICT_API_KEY, PREDICT_PRIVATE_KEY, PREDICT_ACCOUNT } = process.env;
const marketId = process.argv[2];
const N = Number(process.argv[3] ?? 20);
if (!marketId) throw new Error("usage: node loop.mjs <btc_5m_market_id> [samples]");
const PACE_MS = 1500;

const headers = { "x-api-key": PREDICT_API_KEY, "content-type": "application/json" };
let budget = { left: 500, reset: 0 };
const call = async (path, init = {}) => {
  const r = await fetch(API + path, { ...init, headers: { ...headers, ...init.headers } });
  // ratelimit: "rps";r=39;t=1, "rpm";r=168;t=48
  const rpm = (r.headers.get("ratelimit") ?? "").split(",").find((p) => p.includes('"rpm"'));
  if (rpm) budget = { left: Number(/r=(\d+)/.exec(rpm)?.[1]), reset: Number(/t=(\d+)/.exec(rpm)?.[1]) };
  const j = await r.json();
  if (!r.ok) throw new Error(`${r.status} ${j.error ?? JSON.stringify(j)}`);
  return j;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Same rule as Market::is_btc_5m: a Bitcoin title whose time window spans exactly 5 minutes.
const minutes = (s) => {
  const m = /^(\d{1,2})(?::(\d{2}))?(am|pm)$/.exec(s);
  if (!m || +m[1] < 1 || +m[1] > 12) return null;
  return ((+m[1] % 12) + (m[3] === "pm" ? 12 : 0)) * 60 + +(m[2] ?? 0);
};
const isBtc5m = (m) => {
  const t = `${m.title} ${m.question}`.toLowerCase();
  if (!t.includes("btc") && !t.includes("bitcoin")) return false;
  return t.split(/\s+/).some((tok) => {
    const [a, b, ...rest] = tok.replace(/^[^a-z0-9:-]+|[^a-z0-9:-]+$/g, "").split("-");
    if (b === undefined || rest.length) return false;
    const s = minutes(a), e = minutes(b);
    return s !== null && e !== null && (e + 1440 - s) % 1440 === 5;
  });
};

const signer = new Wallet(PREDICT_PRIVATE_KEY);
const builder = await OrderBuilder.make(ChainId.BnbMainnet, signer, PREDICT_ACCOUNT ? { predictAccount: PREDICT_ACCOUNT } : {});
const { data: { message } } = await call("/v1/auth/message");
const signature = PREDICT_ACCOUNT ? await builder.signPredictAccountMessage(message) : await signer.signMessage(message);
const { data: { token } } = await call("/v1/auth", {
  method: "POST",
  body: JSON.stringify({ signer: PREDICT_ACCOUNT ?? signer.address, message, signature }),
});
headers.authorization = `Bearer ${token}`;

const { data: market } = await call(`/v1/markets/${marketId}`);
if (!isBtc5m(market) || market.tradingStatus !== "OPEN") throw new Error(`market ${marketId} is not an open BTC 5-minute market: '${market.title}'`);
console.error(`market ${market.id} '${market.title}'`);
const outcome = market.outcomes[0];
const tickWei = 10n ** BigInt(18 - market.decimalPrecision);
const qtyWei = 10n ** 18n * 10n ** BigInt(market.decimalPrecision); // $1 at the lowest tick

const prep = [], post = [], total = [], errors = [];
for (let i = 0; i < N; i++) {
  if (budget.left < 30) {
    process.stderr.write(`\rrate budget ${budget.left} left, waiting ${budget.reset}s   `);
    await sleep((budget.reset + 1) * 1000);
  }
  // ---- signal ----
  const t0 = performance.now();
  const { pricePerShare, makerAmount, takerAmount } = builder.getLimitOrderAmounts({
    side: Side.BUY, pricePerShareWei: tickWei, quantityWei: qtyWei,
  });
  const order = builder.buildOrder("LIMIT", {
    side: Side.BUY, tokenId: outcome.onChainId, makerAmount, takerAmount, nonce: 0n, feeRateBps: market.feeRateBps,
  });
  const typed = builder.buildTypedData(order, { isNegRisk: market.isNegRisk, isYieldBearing: market.isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed);
  const hash = builder.buildTypedDataHash(typed);
  const body = JSON.stringify({ data: { order: { ...signed, hash }, pricePerShare: String(pricePerShare), strategy: "LIMIT", isPostOnly: true } });
  const t1 = performance.now();
  try {
    const { data: placed } = await call("/v1/orders", { method: "POST", body });
    const t2 = performance.now();
    prep.push(t1 - t0); post.push(t2 - t1); total.push(t2 - t0);
    await call("/v1/orders/remove", { method: "POST", body: JSON.stringify({ data: { ids: [placed.orderId] } }) });
  } catch (e) {
    errors.push(e.message);
  }
  process.stderr.write(`\rsample ${i + 1}/${N}   `);
  await sleep(PACE_MS);
}
console.error();

const q = (v) => {
  const s = [...v].sort((a, b) => a - b);
  return [0, 0.25, 0.5, 0.9, 1].map((p) => s[Math.floor((s.length - 1) * p)]?.toFixed(2) ?? "-");
};
console.log(`official SDK + fetch: ${total.length} real orders accepted and cancelled`);
console.log(`              min    p25    p50    p90    max   (ms)`);
for (const [k, v] of [["sign", prep], ["POST", post], ["accepted", total]]) console.log(k.padEnd(10), q(v).map((x) => x.padStart(6)).join(" "));
if (errors.length) console.log("errors:", errors);
