//! Taker-delay probe for predict.fun BTC 5-minute markets ONLY; refuses any other market.
//! Reads `.env`. TRADES REAL MONEY: each round lifts the ask with ~$1 BUY YES and ~$1 BUY NO,
//! plus one more for the cancel race (≈ $9 of positions plus taker fees at the default 3 rounds).
//! Takers are only ever BUYs at the ask and always fill-or-kill, so they never sell into a bid
//! and never rest on the book. The positions are left open.
//!
//!   cargo run --release --example taker_delay                         # current BTC 5-min market
//!   cargo run --release --example taker_delay -- <btc_5m_id> [rounds]
//!
//! The book is quoted in YES prices: the YES ask is the lowest ask, the NO ask is 1 − the
//! highest bid (bought through the outcome-1 token).
//!
//! Tests:
//!   1. round trip: post-only BUY YES at the lowest tick (rests, cancelled) vs post-only BUY YES
//!      at the ask (crosses, must be rejected, free) vs FOK BUY YES / BUY NO at the ask (takes).
//!   2. time to fill: after each taker's response, `GET /v1/orders/{hash}` on a backoff schedule
//!      until it reads FILLED (or killed).
//!   3. cancel race: FOK taker sent, cancel-by-hash 1 ms later on the cancel connection; what the
//!      cancel did and the order's final state.
//! Every accepted order also prints the server `code` and how far `removalLockedUntil` is past
//! the local clock (the server's own cancel lock; keep the host clock NTP-synced).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use predict_gateway::crypto::to_hex;
use predict_gateway::{prepare_cancel, Client, Config, LimitOrder, Market, OrderBook, OrderTemplate, Placed};

const POLL_MS: [u64; 9] = [0, 10, 25, 50, 100, 200, 400, 800, 1600];
const DEAD: [&str; 3] = ["CANCELLED", "INVALIDATED", "EXPIRED"];

type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq)]
enum Leg {
    Yes,
    No,
}

impl Leg {
    fn name(self) -> &'static str {
        match self {
            Leg::Yes => "YES",
            Leg::No => "NO",
        }
    }
}

#[derive(Default)]
struct Report {
    maker_rtt: Vec<Duration>,
    cross_rtt: Vec<Duration>,
    taker_rtt: Vec<Duration>,
    maker_lock: Vec<i64>,
    taker_lock: Vec<i64>,
    fill_seen: Vec<String>,
    race: Vec<String>,
}

#[tokio::main]
async fn main() -> Res<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rounds: usize = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(3);

    let client = Client::new(Config::from_env()?)?;
    client.warm().await?;
    client.login().await?;
    println!("chain {:?}  maker {}", client.chain(), client.maker());

    let market = match args.first() {
        Some(id) => client.market(id.parse()?).await?,
        None => pick(&client, &client.open_markets().await?).await?,
    };
    if !market.is_btc_5m() {
        return Err(format!("market {} '{}' is not a BTC 5-minute market; refusing", market.id, market.title).into());
    }

    let mut reports = Vec::new();
    for m in [&market] {
        println!("\n=== market {} {} | {}", m.id, m.title, m.question);
        let names: Vec<&str> = m.outcomes.iter().map(|o| o.name.as_str()).collect();
        println!("    outcomes {names:?} (0 = YES side of the book, 1 = NO)");
        let yes = client.template(m, 0)?;
        let no = client.template(m, 1)?;
        let mut r = Report::default();
        for round in 0..rounds {
            println!("-- round {}", round + 1);
            if let Err(e) = run_round(&client, m, &yes, &no, round, &mut r).await {
                println!("  round failed: {e}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await; // stay well inside 240 req/min
        }
        reports.push((m.id, r));
    }

    println!("\n=== summary (medians; lock = removalLockedUntil − local clock at response)");
    println!("{:<10}{:>11}{:>13}{:>11}{:>12}{:>12}", "market", "maker rtt", "cross reject", "taker rtt", "maker lock", "taker lock");
    for (id, r) in &mut reports {
        println!(
            "{:<10}{:>11}{:>13}{:>11}{:>12}{:>12}",
            id,
            med(&mut r.maker_rtt),
            med(&mut r.cross_rtt),
            med(&mut r.taker_rtt),
            med_ms(&mut r.maker_lock),
            med_ms(&mut r.taker_lock),
        );
    }
    for (id, r) in &reports {
        println!("\nmarket {id}");
        println!("  fill first seen:  {}", r.fill_seen.join("\n                    "));
        println!("  cancel race:      {}", r.race.join("\n                    "));
    }
    println!("\nnote: filled taker buys were left as open positions.");
    Ok(())
}

async fn run_round(
    client: &Client,
    m: &Market,
    yes: &OrderTemplate,
    no: &OrderTemplate,
    round: usize,
    r: &mut Report,
) -> Res<()> {
    let tick = yes.tick();
    let book = client.orderbook(m.id).await?;
    let (ask, size) = best_ask(&book, tick, Leg::Yes)?;

    // 1a. maker: post-only BUY YES resting at the lowest tick, then cancelled.
    match client.place(yes, &LimitOrder::buy(tick, 1.0 / tick).post_only()).await {
        Ok(p) => {
            r.maker_rtt.push(p.timings.round_trip);
            let lock = lock_ms(&p);
            r.maker_lock.extend(lock);
            println!("  maker   rtt {:>9}  code {:<24} lock {}", fmt(p.timings.round_trip), p.code, fmt_lock(lock));
            cancel_hard(client, &p).await;
        }
        Err(e) => println!("  maker   failed: {e}"),
    }

    // 1b. post-only BUY YES at the ask: crosses, so it must be rejected (nothing trades).
    let t = Instant::now();
    match client.place(yes, &LimitOrder::buy(ask, size).post_only()).await {
        Err(e) => {
            r.cross_rtt.push(t.elapsed());
            println!("  cross   rtt {:>9}  rejected: {e}", fmt(t.elapsed()));
        }
        Ok(p) => {
            println!("  cross   ACCEPTED (ask {ask} not the real best ask?) code {}; cancelling", p.code);
            cancel_hard(client, &p).await;
        }
    }

    // 1c + 2. FOK BUY YES and BUY NO at the ask, then poll until the fill is visible.
    for leg in [Leg::Yes, Leg::No] {
        let book = client.orderbook(m.id).await?;
        let (ask, size) = best_ask(&book, tick, leg)?;
        let t = if leg == Leg::Yes { yes } else { no };
        let p = client.place(t, &LimitOrder::buy(ask, size).fill_or_kill()).await?;
        let lock = lock_ms(&p);
        r.taker_rtt.push(p.timings.round_trip);
        r.taker_lock.extend(lock);
        println!(
            "  taker   rtt {:>9}  code {:<24} lock {}  BUY {} {size} @ {ask} FOK",
            fmt(p.timings.round_trip),
            p.code,
            fmt_lock(lock),
            leg.name()
        );
        let start = Instant::now();
        let mut seen = None;
        let mut last = String::new();
        for off in POLL_MS {
            tokio::time::sleep_until((start + Duration::from_millis(off)).into()).await;
            let info = client.order(&p.order_hash).await?;
            last = format!("{} {}/{}", info.status, info.amount_filled, info.amount);
            let filled = info.status == "FILLED" || (info.amount_filled == info.amount && info.amount_filled != "0");
            if filled || DEAD.contains(&info.status.as_str()) {
                seen = Some((off, start.elapsed(), filled));
                break;
            }
        }
        let line = match seen {
            Some((off, at, true)) => format!("{} poll +{off}ms → FILLED, read at {}", leg.name(), fmt(at)),
            Some((off, at, false)) => format!("{} poll +{off}ms → killed ({last}), read at {}", leg.name(), fmt(at)),
            None => format!("{} still {last} after {}ms", leg.name(), POLL_MS[POLL_MS.len() - 1]),
        };
        println!("  fill    {line}");
        r.fill_seen.push(line);
        if seen.is_none() {
            cancel_hard(client, &p).await; // FOK should never rest; make sure it does not
        }
    }

    // 3. cancel race: FOK BUY at the ask (YES on even rounds, NO on odd), cancel 1 ms later.
    let leg = if round % 2 == 0 { Leg::Yes } else { Leg::No };
    let book = client.orderbook(m.id).await?;
    let (ask, size) = best_ask(&book, tick, leg)?;
    let t = if leg == Leg::Yes { yes } else { no };
    let signed = client.prepare(t, &LimitOrder::buy(ask, size).fill_or_kill())?;
    let hash = to_hex(&signed.hash);
    let kill = prepare_cancel(&[signed.hash]);
    let t = Instant::now();
    let (placed, killed) = tokio::join!(
        async { (client.submit(signed).await, t.elapsed()) },
        async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let sent = t.elapsed();
            (client.submit_cancel(kill).await, sent, t.elapsed())
        }
    );
    let (placed, place_done) = placed;
    let (killed, cancel_sent, cancel_done) = killed;
    let cancel = match &killed {
        Ok(x) => format!("removed {} noop {}", x.removed.len(), x.noop.len()),
        Err(e) => format!("error {e}"),
    };
    let place = match &placed {
        Ok(p) => format!("{} lock {}", p.code, fmt_lock(lock_ms(p))),
        Err(e) => format!("error {e}"),
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let fin = client.order(&hash).await;
    let fin_s = match &fin {
        Ok(i) => format!("{} {}/{}", i.status, i.amount_filled, i.amount),
        Err(e) => format!("? {e}"),
    };
    let line = format!(
        "BUY {} @ {ask} | place done {} [{place}] | cancel sent {} done {} [{cancel}] | final {fin_s}",
        leg.name(),
        fmt(place_done),
        fmt(cancel_sent),
        fmt(cancel_done)
    );
    println!("  race    {line}");
    r.race.push(line);
    if let (Ok(p), Ok(i)) = (&placed, &fin) {
        if i.status == "OPEN" {
            cancel_hard(client, p).await;
        }
    }
    Ok(())
}

/// Price and ~$1 share size to lift the best ask of `leg`. The book is in YES prices: the YES
/// ask is the lowest ask; the NO ask is 1 − the highest bid.
fn best_ask(book: &OrderBook, tick: f64, leg: Leg) -> Res<(f64, f64)> {
    let lvl = match leg {
        Leg::Yes => book.asks.iter().min_by(|a, b| a[0].total_cmp(&b[0])).map(|a| (a[0], a[1])),
        Leg::No => book.bids.iter().max_by(|a, b| a[0].total_cmp(&b[0])).map(|b| (1.0 - b[0], b[1])),
    };
    let (price, avail) = lvl.ok_or(format!("no {} ask", leg.name()))?;
    let price = (price / tick).round() * tick;
    let size = ((1.0 / price) * 100.0).ceil() / 100.0; // ≈ $1 notional
    if !(tick..1.0).contains(&price) {
        return Err(format!("{} ask {price} out of range", leg.name()).into());
    }
    if avail < size {
        return Err(format!("{} ask {price} has only {avail} shares", leg.name()).into());
    }
    Ok((price, size))
}

/// First open BTC 5-minute market with both a YES and a NO ask mid-range and $1+ behind them.
async fn pick(client: &Client, all: &[Market]) -> Res<Market> {
    let candidates = all.iter().filter(|m| m.trading_status == "OPEN" && m.is_btc_5m() && m.outcomes.len() == 2);
    for m in candidates.take(25) {
        let Ok(book) = client.orderbook(m.id).await else { continue };
        let ok = |leg| {
            let tick = 10f64.powi(-(m.decimal_precision as i32));
            best_ask(&book, tick, leg).is_ok_and(|(p, s)| (0.05..=0.95).contains(&p) && p * s >= 1.0)
        };
        if ok(Leg::Yes) && ok(Leg::No) {
            return Ok(m.clone());
        }
    }
    Err("no open BTC 5-minute market with both asks found; pass its id explicitly".into())
}

/// Cancel by id; if the server refuses (removal lock), wait out the lock and retry.
async fn cancel_hard(client: &Client, p: &Placed) {
    for _ in 0..3 {
        match client.cancel(&[&p.order_id]).await {
            Ok(_) => return,
            Err(e) => {
                println!("  cancel {} refused: {e}; retrying after lock", p.order_id);
                let wait = lock_ms(p).unwrap_or(0).max(0) as u64 + 100;
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
        }
    }
    println!("  WARNING: order {} may still be open", p.order_id);
}

/// `removalLockedUntil` minus the local clock, in ms.
fn lock_ms(p: &Placed) -> Option<i64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_millis() as i64;
    Some(rfc3339_ms(p.removal_locked_until.as_deref()?)? - now)
}

/// `YYYY-MM-DDTHH:MM:SS[.fff…](Z|±HH:MM)` → unix ms.
fn rfc3339_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d, h, mi, se) = (n(0..4)?, n(5..7)?, n(8..10)?, n(11..13)?, n(14..16)?, n(17..19)?);
    let mut i = 19;
    let mut ms = 0;
    if b.get(i) == Some(&b'.') {
        let start = i + 1;
        i = start;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        ms = format!("{:0<3}", &s[start..i])[..3].parse().ok()?;
    }
    let offset_min = match b.get(i)? {
        b'Z' | b'z' => 0,
        c @ (b'+' | b'-') => (if *c == b'+' { 1 } else { -1 }) * (n(i + 1..i + 3)? * 60 + n(i + 4..i + 6)?),
        _ => return None,
    };
    let days = days_from_civil(y, mo, d);
    Some((days * 86_400 + h * 3_600 + mi * 60 + se - offset_min * 60) * 1_000 + ms)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn med(v: &mut [Duration]) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.sort_unstable();
    fmt(v[v.len() / 2])
}

fn med_ms(v: &mut [i64]) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.sort_unstable();
    format!("{}ms", v[v.len() / 2])
}

fn fmt_lock(l: Option<i64>) -> String {
    l.map_or("none".into(), |ms| format!("{ms}ms"))
}

fn fmt(d: Duration) -> String {
    format!("{:.2}ms", d.as_secs_f64() * 1e3)
}

#[cfg(test)]
mod tests {
    use super::rfc3339_ms;

    #[test]
    fn parses_rfc3339() {
        assert_eq!(rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_ms("2026-10-04T12:00:00.250Z"), Some(1_791_115_200_250));
        assert_eq!(rfc3339_ms("2026-10-04T14:00:00.25+02:00"), Some(1_791_115_200_250));
    }
}
