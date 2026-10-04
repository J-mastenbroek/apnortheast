//! Always-armed hitter: a [`Ladder`] of pre-signed orders, the levels you expect to hit kept
//! pre-sent (last byte held) on every connection of a [`Fanout`], and re-armed before the server
//! resets them. On the signal, [`Hitter::hit`] sends one byte per connection.
//!
//! ```ignore
//! let mut h = Hitter::new(fan, orders_uri, ladder);
//! h.set_targets(&[(Side::Buy, ask_tick)]);
//! loop {
//!     // every ~200 ms, and after every price move (with ladder_mut().recenter(..) first):
//!     h.maintain(&client).await?;
//!     // on the signal:
//!     let shot = h.hit(Side::Buy, ask_tick).await?;
//!     tokio::spawn(async move { let settled = shot.settle().await; /* ... */ });
//! }
//! ```
//!
//! Rotation is make-before-break: the replacement set is armed *before* the old one is reset, so
//! a level never goes unarmed, and at most one level rotates per [`Hitter::maintain`] call so the
//! re-arm requests are spread out. A re-arm reuses the same signed body (an unfinished request
//! cannot execute, and every copy shares one hash), so rotation costs no signing.

use std::time::{Duration, Instant};

use bytes::Bytes;
use h2::client::ResponseFuture;
use http::Uri;

use crate::crypto::B256;
use crate::presend::{self, ArmedSet, Fanout, Hold, RawResponse};
use crate::{Client, Error, Ladder, Result, Side};

/// The server resets a held stream somewhere between 5 s and 15 s; re-arm well before.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(4);

pub struct Hitter {
    fan: Fanout,
    uri: Uri,
    ladder: Ladder,
    targets: Vec<(Side, u32)>,
    slots: Vec<Slot>,
    max_age: Duration,
    stats: HitterStats,
}

struct Slot {
    side: Side,
    tick: u32,
    hash: B256,
    set: ArmedSet,
    armed_at: Instant,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HitterStats {
    /// Armed sets created (first arm + rotations).
    pub arms: u64,
    pub rotations: u64,
    /// Armed copies that died before being fired.
    pub dead_copies: u64,
    pub reconnects: u64,
    /// Hits fired from an armed set.
    pub hits_armed: u64,
    /// Hits on an unarmed level: pre-signed order sent in full on every connection.
    pub hits_cold: u64,
}

/// A fired order. Await [`Shot::settle`] (off the hot path) for the outcome.
pub struct Shot {
    pub hash: B256,
    /// Fired from a pre-sent set (`true`) or sent in full (`false`).
    pub armed: bool,
    /// How long the fired set had been armed.
    pub armed_age: Option<Duration>,
    pub fired_at: Instant,
    /// Signal → all copies handed to the transport.
    pub send: Duration,
    responses: Vec<Result<ResponseFuture>>,
}

#[derive(Default)]
pub struct Settled {
    /// Fire → the accepted copy's response head, and that response.
    pub accepted: Option<(Duration, RawResponse)>,
    /// Fire → first response head of any copy.
    pub first: Option<Duration>,
    /// Losing copies rejected as duplicates (proof that some copy got in).
    pub duplicates: usize,
    /// Other non-2xx responses.
    pub rejected: Vec<RawResponse>,
    pub errors: Vec<String>,
}

impl Hitter {
    pub fn new(fan: Fanout, orders_uri: Uri, ladder: Ladder) -> Self {
        Self {
            fan,
            uri: orders_uri,
            ladder,
            targets: Vec::new(),
            slots: Vec::new(),
            max_age: DEFAULT_MAX_AGE,
            stats: HitterStats::default(),
        }
    }

    /// Re-arm a set once it is this old (default 4 s).
    pub fn max_age(mut self, d: Duration) -> Self {
        self.max_age = d;
        self
    }

    pub fn ladder(&self) -> &Ladder {
        &self.ladder
    }

    /// Recenter or refill the ladder here; armed levels keep their (still valid) orders.
    pub fn ladder_mut(&mut self) -> &mut Ladder {
        &mut self.ladder
    }

    /// For refreshing the `authorization` header on every connection.
    pub fn fanout_mut(&mut self) -> &mut Fanout {
        &mut self.fan
    }

    pub fn stats(&self) -> HitterStats {
        self.stats
    }

    /// The levels to keep armed. Keep it to one or two: arming many at once erased the gain.
    /// Takes effect on the next [`Hitter::maintain`].
    pub fn set_targets(&mut self, targets: &[(Side, u32)]) {
        self.targets = targets.to_vec();
    }

    pub fn is_armed(&self, side: Side, tick: u32) -> bool {
        self.slots.iter().any(|s| s.side == side && s.tick == tick)
    }

    /// Housekeeping, off the hot path; call every ~200 ms. Heals dead connections, re-signs used
    /// ladder levels, drops dead copies, arms new targets, disarms old ones, and rotates the
    /// oldest set once it reaches `max_age`.
    pub async fn maintain(&mut self, client: &Client) -> Result<()> {
        let healed = self.fan.heal().await?;
        self.stats.reconnects += healed as u64;
        self.ladder.refill(client)?;

        // Disarm levels no longer targeted; drop copies that died.
        let mut keep = Vec::with_capacity(self.slots.len());
        for mut s in self.slots.drain(..) {
            if !self.targets.contains(&(s.side, s.tick)) {
                s.set.cancel();
                continue;
            }
            self.stats.dead_copies += s.set.prune_dead().await as u64;
            if s.set.len() * 2 > self.fan.conns().len() {
                keep.push(s);
            } else {
                s.set.cancel(); // too few live copies left: re-arm below
            }
        }
        self.slots = keep;

        // Arm targets that have no set.
        for (side, tick) in self.targets.clone() {
            if !self.is_armed(side, tick) {
                let slot = self.arm(side, tick).await?;
                self.slots.push(slot);
            }
        }

        // Rotate the oldest set if due: make the new one, then break the old one.
        if let Some(i) = (0..self.slots.len())
            .filter(|&i| self.slots[i].armed_at.elapsed() >= self.max_age)
            .min_by_key(|&i| self.slots[i].armed_at)
        {
            let (side, tick) = (self.slots[i].side, self.slots[i].tick);
            let fresh = self.arm(side, tick).await?;
            let old = std::mem::replace(&mut self.slots[i], fresh);
            old.set.cancel();
            self.stats.rotations += 1;
        }
        Ok(())
    }

    /// Fire `side` at `tick`: one byte per connection if armed, otherwise the pre-signed ladder
    /// order sent in full on every connection. Errors if the level is neither armed nor signed.
    pub async fn hit(&mut self, side: Side, tick: u32) -> Result<Shot> {
        if let Some(i) = self.slots.iter().position(|s| s.side == side && s.tick == tick) {
            let slot = self.slots.swap_remove(i);
            let fired_at = Instant::now();
            let responses = slot.set.fire();
            let send = fired_at.elapsed();
            // Consume the ladder's order if it is the one we just sent, so it is re-signed.
            if self.ladder.get(side, tick).is_some_and(|o| o.hash == slot.hash) {
                self.ladder.take(side, tick);
            }
            self.stats.hits_armed += 1;
            return Ok(Shot {
                hash: slot.hash,
                armed: true,
                armed_age: Some(fired_at - slot.armed_at),
                fired_at,
                send,
                responses,
            });
        }

        let fired_at = Instant::now();
        let order = self.ladder.take(side, tick).ok_or(Error::InvalidOrder("level not signed"))?;
        let body = Bytes::copy_from_slice(order.body().as_bytes());
        let mut responses = Vec::with_capacity(self.fan.conns().len());
        for c in self.fan.conns() {
            responses.push(c.post(&self.uri, body.clone()).await);
        }
        let send = fired_at.elapsed();
        self.stats.hits_cold += 1;
        Ok(Shot { hash: order.hash, armed: false, armed_age: None, fired_at, send, responses })
    }

    async fn arm(&mut self, side: Side, tick: u32) -> Result<Slot> {
        let order = self.ladder.get(side, tick).ok_or(Error::InvalidOrder("target level not signed"))?;
        let hash = order.hash;
        let set = self.fan.arm(&self.uri, order.body().as_bytes(), Hold::LastByte, true).await?;
        self.stats.arms += 1;
        Ok(Slot { side, tick, hash, set, armed_at: Instant::now() })
    }
}

impl Shot {
    /// Await every copy, timed from the fire instant.
    pub async fn settle(self) -> Settled {
        let fired_at = self.fired_at;
        let handles: Vec<_> = self
            .responses
            .into_iter()
            .map(|f| {
                tokio::spawn(async move {
                    let head = f?.await?;
                    let d = fired_at.elapsed();
                    Ok::<_, Error>((d, presend::read_body(head).await?))
                })
            })
            .collect();
        let mut out = Settled::default();
        for h in handles {
            match h.await {
                Ok(Ok((d, r))) => {
                    out.first = Some(out.first.map_or(d, |f| f.min(d)));
                    if (200..300).contains(&r.status) {
                        if out.accepted.is_none() {
                            out.accepted = Some((d, r));
                        }
                    } else if is_duplicate(&r) {
                        out.duplicates += 1;
                    } else {
                        out.rejected.push(r);
                    }
                }
                Ok(Err(e)) => out.errors.push(e.to_string()),
                Err(e) => out.errors.push(e.to_string()),
            }
        }
        out
    }
}

fn is_duplicate(r: &RawResponse) -> bool {
    std::str::from_utf8(&r.body).is_ok_and(|b| b.contains("create_order_duplicate_order"))
}
