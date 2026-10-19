// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Bars engine - per-(symbol, interval) bucket state machineschines
//! with market-clock alignment.
//!
//! Design: `../bars/DESIGN.md` §4.

use std::collections::HashMap;

use parking_lot::RwLock;
use trade8::contracts::{Bar, BarInterval, TradePrint};
use uuid::Uuid;

/// The open bucket state for one (symbol, interval).
#[derive(Debug, Clone, PartialEq)]
struct OpenBucket {
    interval: BarInterval,
    bucket_start_ms: u64,
    bar: Bar,
}

/// The bars engine: open-bucket state per (symbol, interval) + closed-bar
/// history (in-memory hot window; the DB pass persists on close).
#[derive(Default)]
pub struct BarsEngine {
    open: RwLock<HashMap<(Uuid, BarInterval), OpenBucket>>,
    closed: RwLock<Vec<Bar>>,
}

impl BarsEngine {
    /// Bucket start for a print at market-clock `ts_ms` (session open =
    /// bucket 0 - bars design §4: market-clock alignment, no wall-clock
    /// buckets). Here `session_open_ms` is the market session's open
    /// timestamp; prints before it clamp to bucket 0.
    pub fn bucket_start(ts_ms: u64, session_open_ms: u64, interval: BarInterval) -> u64 {
        let since_open = ts_ms.saturating_sub(session_open_ms);
        session_open_ms + (since_open / interval.duration_ms()) * interval.duration_ms()
    }

    /// Apply a print: updates the open bucket, closes it when the print
    /// belongs to a later bucket. Returns the closed bar (if any) -
    /// the `bar:closed` event source.
    pub fn apply_print(
        &self,
        p: &TradePrint,
        session_open_ms: u64,
        intervals: &[BarInterval],
    ) -> Vec<Bar> {
        let mut closed = Vec::new();
        let mut open = self.open.write();
        for &interval in intervals {
            let start = Self::bucket_start(p.venue_ts_ms, session_open_ms, interval);
            let key = (p.symbol_id, interval);
            match open.get_mut(&key) {
                Some(bucket) if bucket.bucket_start_ms == start => {
                    bucket.bar.apply(p);
                }
                Some(bucket) => {
                    // print crossed into a later bucket - close the old.
                    let finished = bucket.bar;
                    closed.push(finished);
                    let new_bar = Bar::from_first(p.clone(), interval, start);
                    *bucket = OpenBucket {
                        interval,
                        bucket_start_ms: start,
                        bar: new_bar,
                    };
                }
                None => {
                    open.insert(
                        key,
                        OpenBucket {
                            interval,
                            bucket_start_ms: start,
                            bar: Bar::from_first(p.clone(), interval, start),
                        },
                    );
                }
            }
        }
        if !closed.is_empty() {
            self.closed.write().extend(closed.iter().cloned());
        }
        closed
    }

    /// Close all open buckets at session end (EOD finalization).
    pub fn close_all(&self) -> Vec<Bar> {
        let mut open = self.open.write();
        let bars: Vec<Bar> = open.drain().map(|(_, b)| b.bar).collect();
        self.closed.write().extend(bars.iter().cloned());
        bars
    }

    /// History window read (hot window; the DB/Storage pass extends).
    pub fn history(&self, symbol_id: Uuid, interval: BarInterval) -> Vec<Bar> {
        self.closed
            .read()
            .iter()
            .filter(|b| b.symbol_id == symbol_id && b.interval == interval)
            .cloned()
            .collect()
    }

    /// Intrabar snapshot for live charts (running OHLCV of open buckets).
    pub fn intrabar(&self) -> Vec<Bar> {
        self.open.read().values().map(|b| b.bar).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trade8::fixed::Price;

    fn print(ts: u64, raw: i128, qty: u64) -> TradePrint {
        TradePrint {
            symbol_id: Uuid::new_v4(),
            symbol_name: "X".into(),
            venue_ts_ms: ts,
            price: Price::equity(raw),
            qty,
            seq: 0,
        }
    }

    const SESSION_OPEN: u64 = 34_200_000; // 09:30 market clock

    #[test]
    fn bucket_alignment() {
        // 1-minute buckets from session open
        assert_eq!(
            BarsEngine::bucket_start(SESSION_OPEN, SESSION_OPEN, BarInterval::M1),
            SESSION_OPEN
        );
        assert_eq!(
            BarsEngine::bucket_start(SESSION_OPEN + 59_999, SESSION_OPEN, BarInterval::M1),
            SESSION_OPEN
        );
        assert_eq!(
            BarsEngine::bucket_start(SESSION_OPEN + 60_000, SESSION_OPEN, BarInterval::M1),
            SESSION_OPEN + 60_000
        );
        // a print before session open clamps to bucket 0
        assert_eq!(
            BarsEngine::bucket_start(SESSION_OPEN - 1, SESSION_OPEN, BarInterval::M1),
            SESSION_OPEN
        );
    }

    #[test]
    fn bar_accumulates_then_closes_on_boundary() {
        let e = BarsEngine::default();
        let sym = Uuid::new_v4();
        let mk = |ts: u64, raw: i128, q: u64| TradePrint {
            symbol_id: sym,
            symbol_name: "X".into(),
            venue_ts_ms: ts,
            price: Price::equity(raw),
            qty: q,
            seq: 0,
        };
        // three prints inside the first minute
        let p1 = mk(SESSION_OPEN, 100_000, 10);
        e.apply_print(&p1, SESSION_OPEN, &[BarInterval::M1]);
        let p2 = mk(SESSION_OPEN + 10_000, 105_000, 5);
        e.apply_print(&p2, SESSION_OPEN, &[BarInterval::M1]);
        let p3 = mk(SESSION_OPEN + 30_000, 98_000, 20);
        e.apply_print(&p3, SESSION_OPEN, &[BarInterval::M1]);
        // intrabar shows running OHLCV
        let live = e.intrabar();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].open.raw, 100_000);
        assert_eq!(live[0].high.raw, 105_000);
        assert_eq!(live[0].low.raw, 98_000);
        assert_eq!(live[0].close.raw, 98_000);
        assert_eq!(live[0].volume, 35);
        // print in the next minute closes the first bar
        let p4 = mk(SESSION_OPEN + 61_000, 101_000, 7);
        let closed = e.apply_print(&p4, SESSION_OPEN, &[BarInterval::M1]);
        assert_eq!(closed.len(), 1);
        let b = &closed[0];
        assert_eq!(b.bucket_start_ms, SESSION_OPEN);
        assert_eq!(b.close.raw, 98_000);
        assert_eq!(b.volume, 35);
        assert_eq!(b.print_count, 3);
        // history holds it
        assert_eq!(e.history(sym, BarInterval::M1).len(), 1);
    }

    #[test]
    fn multi_interval_fanout() {
        let e = BarsEngine::default();
        let sym = Uuid::new_v4();
        let p = TradePrint {
            symbol_id: sym,
            symbol_name: "X".into(),
            venue_ts_ms: SESSION_OPEN,
            price: Price::equity(100_000),
            qty: 1,
            seq: 0,
        };
        e.apply_print(
            &p,
            SESSION_OPEN,
            &[BarInterval::S1, BarInterval::M1, BarInterval::H1],
        );
        assert_eq!(e.intrabar().len(), 3);
    }

    #[test]
    fn close_all_returns_open_bars() {
        let e = BarsEngine::default();
        let sym = Uuid::new_v4();
        let p = TradePrint {
            symbol_id: sym,
            symbol_name: "X".into(),
            venue_ts_ms: SESSION_OPEN,
            price: Price::equity(100_000),
            qty: 1,
            seq: 0,
        };
        e.apply_print(&p, SESSION_OPEN, &[BarInterval::M1]);
        let closed = e.close_all();
        assert_eq!(closed.len(), 1);
        assert!(e.intrabar().is_empty());
        let _ = print(0, 0, 0); // silence unused helper in some configs
    }

    /// Boundary property: for every interval, prints at bucket
    /// start, bucket end − 1, and bucket end land in the aligned buckets;
    /// the bucket grid is exhaustive over a full session.
    #[test]
    fn bucket_boundary_property_all_intervals() {
        let intervals = [
            BarInterval::S1,
            BarInterval::M1,
            BarInterval::M5,
            BarInterval::M15,
            BarInterval::H1,
            BarInterval::D1,
        ];
        for interval in intervals {
            let d = interval.duration_ms();
            // Start, mid, boundary−1, boundary.
            for (ts, expected) in [
                (SESSION_OPEN, SESSION_OPEN),
                (SESSION_OPEN + d / 2, SESSION_OPEN),
                (SESSION_OPEN + d - 1, SESSION_OPEN),
                (SESSION_OPEN + d, SESSION_OPEN + d),
                (SESSION_OPEN + 2 * d - 1, SESSION_OPEN + d),
                (SESSION_OPEN + 7 * d, SESSION_OPEN + 7 * d),
            ] {
                assert_eq!(
                    BarsEngine::bucket_start(ts, SESSION_OPEN, interval),
                    expected,
                    "interval {interval:?} ts {ts}"
                );
            }
            // Property: bucket start is always ≡ session open (mod d)
            // and never after ts.
            for k in 0..100u64 {
                let ts = SESSION_OPEN + k * (d / 7).max(1);
                let b = BarsEngine::bucket_start(ts, SESSION_OPEN, interval);
                assert!(b <= ts);
                assert!(b >= SESSION_OPEN);
                assert_eq!((b - SESSION_OPEN) % d, 0);
            }
        }
    }

    /// Rebuild harness: recompute bars from the print stream for
    /// any (symbol, interval) range and verify against the live rows.
    /// The engine's own history is the live truth; a fresh engine
    /// replaying the same prints must converge byte-equal.
    #[test]
    fn rebuild_harness_recomputes_from_prints() {
        let sym = Uuid::new_v4();
        let mk = |ts: u64, raw: i128, q: u64| TradePrint {
            symbol_id: sym,
            symbol_name: "X".into(),
            venue_ts_ms: ts,
            price: Price::equity(raw),
            qty: q,
            seq: 0,
        };
        // Live pass: prints across 3 minutes.
        let prints: Vec<TradePrint> = vec![
            mk(SESSION_OPEN, 100_000, 10),
            mk(SESSION_OPEN + 20_000, 102_000, 5),
            mk(SESSION_OPEN + 61_000, 101_000, 7),
            mk(SESSION_OPEN + 130_000, 99_000, 3),
            mk(SESSION_OPEN + 190_000, 103_000, 8),
        ];
        let live = BarsEngine::default();
        for p in &prints {
            live.apply_print(p, SESSION_OPEN, &[BarInterval::M1]);
        }
        // Rebuild pass: fresh engine, same prints, same anchor.
        let rebuilt = BarsEngine::default();
        for p in &prints {
            rebuilt.apply_print(p, SESSION_OPEN, &[BarInterval::M1]);
        }
        // Verification: live vs rebuilt converge (deterministic replay).
        let a = live.history(sym, BarInterval::M1);
        let b = rebuilt.history(sym, BarInterval::M1);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x, y);
        }
        // And the closed-bar invariants hold on both: volume sums, OHLC
        // from the print extremes.
        let first = &a[0];
        assert_eq!(first.open.raw, 100_000);
        assert_eq!(first.close.raw, 102_000);
        assert_eq!(first.volume, 15);
    }
}
