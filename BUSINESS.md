# BUSINESS.md: Trade8:Bars — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Trade8:Bars **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Give traders **faithful price history**: OHLCV bars at standard intervals,
  aligned to the market session (not wall-clock), built only from actual
  trade prints.
- Power the **charting experience**: current intraday candles live in
  memory; history pages from the DB — one API for both.
- Provide the **end-of-day record**: finalized daily bars, day reports, and
  (via the designed cold tier) archive objects for long-term ranges.
- Keep storage honest: bars are rebuildable from prints, so the bar store is
  a projection — no adjustment math, no manual candles.

## 2. Business Scope (What the Service Provides)

| Capability | Description |
|---|---|
| Bar aggregation | Prints → OHLCV buckets at 1s/1m/5m/15m/1h/1d, market-clock aligned |
| Live intraday view | Latest candles (running OHLCV) for charts and widgets |
| History API | Windowed bar queries (symbol + interval + from/to/limit), merged from DB + hot window |
| Interval merges | Re-aggregation from finer intervals; app derives coarser frames client-side |
| EOD finalization | Close all buckets, persist daily bars, complete the day report |
| Day reports | Prints, active symbols, completion, archive references |
| Cold tier (designed) | Monthly columnar archive objects per symbol×interval via Sentin8:Storage |
| Admin workspace | RFW pages: coverage (symbol × interval freshness), EOD reports |

### What the service does NOT own (business boundary)

| Concern | Owned by |
|---|---|
| Prints/quotes production | Trade8:Feed |
| Trading calendar/session dates | Trade8:Desk |
| Chart rendering, indicators, drawing | etrader app |
| Corporate-actions adjustments | exchange8 RefData (when deployed) |
| Cold-object storage | Sentin8:Storage |
| Scheduling | Sentin8:Cron |

## 3. Business Capabilities

| Area | Capability | Business rule |
|---|---|---|
| Aggregation | Session-aligned buckets | A print lands in the bucket containing its venue timestamp measured from the session open |
| Aggregation | Cross-bucket closes | The first print of a later bucket finalizes the previous one exactly once |
| Aggregation | Interval set | Six fixed intervals; nothing else computed server-side |
| History | Truthful merges | DB rows carry persisted candles; the engine's hot window wins for the freshest buckets; no duplicates |
| History | Entitlement-gated | History inherits the quote-reading entitlement (one scope for charts everywhere) |
| EOD | Daily bars finalized once | Daily bars record the day's final OHLCV (+vwap semantics on D1); finalize is idempotent per date |
| EOD | Day report | Prints count, active symbols, completion timestamp, archive object refs |
| Cold tier | Long ranges | Older bars archive to monthly objects; queries rehydrate the touched range (designed loop) |

## 4. Business Events (Outbound)

| Event | Fires when |
|---|---|
| `trade8:bars:bar:closed` | An interval bucket completes (closed-bar persist) |
| `trade8:bars:eod:completed` | The EOD finalize pass completes |

**Consumed:** `trade8:feed:trade:printed` (aggregation input).

## 5. Business Rules & Invariants

1. Bars contain only print-derived data — no interpolated or manual candles.
2. A candle's high never falls, low never rises, volume never shrinks
   (merge semantics make any rewrite idempotent).
3. Buckets align to the market session, not the UTC wall clock.
4. Every history query returns deduplicated, chronologically ordered bars.
5. Daily bars finalize once per trading date; retries converge.
6. Coarser frames than 1d are client-derived (e.g. 4h from 1h) — the
   service stays lean.
7. All price values remain at instrument scale (×10⁴ ints) — never floats.

## 6. Target-User Roles & Use Cases

| Persona | Use cases |
|---|---|
| Trader | Read the depth ladder's companion chart: current candle + historical window per interval |
| Quant/research | Pull symbol history for studies (windowed API) |
| Back office | Verify the day's EOD report and archive references |
| App developers | Consume `GET /v1/bars` + intrabar snapshots for chart widgets |
| Ops | Coverage board: which symbol/interval series are stale |

## 7. KPIs / Success Criteria

- Roll-up throughput: 10k prints fold within the CI latency budget
  (release-measured design number in the harness).
- Bar accuracy: bucket boundaries exact to the millisecond; no phantom
  candles for closed sessions.
- History completeness: any requested window returns every persisted bar
  (no gaps beyond missing prints), deduped.
- EOD completion on schedule each trading day; reports complete.
- Storage projection correctness: rebuild of a range from prints matches
  stored bars (property tests).

## 8. Dependencies & Reliance on Other Services

| Dependency | Why the business relies on it |
|---|---|
| Trade8:Feed | The print stream (sole aggregation input) |
| Trade8:Desk | Session calendar (anchor wiring) |
| Sentin8:Cron | EOD finalize scheduling |
| Sentin8:Storage | Cold-tier archive objects |
| Sentin8:Bus | Print delivery + bar-closed fan-out |
| Sentin8:Auth + Shield | History/EOD entitlements |
| Sentin8:DB | Bar + report persistence (schema `bars`) |

## 9. Acceptance Criteria

1. A chart can render today's candles and months of history from one API.
2. Bucket times align to the venue session as traders expect.
3. The EOD report appears finalized shortly after the scheduled close.
4. Re-running finalize never corrupts or duplicates daily bars.
5. Entitlements behave uniformly: no history access without the chart
   (quote-read) entitlement.
6. Rebuild from prints reproduces stored bars.

## 10. Open Business Questions

- Which interval set is the true production default (1m/5m/1d today vs
  the full six)?
- Official open/close policy for sessions with no prints (carry-forward
  prior close with volume 0 — confirm).
- Long-range SLA: how far back must history queries stay sub-second (cold
  rehydration)?
- Should bars emit live intrabar updates on the SSE stream (charting
  liveness) or keep the pull model?

---

**Design attribution:** trade8:bars and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
