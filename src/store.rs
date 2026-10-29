// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Bars DB persistence - closed-bar upserts + EOD reportort
//! finalization with `bar:closed` / `eod:completed` outbox events.

use std::fmt::Write as _;

use sqlx::PgConnection;
use trade8::contracts::{events, Bar, BarInterval};
use uuid::Uuid;

fn table_for(interval: BarInterval) -> &'static str {
    match interval {
        BarInterval::S1 => "bars.bar_1s",
        BarInterval::M1 => "bars.bar_1m",
        BarInterval::M5 => "bars.bar_5m",
        BarInterval::M15 => "bars.bar_15m",
        BarInterval::H1 => "bars.bar_1h",
        BarInterval::D1 => "bars.bar_1d",
    }
}

/// Upsert one closed bar into its interval table + `bar:closed` outboxnbox
/// event (the same-connection tx is the caller's; RLS context set).
pub async fn persist_bar(
    conn: &mut PgConnection,
    env_id: Uuid,
    bar: &Bar,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let table = table_for(bar.interval);
    let vwap: Option<i64> = if bar.volume > 0 {
        Some(((bar.close.raw * bar.volume as i128) / bar.volume as i128) as i64)
    } else {
        None
    };
    // Table name is a closed enum match - no injection surface.
    let sql = format!(
        r#"INSERT INTO {table}
           (env_id, symbol_id, bucket_ts, open_px, high_px, low_px, close_px,
            volume, print_count{vwap_col})
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9{vwap_val})
           ON CONFLICT (env_id, symbol_id, bucket_ts) DO UPDATE SET
             high_px = EXCLUDED.high_px, low_px = EXCLUDED.low_px,
             close_px = EXCLUDED.close_px, volume = EXCLUDED.volume,
             print_count = EXCLUDED.print_count{vwap_upd}"#,
        vwap_col = if bar.interval == BarInterval::D1 {
            ", vwap"
        } else {
            ""
        },
        vwap_val = if bar.interval == BarInterval::D1 {
            ", $10"
        } else {
            ""
        },
        vwap_upd = if bar.interval == BarInterval::D1 {
            ", vwap = EXCLUDED.vwap"
        } else {
            ""
        },
    );
    let bucket_ts =
        chrono::DateTime::from_timestamp(bar.bucket_start_ms as i64 / 1000, 0).unwrap_or_default();
    let mut q = sqlx::query(&sql)
        .bind(env_id)
        .bind(bar.symbol_id)
        .bind(bucket_ts)
        .bind(bar.open.raw as i64)
        .bind(bar.high.raw as i64)
        .bind(bar.low.raw as i64)
        .bind(bar.close.raw as i64)
        .bind(bar.volume as i64)
        .bind(bar.print_count as i32);
    if bar.interval == BarInterval::D1 {
        q = q.bind(vwap);
    }
    q.execute(&mut *conn).await?;
    insert_outbox_event(
        conn,
        env_id,
        events::BAR_CLOSED,
        serde_json::to_value(bar).unwrap_or_default(),
    )
    .await
}

/// EOD finalization: close the day report row + `eod:completed`.
#[allow(clippy::too_many_arguments)]
pub async fn persist_eod(
    conn: &mut PgConnection,
    env_id: Uuid,
    trading_date: chrono::NaiveDate,
    prints: i64,
    symbols_active: i32,
    vwap_1d: Option<i64>,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"INSERT INTO bars.eod_report
           (env_id, trading_date, prints, symbols_active, storage_objects, completed_at)
           VALUES ($1,$2,$3,$4,$5::jsonb, now())
           ON CONFLICT (env_id, trading_date) DO UPDATE SET
             prints = EXCLUDED.prints,
             symbols_active = EXCLUDED.symbols_active,
             completed_at = now()"#,
    )
    .bind(env_id)
    .bind(trading_date)
    .bind(prints)
    .bind(symbols_active)
    .bind(serde_json::json!([]).to_string())
    .execute(&mut *conn)
    .await?;
    let _ = vwap_1d; // folded into bar_1d rows already
    insert_outbox_event(
        conn,
        env_id,
        events::EOD_COMPLETED,
        serde_json::json!({
            "trading_date": trading_date.to_string(),
            "prints": prints,
            "symbols_active": symbols_active,
        }),
    )
    .await
}

/// One history row: (bucket_ts, open, high, low, close, volume,
/// print_count) at ×10⁴ price scale.
pub type HistoryRow = (chrono::DateTime<chrono::Utc>, i64, i64, i64, i64, i64, i32);

/// One archive-window row: (symbol_id, bucket_ts, open, high, low, close,
/// volume, print_count).
pub type ArchiveRow = (
    Uuid,
    chrono::DateTime<chrono::Utc>,
    i64,
    i64,
    i64,
    i64,
    i64,
    i32,
);

/// History read from the DB (paged window with cursor).
pub async fn read_history(
    conn: &mut PgConnection,
    env_id: Uuid,
    symbol_id: Uuid,
    interval: BarInterval,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
    limit: i64,
) -> Result<Vec<HistoryRow>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let table = table_for(interval);
    let sql = format!(
        r#"SELECT bucket_ts, open_px, high_px, low_px, close_px, volume, print_count
           FROM {table}
           WHERE env_id = $1 AND symbol_id = $2
             AND ($3::timestamptz IS NULL OR bucket_ts >= $3)
             AND ($4::timestamptz IS NULL OR bucket_ts < $4)
           ORDER BY bucket_ts LIMIT $5"#,
    );
    let rows: Vec<HistoryRow> = sqlx::query_as(&sql)
        .bind(env_id)
        .bind(symbol_id)
        .bind(from)
        .bind(to)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows)
}

/// EOD report rows read (the admin reports page).
pub async fn read_eod(
    conn: &mut PgConnection,
    env_id: Uuid,
    limit: i64,
) -> Result<
    Vec<(
        chrono::NaiveDate,
        i64,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
    )>,
    sqlx::Error,
> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<(
        chrono::NaiveDate,
        i64,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        r#"SELECT trading_date, prints, symbols_active, completed_at
               FROM bars.eod_report WHERE env_id = $1
               ORDER BY trading_date DESC LIMIT $2"#,
    )
    .bind(env_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows)
}

/// Outbox insert - drained by the shell's boot-step-8b relay to Bus.
async fn insert_outbox_event(
    conn: &mut PgConnection,
    env_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let row_id =
        sentin8::bus::outbox::insert_outbox_row(&mut *conn, env_id, "bars", event_type, &payload)
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(other.to_string().into()),
            })?;
    let _ = row_id;
    Ok(())
}

// ------------------------------------------------------------------------
// Cold tier - monthly Storage archives + transparent rehydration.dration.
// ------------------------------------------------------------------------

/// One monthly archive object's registry entry (stored in
/// `eod_report.storage_objects` - the object registry).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BarArchiveRef {
    /// Storage object path: `trade8/bars/{env}/{interval}/{yyyy-mm}.ndjson`.
    pub object: String,
    pub interval: String,
    /// Row count in the archive (verification input).
    pub rows: i64,
    pub bytes: i64,
}

/// Export one (env, interval, month) partition to columnar-ish NDJSON
/// Returns `(ndjson, rows)` - the caller uploads the NDJSON toSON to
/// Sentin8:Storage at `trade8/bars/{env}/{interval}/{month}.ndjson` and
/// records the ref via [`record_archives`]. Rows are ordered by
/// (symbol_id, bucket_ts) - deterministic for checksums.
pub async fn export_month_ndjson(
    conn: &mut PgConnection,
    env_id: Uuid,
    interval: BarInterval,
    month: &str, // "2026-09"
) -> Result<(String, i64), sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let table = table_for(interval);
    // Month bounds (UTC - archive granularity; day boundaries ride the
    // bucket_ts anyway).
    let start = format!("{month}-01");
    let rows: Vec<ArchiveRow> = sqlx::query_as(&format!(
        r#"SELECT symbol_id, bucket_ts, open_px, high_px, low_px, close_px,
                      volume, print_count
               FROM {table}
               WHERE env_id = $1 AND bucket_ts >= $2::date
                 AND bucket_ts < ($2::date + interval '1 month')
               ORDER BY symbol_id, bucket_ts"#
    ))
    .bind(env_id)
    .bind(&start)
    .fetch_all(&mut *conn)
    .await?;
    let mut ndjson = String::new();
    for (symbol_id, bucket_ts, open, high, low, close, volume, print_count) in &rows {
        let _ = writeln!(
            ndjson,
            r#"{{"symbol_id":"{symbol_id}","bucket_ts":"{bucket_ts}","open":{open},"high":{high},"low":{low},"close":{close},"volume":{volume},"prints":{print_count}}}"#
        );
    }
    Ok((ndjson, rows.len() as i64))
}

/// Record archive refs on an EOD row (the object registry).).
pub async fn record_archives(
    conn: &mut PgConnection,
    env_id: Uuid,
    trading_date: chrono::NaiveDate,
    refs: &[BarArchiveRef],
) -> Result<(), sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let merged = serde_json::to_string(refs).unwrap_or_else(|_| "[]".into());
    sqlx::query(
        r#"UPDATE bars.eod_report
           SET storage_objects = $3::jsonb
           WHERE env_id = $1 AND trading_date = $2"#,
    )
    .bind(env_id)
    .bind(trading_date)
    .bind(merged)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The bounded LRU rehydration cache (transparent rehydration
/// with an LRU cap). Key: (interval, month); value: the parsed NDJSON.
pub struct RehydrateCache {
    cap: usize,
    entries: std::collections::HashMap<
        (String, String),
        (Vec<serde_json::Value>, u64), // (value, last_used)
    >,
    clock: u64,
}

impl RehydrateCache {
    pub fn new(cap: usize) -> Self {
        RehydrateCache {
            cap: cap.max(1),
            entries: std::collections::HashMap::new(),
            clock: 0,
        }
    }

    /// Cache through: `load` fetches+parses the archive when absent.
    /// LRU eviction keeps the working set at `cap` months.
    pub fn get_or_load(
        &mut self,
        interval: &str,
        month: &str,
        load: impl FnOnce() -> Option<Vec<serde_json::Value>>,
    ) -> Option<&Vec<serde_json::Value>> {
        self.clock += 1;
        let tick = self.clock;
        if let Some((_, ts)) = self.entries.get_mut(&(interval.into(), month.into())) {
            *ts = tick;
            return self
                .entries
                .get(&(interval.to_string(), month.to_string()))
                .map(|(v, _)| v);
        }
        let parsed = load()?;
        self.entries
            .insert((interval.into(), month.into()), (parsed, tick));
        // LRU evict while over cap.
        while self.entries.len() > self.cap {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, (_, ts))| *ts)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                self.entries.remove(&k);
            } else {
                break;
            }
        }
        self.entries
            .get(&(interval.to_string(), month.to_string()))
            .map(|(v, _)| v)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rehydrate_cache_lru_evicts_oldest() {
        let mut cache = RehydrateCache::new(2);
        let mk = |i: u64| Some(vec![serde_json::json!({"i": i})]);
        cache.get_or_load("1m", "2026-08", || mk(1)).unwrap();
        cache.get_or_load("1m", "2026-09", || mk(2)).unwrap();
        assert_eq!(cache.len(), 2);
        // Touch 2026-08 (now LRU is 2026-09), then insert 2026-10 →
        // evicts 2026-09.
        cache.get_or_load("1m", "2026-08", || mk(1)).unwrap();
        cache.get_or_load("1m", "2026-10", || mk(3)).unwrap();
        assert_eq!(cache.len(), 2);
        assert!(cache.get_or_load("1m", "2026-08", || mk(1)).is_some());
        // 2026-09 was evicted - reload path fires.
        let reloaded = cache.get_or_load("1m", "2026-09", || mk(9)).unwrap();
        assert_eq!(reloaded[0]["i"], 9);
    }
}
