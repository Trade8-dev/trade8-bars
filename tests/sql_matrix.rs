// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Bars SQL integration tests (test-architecture-design §5.5).
//!
//! Real `trade8_bars_service` NOBYPASSRLS role: bar upsert/merge across
//! intervals (incl. the D1 vwap column), history window reads, EOD
//! report + archive refs, NDJSON export shape, and the RLS matrix.
//!
//! Design: `../../DESIGN.md §9 (testing)` §5.5.

#[path = "../src/store.rs"]
#[allow(dead_code)]
mod store;

use sqlx::postgres::PgPoolOptions;
use sqlx::Acquire;
use trade8::contracts::{events, Bar, BarInterval};
use trade8::fixed::Price;
use uuid::Uuid;

const BARS_URL: &str = "postgresql://trade8_bars_service@localhost:5432/trade8";
const PROBE_URL: &str = "postgresql://trade8_probe@localhost:5432/trade8";

fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}
fn env_b() -> Uuid {
    Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
}

async fn bars_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(BARS_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn probe_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(PROBE_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

fn bar(
    interval: BarInterval,
    symbol_id: Uuid,
    bucket_ms: u64,
    close_raw: i128,
    volume: u64,
) -> Bar {
    Bar {
        symbol_id,
        interval,
        bucket_start_ms: bucket_ms,
        open: Price::equity(close_raw - 100),
        high: Price::equity(close_raw + 200),
        low: Price::equity(close_raw - 200),
        close: Price::equity(close_raw),
        volume,
        print_count: 42,
    }
}

/// Test bucket timestamps are pinned to 2026-09-01 so cleanup is bounded.
const BUCKET_1: u64 = 1_788_480_000_000; // 2026-09-01T00:00:00Z (approx)
const BUCKET_2: u64 = BUCKET_1 + 60_000;
const BUCKET_3: u64 = BUCKET_1 + 120_000;

async fn cleanup(conn: &mut sqlx::PgConnection, env: Uuid, symbol_id: Uuid) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    for t in ["bar_1s", "bar_1m", "bar_5m", "bar_15m", "bar_1h", "bar_1d"] {
        let _ = sqlx::query(&format!(
            "DELETE FROM bars.{t} WHERE env_id=$1 AND symbol_id=$2"
        ))
        .bind(env)
        .bind(symbol_id)
        .execute(&mut *tx)
        .await;
    }
    let _ = sqlx::query(
        r#"DELETE FROM bars.eod_report WHERE env_id=$1 AND trading_date = '2026-09-01'"#,
    )
    .bind(env)
    .execute(&mut *tx)
    .await;
    let _ = sqlx::query(r#"DELETE FROM bars.outbox WHERE env_id=$1 AND stream_family='bars' AND created_at > now() - interval '1 minute'"#)
        .bind(env)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
}

// ------------------------------------------------------------------------
// RLS matrix
// ------------------------------------------------------------------------

#[tokio::test]
async fn rls_matrix_no_context_returns_zero() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM bars.bar_1m")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(n, 0, "no tenant context must see zero rows");
}

#[tokio::test]
async fn rls_matrix_cross_env_returns_zero() {
    let Some(mut conn) = bars_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let sym = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_bar(
            &mut tx,
            env_a(),
            &bar(BarInterval::M1, sym, BUCKET_1, 100_500, 100),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_b()).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM bars.bar_1m WHERE symbol_id = $1")
        .bind(sym)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(n, 0, "env-b must not see env-a's bars");
    cleanup(&mut conn, env_a(), sym).await;
}

// ------------------------------------------------------------------------
// Bar upsert across intervals (design §5.5)
// ------------------------------------------------------------------------

#[tokio::test]
async fn persist_bar_upserts_all_intervals() {
    let Some(mut conn) = bars_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let sym = Uuid::new_v4();
    let intervals = [
        (BarInterval::S1, "bar_1s"),
        (BarInterval::M1, "bar_1m"),
        (BarInterval::M5, "bar_5m"),
        (BarInterval::M15, "bar_15m"),
        (BarInterval::H1, "bar_1h"),
        (BarInterval::D1, "bar_1d"),
    ];
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        for (iv, _) in intervals {
            store::persist_bar(&mut tx, env_a(), &bar(iv, sym, BUCKET_1, 100_500, 100))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
    }
    for (_, table) in intervals {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let rows: Vec<(i64, i64)> = sqlx::query_as(&format!(
            r#"SELECT close_px, volume FROM bars.{table}
               WHERE env_id=$1 AND symbol_id=$2"#,
        ))
        .bind(env_a())
        .bind(sym)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1, "{table} must hold exactly one bar");
        assert_eq!(rows[0].0, 100_500);
        assert_eq!(rows[0].1, 100);
        tx.commit().await.unwrap();
    }
    // The D1 row carries vwap (close × volume / volume = close here).
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let vwap: Option<i64> =
        sqlx::query_scalar("SELECT vwap FROM bars.bar_1d WHERE env_id=$1 AND symbol_id=$2")
            .bind(env_a())
            .bind(sym)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(vwap, Some(100_500));
    tx.commit().await.unwrap();

    // Merge semantics: re-persisting the same bucket updates high/low/
    // close - no duplicate row.
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_bar(
            &mut tx,
            env_a(),
            &bar(BarInterval::M1, sym, BUCKET_1, 101_000, 150),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let rows: Vec<(i64, i64, i64)> = sqlx::query_as(
        r#"SELECT close_px, high_px, volume FROM bars.bar_1m WHERE env_id=$1 AND symbol_id=$2"#,
    )
    .bind(env_a())
    .bind(sym)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "same bucket must merge, not duplicate");
    assert_eq!(rows[0].0, 101_000);
    assert_eq!(rows[0].1, 101_200);
    assert_eq!(rows[0].2, 150);
    tx.commit().await.unwrap();
    cleanup(&mut conn, env_a(), sym).await;
}

#[tokio::test]
async fn history_read_window() {
    let Some(mut conn) = bars_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let sym = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        for (b, c) in [
            (BUCKET_1, 100_100),
            (BUCKET_2, 100_200),
            (BUCKET_3, 100_300),
        ] {
            store::persist_bar(&mut tx, env_a(), &bar(BarInterval::M1, sym, b, c, 10))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
    }
    let rows = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::read_history(
            &mut tx,
            env_a(),
            sym,
            BarInterval::M1,
            Some(chrono::DateTime::from_timestamp(BUCKET_2 as i64 / 1000, 0).unwrap()),
            None,
            10,
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    // The history row is (ts, open, high, low, close, volume,
    // print_count) - the close rides index 4.
    assert_eq!(rows.len(), 2, "from-filter excludes the first bucket");
    assert_eq!(rows[0].4, 100_200, "close of the from-bucket");
    assert_eq!(rows[1].4, 100_300);
    let limited = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::read_history(&mut tx, env_a(), sym, BarInterval::M1, None, None, 1)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].4, 100_100, "limit takes the earliest buckets");
    cleanup(&mut conn, env_a(), sym).await;
}

#[tokio::test]
async fn eod_report_and_archive_refs() {
    let Some(mut conn) = bars_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let date = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_eod(&mut tx, env_a(), date, 1234, 7, Some(100_500))
            .await
            .unwrap();
        store::record_archives(
            &mut tx,
            env_a(),
            date,
            &[store::BarArchiveRef {
                object: format!("trade8/bars/{}/1m/2026-09.ndjson", env_a()),
                interval: "1m".into(),
                rows: 2,
                bytes: 256,
            }],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let reports = store::read_eod(&mut tx, env_a(), 10).await.unwrap();
    let mine = reports.iter().find(|r| r.0 == date).expect("eod row");
    assert_eq!(mine.1, 1234);
    assert_eq!(mine.2, 7);
    let objects: serde_json::Value = sqlx::query_scalar(
        r#"SELECT storage_objects FROM bars.eod_report WHERE env_id=$1 AND trading_date=$2"#,
    )
    .bind(env_a())
    .bind(date)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let refs: Vec<store::BarArchiveRef> = serde_json::from_value(objects).unwrap();
    assert_eq!(refs.len(), 1, "archive ref must round-trip through JSONB");
    assert_eq!(refs[0].rows, 2);
    let events_: i64 =
        sqlx::query_scalar(r#"SELECT count(*) FROM bars.outbox WHERE env_id=$1 AND event_type=$2"#)
            .bind(env_a())
            .bind(events::EOD_COMPLETED)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert!(events_ >= 1, "EOD completion must emit the outbox event");
    tx.commit().await.unwrap();
    cleanup(&mut conn, env_a(), Uuid::new_v4()).await;
}

#[tokio::test]
async fn export_month_ndjson_shape() {
    let Some(mut conn) = bars_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let sym = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_bar(
            &mut tx,
            env_a(),
            &bar(BarInterval::M1, sym, BUCKET_1, 100_100, 10),
        )
        .await
        .unwrap();
        store::persist_bar(
            &mut tx,
            env_a(),
            &bar(BarInterval::M1, sym, BUCKET_2, 100_200, 20),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let (ndjson, rows) = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::export_month_ndjson(&mut tx, env_a(), BarInterval::M1, "2026-09")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert!(rows >= 2);
    let lines: Vec<&str> = ndjson.lines().collect();
    // Sorted by (symbol_id, bucket_ts); each line parses back.
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert!(v["symbol_id"].is_string());
        assert!(v["close"].is_i64());
    }
    cleanup(&mut conn, env_a(), sym).await;
}
