// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Bars service binary — OHLCV aggregation + history + EOD.
//!
//! Design: `../bars/DESIGN.md`.
//! Closed bars upsert into the interval tables + `bar:closed` outbox
//! events; history reads the DB with the engine's hot window as fallback;
//! EOD finalization persists the day report. The print consumers (feed
//! broadcast + bus group) drive the buckets.

#![allow(dead_code)]

mod admin_ui;
mod core;
mod openapi;
mod registration;
mod store;

use sqlx::Acquire;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use trade8::contracts::BarInterval;

/// Service state.
pub struct BarsState {
    pub core: sentin8::shell::ServiceCore,
    pub engine: core::BarsEngine,
    /// Feed fan-out subscription (prints drive the buckets).
    pub tx: tokio::sync::broadcast::Sender<String>,
}

fn extend_router(app: Router<Arc<BarsState>>, _state: Arc<BarsState>) -> Router<Arc<BarsState>> {
    app.route("/v1/bars", get(query_bars))
        .route("/v1/bars/latest", get(latest_bars))
        .route("/v1/eod", get(list_eod).post(finalize_eod))
        // Admin RFW pages.
        .route("/v1/admin/ui/{*page}", get(admin_ui::get_admin_ui))
        // ── Shield policy-admin API (shared
        // implementation; handlers generic over this state via HasPolicyAdmin).
        .route(
            "/v1/admin/policy-sets",
            get(sentin8::shield::admin::list_policy_sets::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            get(sentin8::shield::admin::get_policy_set::<Arc<BarsState>>)
                .put(sentin8::shield::admin::put_policy_set::<Arc<BarsState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            post(sentin8::shield::admin::test_policy_set::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            post(sentin8::shield::admin::admin_enforce_policy_set::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            post(sentin8::shield::admin::admin_shadow_policy_set::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            post(sentin8::shield::admin::admin_disable_policy_set::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/relations",
            get(sentin8::shield::admin::list_relations::<Arc<BarsState>>)
                .put(sentin8::shield::admin::put_relation::<Arc<BarsState>>)
                .delete(sentin8::shield::admin::delete_relation::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/relations/check",
            get(sentin8::shield::admin::check_relation::<Arc<BarsState>>),
        )
        .route(
            "/v1/admin/attributes",
            get(sentin8::shield::admin::list_attributes::<Arc<BarsState>>)
                .put(sentin8::shield::admin::put_attribute::<Arc<BarsState>>)
                .delete(sentin8::shield::admin::delete_attribute::<Arc<BarsState>>),
        )
        .route("/v1/admin/coverage", get(admin_ui::admin_coverage))
        .route("/v1/admin/eod", get(admin_ui::admin_eod))
        .route(
            "/v1/health",
            // Carries the binary's build identity (`version`/`build`,
            // BUILDID-1) so captain's monitor populates `make status`'s
            // VERSION column — the consuming BINARY's identity.
            get(|| async { Json(serde_json::json!({"status": "ok", "service": "trade8:bars", "version": sentin8::build_info::id(), "build": sentin8::build_info::build_hash()})) }),
        )
}

fn parse_interval(s: &str) -> Option<BarInterval> {
    Some(match s {
        "1s" => BarInterval::S1,
        "1m" => BarInterval::M1,
        "5m" => BarInterval::M5,
        "15m" => BarInterval::M15,
        "1h" => BarInterval::H1,
        "1d" => BarInterval::D1,
        _ => return None,
    })
}
#[utoipa::path(
    get,
    path = "/v1/bars",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "bars",
)]

/// History: DB window (source of truth), engine hot window
/// as fallback; merged + deduped on bucket start.
pub(crate) async fn query_bars(
    State(st): State<Arc<BarsState>>,
    headers: axum::http::HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:history:read"),
    )
    .await?;
    let env = a.env_id();
    let interval = q.get("interval").and_then(|s| parse_interval(s)).ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "interval required (1s|1m|5m|15m|1h|1d)".into(),
    ))?;
    let symbol = q
        .get("symbol")
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or((
            axum::http::StatusCode::BAD_REQUEST,
            "symbol (uuid) required".into(),
        ))?;
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(500)
        .min(5000);
    let from = q
        .get("from")
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    let to = q
        .get("to")
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
        .map(|d| d.with_timezone(&chrono::Utc));
    // Hot window (live bars) — always fresh in the engine.
    let hot = st.engine.history(symbol, interval);
    let mut bars: Vec<serde_json::Value> = hot
        .iter()
        .map(|b| serde_json::to_value(b).unwrap_or_default())
        .collect();
    // DB window (persisted history).
    if let Ok(mut conn) = st.core.pool.acquire().await {
        sentin8::shell::set_tenant_context(&mut conn, env).await;
        // RLS context is transaction-local (DESIGN.md §5) —
        // begin() pins the tenant context to the SAME tx as the DML/reads.
        let mut tx = match conn.begin().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "bars: tenant tx begin failed — retry next tick");
                return Err(sentin8::shell::err_db(e));
            }
        };
        if let Ok(rows) = store::read_history(&mut tx, env, symbol, interval, from, to, limit).await
        {
            for (ts, o, h, l, c, vol, pc) in rows {
                let start_ms = ts.timestamp_millis().max(0) as u64;
                // Skip buckets already covered by the hot window.
                if hot.iter().any(|b| b.bucket_start_ms == start_ms) {
                    continue;
                }
                bars.push(serde_json::json!({
                    "symbol_id": symbol, "interval": interval.as_str(),
                    "bucket_start_ms": start_ms,
                    "open": {"raw": o, "scale": 4},
                    "high": {"raw": h, "scale": 4},
                    "low": {"raw": l, "scale": 4},
                    "close": {"raw": c, "scale": 4},
                    "volume": vol, "print_count": pc,
                }));
            }
        }
    }
    bars.sort_by_key(|b| {
        b.get("bucket_start_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    });
    let count = bars.len();
    Ok(Json(serde_json::json!({"bars": bars, "count": count})))
}
#[utoipa::path(
    get,
    path = "/v1/bars/latest",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "bars",
)]

pub(crate) async fn latest_bars(
    State(st): State<Arc<BarsState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:history:read"),
    )
    .await?;
    Ok(Json(serde_json::json!({"intrabar": st.engine.intrabar()})))
}

/// EOD report rows (the API read; the admin page shares the query).
pub(crate) async fn list_eod(
    State(st): State<Arc<BarsState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:report:eod:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "bars: tenant tx begin failed — retry next tick");
            return Err(sentin8::shell::err_db(e));
        }
    };
    let rows = store::read_eod(&mut tx, env, 50)
        .await
        .map_err(sentin8::shell::err_db)?;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(serde_json::json!({"eod": rows.into_iter().map(
        |(trading_date, prints, symbols_active, completed_at)| {
            serde_json::json!({
                "trading_date": trading_date, "prints": prints,
                "symbols_active": symbols_active, "completed_at": completed_at,
            })
        }
    ).collect::<Vec<_>>()})))
}
#[utoipa::path(
    post,
    path = "/v1/eod",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "bars",
)]

/// EOD finalization (the Cron proxy-back target): close all open
/// buckets, persist them + the day report, emit `eod:completed`.
pub(crate) async fn finalize_eod(
    State(st): State<Arc<BarsState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<serde_json::Value>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    // The Cron EOD proxy-back. Step 5 — Shield
    // decides (the mirror permits the eod-read action for cron-run scopes).
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:report:eod:read"),
    )
    .await?;
    let env = a.env_id();
    let date = body
        .as_ref()
        .and_then(|Json(b)| b.get("date"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    // Close every open bucket (carry-forward policy is per-config).
    let closed = st.engine.close_all();
    let mut persisted = 0usize;
    let mut prints = 0i64;
    if let Ok(mut conn) = st.core.pool.acquire().await {
        sentin8::shell::set_tenant_context(&mut conn, env).await;
        // RLS context is transaction-local (DESIGN.md §5) —
        // begin() pins the tenant context to the SAME tx as the DML/reads.
        let mut tx = match conn.begin().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "bars: tenant tx begin failed — retry next tick");
                return Err(sentin8::shell::err_db(e));
            }
        };
        for bar in &closed {
            if store::persist_bar(&mut tx, env, bar).await.is_ok() {
                persisted += 1;
                prints += bar.volume as i64;
            }
        }
        let symbols_active = closed
            .iter()
            .map(|b| b.symbol_id)
            .collect::<std::collections::HashSet<_>>()
            .len() as i32;
        let _ = store::persist_eod(&mut tx, env, date, prints, symbols_active, None).await;
        tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    }
    tracing::info!(date = %date, closed = closed.len(), persisted, "EOD finalized");
    Ok(Json(serde_json::json!({
        "closed": closed.len(), "persisted": persisted, "date": date,
    })))
}

/// The print consumer (demo path): subscribes to the feed broadcast and rolls
/// prints into the configured intervals; closed bars persist immediately.
async fn run_print_consumer(
    state: Arc<BarsState>,
    env_id: uuid::Uuid,
    intervals: Vec<BarInterval>,
) {
    let mut rx = state.tx.subscribe();
    loop {
        match rx.recv().await {
            Ok(msg) => {
                let Ok(print) = serde_json::from_str::<trade8::contracts::TradePrint>(&msg) else {
                    continue;
                };
                // Market clock: prints carry venue timestamps; the session
                // open anchors bucket 0 (per-day Cron task resets it).
                let session_open_ms = print.venue_ts_ms; // simplified anchor
                let closed = state
                    .engine
                    .apply_print(&print, session_open_ms, &intervals);
                if !closed.is_empty() {
                    if let Ok(mut conn) = state.core.pool.acquire().await {
                        sentin8::shell::set_tenant_context(&mut conn, env_id).await;
                        // RLS context is transaction-local (DESIGN.md §5) —
                        // begin() pins the tenant context to the SAME tx as the DML/reads.
                        let mut tx = match conn.begin().await {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::warn!(error = %e, "bars: persist tx begin failed — retry next tick");
                                return;
                            }
                        };
                        for bar in &closed {
                            if let Err(e) = store::persist_bar(&mut tx, env_id, bar).await {
                                tracing::warn!(error = %e, "bar persist failed");
                            }
                        }
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(lagged = n, "bars print consumer lagged");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("trade8:bars", "TRADE8_BARS")
            .with_registration(registration::print)
            .with_own_health()
            .with_state(|core| async move {
                let engine = core::BarsEngine::default();
                let (tx, _) = tokio::sync::broadcast::channel(1024);
                let state = Arc::new(BarsState { core, engine, tx });
                // Boot pass: default interval set (1m/5m/1d — the hot
                // consumer; configurable via TRADE8_BARS_INTERVALS later).
                let intervals = vec![BarInterval::M1, BarInterval::M5, BarInterval::D1];
                if let Ok(env_slug) = std::env::var("TRADE8_BARS_ENV") {
                    if let Ok(env_id) = uuid::Uuid::parse_str(&env_slug) {
                        let st2 = state.clone();
                        tokio::spawn(async move {
                            run_print_consumer(st2, env_id, intervals).await;
                        });
                    }
                }
                // Bus print consumer (the platform feed family with
                // the trade8-bars group): prints from the Bus relay, same
                // roll-up + persist path as the broadcast consumer.
                {
                    let st2 = state.clone();
                    tokio::spawn(async move {
                        // Cert-presenting Bus client (CERTFREE-4V): the
                        // `trade8:bars` workload CERTIFICATE is the
                        // identity — no DCR, no token; the mint retries
                        // while the fleet boots.
                        let bus = loop {
                            match sentin8::bus::client::BusClient::for_workload(
                                &st2.core.config.sentin8_bus_url,
                                &st2.core.config.sentin8_auth_issuer,
                                "trade8:bars",
                            )
                            .await
                            {
                                Ok(c) => break c,
                                Err(e) => {
                                    tracing::debug!(error = %e, "bars bus client: certificate not ready yet");
                                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                }
                            }
                        };
                        let bus = std::sync::Arc::new(bus);
                        let st3 = st2.clone();
                        trade8::client::run_consumer(
                            bus,
                            "feed",
                            "trade8:bars",
                            move |msg: trade8::client::BusMessage| {
                                let st = st3.clone();
                                async move {
                                    if msg.event_type != trade8::contracts::events::TRADE_PRINTED {
                                        return trade8::client::HandleOutcome::Ack;
                                    }
                                    // Envelope payload carries the print.
                                    let Ok(print) =
                                        serde_json::from_value::<trade8::contracts::TradePrint>(
                                            msg.payload.clone(),
                                        )
                                    else {
                                        return trade8::client::HandleOutcome::Ack;
                                    };
                                    let session_open_ms = print.venue_ts_ms;
                                    let intervals =
                                        [BarInterval::M1, BarInterval::M5, BarInterval::D1];
                                    let closed =
                                        st.engine.apply_print(&print, session_open_ms, &intervals);
                                    if closed.is_empty() {
                                        return trade8::client::HandleOutcome::Ack;
                                    }
                                    // Persist closed bars + bar:closed outbox events.
                                    let Ok(env_id) = uuid::Uuid::parse_str(&msg.env_id) else {
                                        return trade8::client::HandleOutcome::Ack;
                                    };
                                    let Ok(mut conn) = st.core.pool.acquire().await else {
                                        return trade8::client::HandleOutcome::Nack;
                                    };
                                    sentin8::shell::set_tenant_context(&mut conn, env_id).await;
                                    // RLS context is transaction-local (DESIGN.md §5) —
                                    // begin() pins the tenant context to the SAME tx as the DML/reads.
                                    let mut tx = match conn.begin().await {
                                        Ok(t) => t,
                                        Err(e) => { tracing::warn!(error = %e, "bars: consumer tx begin failed — nack"); return trade8::client::HandleOutcome::Nack; }
                                    };
                                    for bar in &closed {
                                        if store::persist_bar(&mut tx, env_id, bar).await.is_err()
                                        {
                                            return trade8::client::HandleOutcome::Nack;
                                        }
                                    }
                                    trade8::client::HandleOutcome::Ack
                                }
                            },
                        )
                        .await;
                    });
                }
                // Cron EOD finalization: then): the
                // 16:15 weekday schedule proxy-calls `/v1/eod`.
                if let Ok(cron_url) = std::env::var("SENTIN8_CRON_URL") {
                    if !cron_url.is_empty() {
                        trade8::client::cron::register_defaults(
                            &state.core.config.sentin8_auth_issuer,
                            &cron_url,
                            "trade8:bars",
                            state.core.config.port,
                        )
                        .await;
                    }
                }
                Ok(state)
            })
            .with_router(extend_router),
    )
    .await
}

// ---------------------------------------------------------------------------
// Latency harness — bars roll-up budget (10k prints < 500ms CI
// headroom; the 100ms design number is measured release-mode).
// ---------------------------------------------------------------------------

/// Shared policy-admin surface accessor: the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache.
impl sentin8::shield::admin::HasPolicyAdmin for BarsState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}
#[cfg(test)]
mod latency {
    use super::*;
    use std::time::Instant;
    use trade8::contracts::TradePrint;

    #[test]
    fn bars_rollup_latency() {
        let e = core::BarsEngine::default();
        let sym = uuid::Uuid::new_v4();
        let intervals = [BarInterval::M1, BarInterval::M5, BarInterval::D1];
        let start = Instant::now();
        for i in 0..10_000u64 {
            let p = TradePrint {
                symbol_id: sym,
                symbol_name: "AAPL".into(),
                price: trade8::fixed::Price::equity(100_000 + (i % 50) as i128 * 100),
                qty: 100,
                seq: i,
                venue_ts_ms: 1_000_000 + i,
            };
            let _ = e.apply_print(&p, 1_000_000, &intervals);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 500,
            "10k-print roll-up took {:?}",
            elapsed
        );
        println!("bars roll-up: 10k prints, {}ms", elapsed.as_millis());
    }
}
