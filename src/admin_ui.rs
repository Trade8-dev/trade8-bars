// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Bars admin UI — RFW pages + data endpoints.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use sentin8::rfw::page::AdminPage;
use sentin8::rfw::response::AdminUiResponse;
use sentin8::rfw::widget::*;

use crate::BarsState;

fn cols(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: true,
        width: None,
        format: None,
    }
}
fn col(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: false,
        width: None,
        format: None,
    }
}
fn table(columns: Vec<WidgetDataTableColumn>, endpoint: &str) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 25,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions: vec![],
    })
}
fn card(title: &str, body: &str) -> RemoteWidget {
    RemoteWidget::Card(WidgetCard {
        title: Some(title.into()),
        subtitle: None,
        children: vec![RemoteWidget::Text(WidgetText {
            text: body.into(),
            text_type: WidgetTextType::Body,
            align: TextAlign::Left,
            max_lines: None,
        })],
        actions: vec![],
    })
}
#[utoipa::path(
    get,
    path = "/v1/admin/ui/{page}",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/ui/{page}` — RFW definitions.
pub async fn get_admin_ui(
    State(_st): State<std::sync::Arc<BarsState>>,
    headers: HeaderMap,
    Path(page): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&_st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &_st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:history:read"),
    )
    .await?;
    let def = match page.as_str() {
        "coverage" => AdminPage::new("Bars Coverage")
            .widget(card(
                "Coverage",
                "Per-symbol bucket coverage and lag from the engine's live state.",
            ))
            .widget(table(
                vec![
                    cols("symbol_id", "Symbol"),
                    cols("interval", "Interval"),
                    cols("buckets", "Buckets"),
                    col("latest_close", "Latest Close"),
                ],
                "/v1/admin/coverage",
            ))
            .build(),
        "eod" => AdminPage::new("EOD Reports")
            .widget(table(
                vec![
                    cols("trading_date", "Date"),
                    cols("prints", "Prints"),
                    cols("symbols_active", "Symbols Active"),
                    col("completed_at", "Completed"),
                ],
                "/v1/admin/eod",
            ))
            .build(),
        _ => {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("Unknown admin UI page: {page}"),
            ))
        }
    };
    Ok(Json(
        serde_json::to_value(AdminUiResponse::page(def)).unwrap_or_default(),
    ))
}
#[utoipa::path(
    get,
    path = "/v1/admin/coverage",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/coverage` — engine intrabar + closed-window coverage.
pub async fn admin_coverage(
    State(st): State<std::sync::Arc<BarsState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:history:read"),
    )
    .await?;
    let intrabar = st.engine.intrabar();
    let rows: Vec<_> = intrabar
        .iter()
        .map(|b| {
            serde_json::json!({
                "symbol_id": b.symbol_id,
                "interval": b.interval.as_str(),
                "buckets": 1, // open bucket (live)
                "latest_close": b.close.raw,
            })
        })
        .collect();
    Ok(Json(serde_json::json!(rows)))
}
#[utoipa::path(
    get,
    path = "/v1/admin/eod",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/eod` — EOD report rows from the DB.
pub async fn admin_eod(
    State(st): State<std::sync::Arc<BarsState>>,
    headers: HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
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
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50)
        .min(365);
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let rows = crate::store::read_eod(&mut conn, env, limit)
        .await
        .map_err(sentin8::shell::err_db)?;
    Ok(Json(serde_json::json!(rows
        .into_iter()
        .map(|(trading_date, prints, symbols_active, completed_at)| {
            serde_json::json!({
                "trading_date": trading_date, "prints": prints,
                "symbols_active": symbols_active, "completed_at": completed_at,
            })
        })
        .collect::<Vec<_>>())))
}
