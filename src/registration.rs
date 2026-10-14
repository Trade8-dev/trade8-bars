// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
// SPDX-License-Identifier: Apache-2.0

//! Service self-registration data for Trade8:Bars.
//!
//! Output via `--print-registration`. Scopes per master design §9; pages
//! per bars design §8.

use sentin8::registration::{
    AdminScope, AdminUi, AdminUiPage, ErrorCode, M2mGrant, PgRole, PolicySetDecl, Registration,
    RelationDecl, SqlMigration,
};

/// The Shield text of the declared scope-mirror set — public so the
/// policy wiring tests assert against the REAL seed.
pub const SCOPE_MIRROR_POLICIES: &str = "permit(principal, action == Action::\"trade8:feed:history:read\", resource) when { principal.scopes.contains(\"trade8:feed:history:read\") };\npermit(principal, action == Action::\"trade8:feed:history:read\", resource) when { principal.scopes.contains(\"trade8:feed:quote:read\") };\npermit(principal, action == Action::\"trade8:feed:history:read\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:feed:history:read\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:feed:history:read\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };\npermit(principal, action == Action::\"trade8:report:eod:read\", resource) when { principal.scopes.contains(\"trade8:report:eod:read\") };\npermit(principal, action == Action::\"trade8:report:eod:read\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };\npermit(principal, action == Action::\"trade8:report:eod:read\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };\npermit(principal, action == Action::\"trade8:report:eod:read\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };";

/// Default declarative policy set (Sentin8:Policy design §3.2/§3.4,
/// seeded by `vertical enroll` as `origin='platform'`, `mode='enforced'`):
/// action-granular scope permits over the LIVE effective scope set — the
/// exact mirror of the gates this service's handlers evaluate in step 5
/// (policy design §4.3). Enforced: step 5 is the
/// only gate; shadow is the emergency one-UPDATE rollback lever only.
const POLICIES: &[PolicySetDecl] = &[PolicySetDecl {
    policy_set_name: "scope-mirror",
    description: "Action-granular scope permits for trade8:bars over the LIVE effective scope set (design §3.1d). Enforced — step 5 is the only gate.",
    // Shield text lives in SCOPE_MIRROR_POLICIES (single source).
    policies: SCOPE_MIRROR_POLICIES,
    schema: None,
    mode: "enforced",
    default_enabled: true,
}];

/// Default ReBAC relation tuples (design §3.1c) — none declared by this
/// service today; env admins author their own graph (office/trader,
/// entitlement edges) via the relations admin API.
const RELATIONS: &[RelationDecl] = &[];

const PG_ROLES: &[PgRole] = &[PgRole {
    scope: "trade8:feed:history:read,trade8:report:eod:read,trade8:cron:run",
    role: "trade8_bars_service",
    bypass_rls: false,
    database: "trade8",
    // Owns all its reads — no cross-schema grants.
    cross_schema_grants: &[],
}];

const ADMIN_SCOPES: &[AdminScope] = &[
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:feed:history:read",
    },
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:report:eod:read",
    },
    AdminScope::Typed {
        role: "trade8:admin",
        scope: "trade8:bars:manage",
    },
    // Policy-admin surface (policy design §3.6; principles §6.0b per-service scope).
    AdminScope::Typed {
        role: "trade8:bars:admin",
        scope: "trade8:bars:policy:read",
    },
    AdminScope::Typed {
        role: "trade8:bars:admin",
        scope: "trade8:bars:policy:write",
    },
];

const ERROR_CODES: &[ErrorCode] = &[ErrorCode {
    enum_name: "Trade8BarIntervalInvalid",
    error_code: "trade8_bar_interval_invalid",
    status_code: 400,
}];

const ADMIN_UI_PAGES: &[AdminUiPage] = &[
    AdminUiPage {
        route: "bars",
        label: "Bars",
        ui_endpoint: "/v1/admin/ui/bars",
        data_endpoint: "/v1/admin/bars",
        icon: "0xe261",
    },
    AdminUiPage {
        route: "eod",
        label: "EOD Reports",
        ui_endpoint: "/v1/admin/ui/eod",
        data_endpoint: "/v1/admin/eod",
        icon: "0xe8d0",
    },
    AdminUiPage {
        route: "policy-sets",
        label: "Policy Sets",
        ui_endpoint: "/v1/admin/ui/policy-sets",
        data_endpoint: "/v1/admin/policy-sets",
        icon: "0xe8b8",
    },
    AdminUiPage {
        route: "relations",
        label: "Relations",
        ui_endpoint: "/v1/admin/ui/relations",
        data_endpoint: "/v1/admin/relations",
        icon: "0xe572",
    },
    AdminUiPage {
        route: "attributes",
        label: "Attributes",
        ui_endpoint: "/v1/admin/ui/attributes",
        data_endpoint: "/v1/admin/attributes",
        icon: "0xe8d3",
    },
];

/// Workload scope grants (self-registration design §3.11). One
/// entry per workload this service presents a certificate for — the
/// fail-closed registry map admits only these. Scope values use the
/// full vocabulary form (bare `bus:consume` fails the map's superset
/// check).
const M2M_GRANTS: &[M2mGrant] = &[
    // The service's own workload identity: the shell 8b outbox relay +
    // the direct callers. Colon + short spellings both granted — the
    // registry map looks up the exact workload name each call site
    // presents a certificate for.
    M2mGrant {
        workload: "trade8:bars",
        scopes: &[
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "bars",
        scopes: &[
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    // Shell audit-journal pusher workload (audit crate §6.8; boot step 8c).
    M2mGrant {
        workload: "bars-audit",
        scopes: &["sentin8:sys:audit:write"],
    },
    // Shield hot-reload consumer (policy crate `policy_bus_client`,
    // full colon identity) + stat emitter (stat_emit short form).
    M2mGrant {
        workload: "trade8:bars-policy",
        scopes: &["sentin8:bus:consume", "sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "bars-policy",
        scopes: &["sentin8:stat:measure"],
    },
    // TLS-RLS-4 (VDC §VDC.7): the database-free HostContextResolver's
    // resolve workload — cache miss calls Sys `GET /v1/resolve/hostname`.
    M2mGrant {
        workload: "trade8-bars-tls",
        scopes: &["sentin8:sys:instances:read"],
    },
    // Sentin8:Cron EOD schedule registration at boot (trade8-client
    // cron.rs: the `{service_type}-cron` workload certificate,
    // POST /v1/schedules).
    M2mGrant {
        workload: "trade8:bars-cron",
        scopes: &["sentin8:cron:write"],
    },
];

const SQL_1_INIT: &str = include_str!("../sql/init.sql");

fn empty_openapi() -> serde_json::Value {
    serde_json::Value::Null
}

pub fn print(section: Option<String>) -> anyhow::Result<()> {
    let reg = Registration {
        service_type: "trade8:bars",
        subdomain: "trade8-bars",
        health_path: "/v1/health",
        admin_scopes: ADMIN_SCOPES,
        error_codes: ERROR_CODES,
        consent_providers: &[],
        consent_flows: &[],
        admin_ui: Some(AdminUi {
            label: "Bars",
            icon: "0xe261",
            service_flag: "trade8-bars",
            pages: ADMIN_UI_PAGES,
        }),
        sql_migrations: vec![SqlMigration {
            version: 1,
            name: "init",
            sql: SQL_1_INIT,
        }],
        pg_roles: PG_ROLES,
        audit_findings: None,
        service_actions: None,
        m2m_grants: Some(M2M_GRANTS),
        openapi_url: "/openapi.json",
        openapi: empty_openapi,
        policies: POLICIES,
        relations: RELATIONS,
        event_triggers: &[],
        schedules: &[],
    };
    sentin8::registration::print_registration(section, &reg, empty_openapi)
}
