-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Bars
-- SPDX-License-Identifier: Apache-2.0

-- Trade8:Bars — OHLCV bar tables + EOD reports.
--
-- Design: ../bars/DESIGN.md §5.
--
-- modernization — no stored procedures, no per-symbol file DBs).

CREATE SCHEMA IF NOT EXISTS bars;

CREATE TABLE IF NOT EXISTS bars.bar_1s (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL,
 bucket_ts TIMESTAMPTZ NOT NULL,
 open_px BIGINT NOT NULL,
 high_px BIGINT NOT NULL,
 low_px BIGINT NOT NULL,
 close_px BIGINT NOT NULL,
 volume BIGINT NOT NULL,
 print_count INT NOT NULL,
 PRIMARY KEY (env_id, symbol_id, bucket_ts)
);

CREATE TABLE IF NOT EXISTS bars.bar_1m (LIKE bars.bar_1s INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bars.bar_5m (LIKE bars.bar_1s INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bars.bar_15m (LIKE bars.bar_1s INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bars.bar_1h (LIKE bars.bar_1s INCLUDING ALL);

CREATE TABLE IF NOT EXISTS bars.bar_1d (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL,
 bucket_ts TIMESTAMPTZ NOT NULL,
 open_px BIGINT NOT NULL,
 high_px BIGINT NOT NULL,
 low_px BIGINT NOT NULL,
 close_px BIGINT NOT NULL,
 volume BIGINT NOT NULL,
 print_count INT NOT NULL,
 vwap BIGINT,
 PRIMARY KEY (env_id, symbol_id, bucket_ts)
);

CREATE TABLE IF NOT EXISTS bars.eod_report (
 env_id UUID NOT NULL,
 trading_date DATE NOT NULL,
 prints BIGINT NOT NULL DEFAULT 0,
 symbols_active INT NOT NULL DEFAULT 0,
 storage_objects JSONB NOT NULL DEFAULT '[]',
 completed_at TIMESTAMPTZ,
 PRIMARY KEY (env_id, trading_date)
);

-- ---------------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
DECLARE pol TEXT;
BEGIN
 FOREACH tbl IN ARRAY ARRAY[
 'bars.bar_1s', 'bars.bar_1m', 'bars.bar_5m',
 'bars.bar_15m', 'bars.bar_1h', 'bars.bar_1d',
 'bars.eod_report'
 ] LOOP
 BEGIN
 pol := replace(split_part(tbl, '.', 2), '"', '');
 EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
 EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
 -- Suffix BEFORE %I quoting: `limit` alone is fine but the concatenation
 -- `"limit"_select` is invalid SQL — build 'limit_select' unquoted
 -- (desk-store DO-block memory: reserved-word table names are the
 -- classic failure class).
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_select', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_insert', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_update', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_delete', tbl);
 EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
 END;
 END LOOP;
END $$;

-- Local outbox + audit journal (DBX §6.7 / §6.8 — shell-generated DDL).
CREATE TABLE IF NOT EXISTS bars.outbox (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 event_type TEXT NOT NULL,
 stream_family TEXT NOT NULL,
 payload JSONB NOT NULL,
 publisher_id_token TEXT,
 publisher_subject TEXT,
 relayed BOOLEAN NOT NULL DEFAULT false,
 relayed_at TIMESTAMPTZ,
 relay_attempts INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unrelayed
 ON bars.outbox (created_at) WHERE relayed = false;
ALTER TABLE bars.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE bars.outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_select ON bars.outbox;
CREATE POLICY outbox_select ON bars.outbox FOR SELECT
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_insert ON bars.outbox;
CREATE POLICY outbox_insert ON bars.outbox FOR INSERT
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_update ON bars.outbox;
CREATE POLICY outbox_update ON bars.outbox FOR UPDATE
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
CREATE OR REPLACE FUNCTION bars.bars_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
 PERFORM pg_notify('bars_outbox_changed', '');
 RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON bars.outbox;
CREATE TRIGGER outbox_notify_trigger
 AFTER INSERT ON bars.outbox
 FOR EACH ROW
 EXECUTE FUNCTION bars.bars_notify_outbox();

CREATE TABLE IF NOT EXISTS bars.audit_journal (
 id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 action TEXT NOT NULL,
 target_resource_type TEXT,
 target_resource_id TEXT,
 actor_user_id TEXT,
 actor_subject_type TEXT,
 severity TEXT NOT NULL DEFAULT 'info',
 correlation_id TEXT,
 before_state JSONB,
 after_state JSONB,
 details JSONB,
 pushed BOOLEAN NOT NULL DEFAULT false,
 pushed_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bars_audit_journal_unpushed
 ON bars.audit_journal (created_at) WHERE pushed = false;
ALTER TABLE bars.audit_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE bars.audit_journal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_journal_dml ON bars.audit_journal;
CREATE POLICY audit_journal_dml ON bars.audit_journal
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());

-- ---------------------------------------------------------------------------
-- Per-service role grants + search_path (bank8-ledger pattern; the CLI
-- creates the role BEFORE this SQL — grant-before-role incident rule).
-- Grants live in db trade8 (this database); the scope→role map row is
-- upserted by `vertical enroll` from --section=pg_roles.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trade8_bars_service') THEN
  ALTER ROLE trade8_bars_service SET search_path = bars, public;
  GRANT USAGE ON SCHEMA bars TO trade8_bars_service;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bars TO trade8_bars_service;
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bars TO trade8_bars_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA bars
   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trade8_bars_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA bars
   GRANT USAGE, SELECT ON SEQUENCES TO trade8_bars_service;
 END IF;
END $$;
-- Declared cross-schema reads (SELECT-only) — mirrors registration.rs
-- PgRole.cross_schema_grants; `vertical enroll` owns their application,
-- this block only re-asserts them idempotently after a role recreation.

