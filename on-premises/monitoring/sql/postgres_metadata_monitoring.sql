-- ============================================================================
-- POSTGRES DAGSTER METADATA MONITORING SETUP
-- ============================================================================
-- Database: dagster_metadata
--
-- Mục tiêu:
-- - Tạo monitoring role least privilege cho Control Plane metadata.
-- - Tạo functions đo daemon heartbeat và split-brain.
-- - Không sửa đổi logic của Dagster.
-- - Không gây ảnh hưởng đến run storage.
--
-- Production note:
-- - Metadata DB là Control Plane state.
-- - Nếu DB này unhealthy, pipeline có thể không dequeue run.
-- - Tuy nhiên data đã swap vào orders_production không tự mất.
--
-- [INCIDENT FIX 2026-09-03]
-- - Thêm IN-list version-tolerant cho 'RUN_COORDINATOR' / 'QUEUED_RUN_COORDINATOR'
--   để tương thích Dagster 1.10.x.
-- - Thêm drift detector function daemon_type_inventory().
-- ============================================================================

-- 1. Tạo role monitoring idempotent
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles
        WHERE rolname = 'monitoring'
    ) THEN
        CREATE ROLE monitoring
            LOGIN
            PASSWORD 'MONITORING_POSTGRES_METADATA_PASSWORD';
    ELSE
        ALTER ROLE monitoring
            LOGIN
            PASSWORD 'MONITORING_POSTGRES_METADATA_PASSWORD';
    END IF;
END
$$;

-- 2. Tạo schema monitoring
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ============================================================================
-- 3. Function tính tuổi heartbeat của Run Coordinator family
-- ============================================================================
-- [INCIDENT FIX 2026-09-03]
-- Dagster 1.10.x ghi 'QUEUED_RUN_COORDINATOR', version cũ ghi 'RUN_COORDINATOR'.
-- IN-list dưới đây tolerate cả hai. Khi có alias mới, THÊM vào IN-list
-- ở đây VÀ ở function active_daemon_count() phần 4 (anchor comment).
CREATE OR REPLACE FUNCTION monitoring.daemon_heartbeat_age_seconds()
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_type text;
    v_age  double precision;
BEGIN
    -- Defensive layer 1 (structural drift): bảng chưa tồn tại => sentinel.
    IF to_regclass('public.daemon_heartbeats') IS NULL THEN
        RETURN 999999;
    END IF;

    -- Defensive layer 2: cột timestamp có tồn tại và có type hợp lệ?
    SELECT data_type INTO v_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'daemon_heartbeats'
      AND column_name  = 'timestamp';

    IF v_type IS NULL THEN
        RETURN 999999;
    END IF;

    -- Defensive layer 3 (enum drift): IN-list các alias đã biết.
    IF v_type IN ('timestamp with time zone', 'timestamp without time zone') THEN
        EXECUTE '
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX("timestamp")))
            FROM public.daemon_heartbeats
            WHERE UPPER(daemon_type) IN (''RUN_COORDINATOR'', ''QUEUED_RUN_COORDINATOR'')
        ' INTO v_age;
    ELSE
        -- Numeric epoch seconds (behavioral drift tolerance).
        EXECUTE '
            SELECT EXTRACT(EPOCH FROM NOW()) - MAX("timestamp")
            FROM public.daemon_heartbeats
            WHERE UPPER(daemon_type) IN (''RUN_COORDINATOR'', ''QUEUED_RUN_COORDINATOR'')
        ' INTO v_age;
    END IF;

    RETURN COALESCE(v_age, 999999);
EXCEPTION WHEN others THEN
    RETURN 999999;
END;
$$;

-- ============================================================================
-- 4. Function đếm số daemon active gần đây (split-brain detection)
-- ============================================================================
-- [INCIDENT FIX 2026-09-03] - cùng root cause với phần 3.
-- [ANCHOR] CÙNG IN-list với phần 3. Sửa 1 chỗ thì sửa cả 2.
CREATE OR REPLACE FUNCTION monitoring.active_daemon_count(
    max_stale_seconds double precision DEFAULT 120
)
RETURNS integer
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_type          text;
    v_has_daemon_id boolean;
    v_cutoff        double precision;
    v_sql           text;
    v_count         integer;
BEGIN
    IF to_regclass('public.daemon_heartbeats') IS NULL THEN
        RETURN 0;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'daemon_heartbeats'
          AND column_name  = 'daemon_id'
    ) INTO v_has_daemon_id;

    IF NOT v_has_daemon_id THEN
        -- Fail-safe: không vu oan split-brain nếu không có daemon_id column.
        RETURN 1;
    END IF;

    SELECT data_type INTO v_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'daemon_heartbeats'
      AND column_name  = 'timestamp';

    IF v_type IS NULL THEN
        RETURN 0;
    END IF;

    v_cutoff := EXTRACT(EPOCH FROM NOW()) - max_stale_seconds;

    IF v_type IN ('timestamp with time zone', 'timestamp without time zone') THEN
        v_sql := format(
            'SELECT COUNT(DISTINCT daemon_id)
             FROM public.daemon_heartbeats
             WHERE UPPER(daemon_type) IN (''RUN_COORDINATOR'', ''QUEUED_RUN_COORDINATOR'')
               AND "timestamp" > TO_TIMESTAMP(%s)',
            v_cutoff
        );
    ELSE
        v_sql := format(
            'SELECT COUNT(DISTINCT daemon_id)
             FROM public.daemon_heartbeats
             WHERE UPPER(daemon_type) IN (''RUN_COORDINATOR'', ''QUEUED_RUN_COORDINATOR'')
               AND "timestamp" > %s',
            v_cutoff
        );
    END IF;

    EXECUTE v_sql INTO v_count;
    RETURN COALESCE(v_count, 0);
EXCEPTION WHEN others THEN
    RETURN 0;
END;
$$;

-- ============================================================================
-- 5. Grants cho monitoring role
-- ============================================================================
GRANT USAGE ON SCHEMA public TO monitoring;
GRANT USAGE ON SCHEMA monitoring TO monitoring;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitoring;

-- Để các bảng tương lai Dagster tạo ra cũng readable.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO monitoring;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA monitoring TO monitoring;
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring
    GRANT EXECUTE ON FUNCTIONS TO monitoring;

-- ============================================================================
-- 6. NEW (v3): Drift Detector - phơi bày sự thật thô về mọi daemon_type
-- ============================================================================
-- [FIX v3 2026-09-03]
-- LỖI CỦA v2: "structure of query does not match function result type".
-- Nguyên nhân:
--   - LOWER(varchar) có thể trả về varchar, không phải text.
--   - EXTRACT(EPOCH FROM ...) trả về numeric, không phải double precision.
--   - Dynamic SQL không được type-check lúc CREATE FUNCTION, chỉ check runtime.
--
-- FIX: dùng subquery để tách 2 trách nhiệm:
--   (1) Inner query: tính toán thô (group by, max, coalesce).
--   (2) Outer query: cast tường minh sang đúng RETURNS TABLE signature.
--
-- SRE principle: "Explicit beats implicit in dynamic SQL."
-- Pattern này hoạt động ổn định trên mọi PostgreSQL version (12+) và không
-- phụ thuộc vào implicit cast policy thay đổi giữa các minor release.
CREATE OR REPLACE FUNCTION monitoring.daemon_type_inventory()
RETURNS TABLE (daemon_type text, heartbeat_age_seconds double precision)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_err text;
BEGIN
    IF to_regclass('public.daemon_heartbeats') IS NULL THEN
        RETURN QUERY SELECT 'no_table'::text, 999999::double precision;
        RETURN;
    END IF;

    -- [SRE] Subquery pattern: inner = computation, outer = type casting.
    -- - GROUP BY dùng LOWER(daemon_type) (không cast) để grouping ổn định.
    -- - Outer SELECT cast ::text và ::double precision để khớp RETURNS TABLE.
    -- - left(v_err, 60) vẫn giữ discipline cardinality cho error message.
    RETURN QUERY EXECUTE '
        SELECT
            inner_result.daemon_type::text,
            inner_result.heartbeat_age_seconds::double precision
        FROM (
            SELECT LOWER(daemon_type) AS daemon_type,
                   COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX("timestamp"))), 999999) AS heartbeat_age_seconds
            FROM public.daemon_heartbeats
            GROUP BY LOWER(daemon_type)
        ) AS inner_result
    ';
EXCEPTION WHEN others THEN
    GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    RETURN QUERY SELECT ('check_error: ' || left(v_err, 60))::text, 999999::double precision;
END;
$$;

-- Cấp quyền cho monitoring role đọc function mới
GRANT EXECUTE ON FUNCTION monitoring.daemon_type_inventory() TO monitoring;