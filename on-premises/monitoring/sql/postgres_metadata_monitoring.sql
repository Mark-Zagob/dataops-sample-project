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
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Tạo role monitoring idempotent
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles
        WHERE rolname = 'monitoring'
    ) THEN
        CREATE ROLE monitoring
        LOGIN
        PASSWORD '__MONITORING_POSTGRES_METADATA_PASSWORD__';
    ELSE
        ALTER ROLE monitoring
        LOGIN
        PASSWORD '__MONITORING_POSTGRES_METADATA_PASSWORD__';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 2. Tạo schema monitoring
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ----------------------------------------------------------------------------
-- 3. Function tính tuổi heartbeat của RUN_COORDINATOR daemon
-- ----------------------------------------------------------------------------
-- Hàm này xử lý cả hai khả năng:
-- - timestamp là kiểu timestamp/timestamptz
-- - timestamp là kiểu numeric epoch
--
-- Điều này quan trọng vì healthcheck hiện tại của bạn cũng phải xử lý đa schema.
CREATE OR REPLACE FUNCTION monitoring.daemon_heartbeat_age_seconds()
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_type text;
    v_age double precision;
BEGIN
    IF to_regclass('public.daemon_heartbeats') IS NULL THEN
        RETURN 999999;
    END IF;

    SELECT data_type
    INTO v_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'daemon_heartbeats'
      AND column_name = 'timestamp';

    IF v_type IS NULL THEN
        RETURN 999999;
    END IF;

    IF v_type IN ('timestamp with time zone', 'timestamp without time zone') THEN
        EXECUTE '
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX("timestamp")))
            FROM public.daemon_heartbeats
            WHERE UPPER(daemon_type) = ''RUN_COORDINATOR''
        ' INTO v_age;
    ELSE
        -- Giả định numeric epoch seconds.
        EXECUTE '
            SELECT EXTRACT(EPOCH FROM NOW()) - MAX("timestamp")
            FROM public.daemon_heartbeats
            WHERE UPPER(daemon_type) = ''RUN_COORDINATOR''
        ' INTO v_age;
    END IF;

    RETURN COALESCE(v_age, 999999);
EXCEPTION WHEN others THEN
    RETURN 999999;
END;
$$;

-- ----------------------------------------------------------------------------
-- 4. Function đếm số daemon active gần đây
-- ----------------------------------------------------------------------------
-- Dùng để phát hiện split-brain.
-- Nếu không có daemon_id, trả về 1 để không tạo alert giả.
CREATE OR REPLACE FUNCTION monitoring.active_daemon_count(
    max_stale_seconds double precision DEFAULT 120
)
RETURNS integer
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_type text;
    v_has_daemon_id boolean;
    v_cutoff double precision;
    v_sql text;
    v_count integer;
BEGIN
    IF to_regclass('public.daemon_heartbeats') IS NULL THEN
        RETURN 0;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'daemon_heartbeats'
          AND column_name = 'daemon_id'
    )
    INTO v_has_daemon_id;

    IF NOT v_has_daemon_id THEN
        RETURN 1;
    END IF;

    SELECT data_type
    INTO v_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'daemon_heartbeats'
      AND column_name = 'timestamp';

    IF v_type IS NULL THEN
        RETURN 0;
    END IF;

    v_cutoff := EXTRACT(EPOCH FROM NOW()) - max_stale_seconds;

    IF v_type IN ('timestamp with time zone', 'timestamp without time zone') THEN
        v_sql := format(
            'SELECT COUNT(DISTINCT daemon_id)
             FROM public.daemon_heartbeats
             WHERE UPPER(daemon_type) = %L
               AND "timestamp" > TO_TIMESTAMP(%s)',
            'RUN_COORDINATOR',
            v_cutoff
        );
    ELSE
        v_sql := format(
            'SELECT COUNT(DISTINCT daemon_id)
             FROM public.daemon_heartbeats
             WHERE UPPER(daemon_type) = %L
               AND "timestamp" > %s',
            'RUN_COORDINATOR',
            v_cutoff
        );
    END IF;

    EXECUTE v_sql INTO v_count;

    RETURN COALESCE(v_count, 0);
EXCEPTION WHEN others THEN
    RETURN 0;
END;
$$;

-- ----------------------------------------------------------------------------
-- 5. Grants cho monitoring role
-- ----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO monitoring;
GRANT USAGE ON SCHEMA monitoring TO monitoring;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitoring;

-- Nếu Dagster tạo thêm bảng mới trong tương lai, monitoring vẫn đọc được.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO monitoring;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA monitoring TO monitoring;

ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring
GRANT EXECUTE ON FUNCTIONS TO monitoring;