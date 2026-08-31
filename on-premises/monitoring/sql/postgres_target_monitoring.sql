-- ============================================================================
-- POSTGRES TARGET MONITORING SETUP
-- ============================================================================
-- Database: analytics_dwh
--
-- Mục tiêu:
-- - Tạo monitoring role least privilege.
-- - Tạo các function giúp postgres_exporter scrape an toàn.
-- - Tránh lỗi scrape khi orders_production chưa tồn tại.
-- - Tránh query trực tiếp quá mạnh trong scrape config.
--
-- Production note:
-- - Các query COUNT(*) / duplicate / NULL có thể rất nặng với bảng lớn.
-- - Trong production thật, nên ưu tiên pipeline-emitted metrics.
-- - Lab này chấp nhận query trực tiếp vì dữ liệu nhỏ.
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
        PASSWORD '__MONITORING_POSTGRES_TARGET_PASSWORD__';
    ELSE
        -- Hỗ trợ rotate password trong lab.
        ALTER ROLE monitoring
        LOGIN
        PASSWORD '__MONITORING_POSTGRES_TARGET_PASSWORD__';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 2. Tạo schema monitoring
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ----------------------------------------------------------------------------
-- 3. Function kiểm tra orders_production có tồn tại không
-- ----------------------------------------------------------------------------
-- SRE reasoning:
-- Monitoring không được fail chỉ vì production table chưa tồn tại.
-- Thay vào đó trả về 0 để Prometheus có metric và alert phù hợp.
CREATE OR REPLACE FUNCTION monitoring.orders_production_exists()
RETURNS integer
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN CASE
        WHEN to_regclass('public.orders_production') IS NULL THEN 0
        ELSE 1
    END;
END;
$$;

-- ----------------------------------------------------------------------------
-- 4. Function tính business data age theo MAX(created_at)
-- ----------------------------------------------------------------------------
-- Contract hiện tại định nghĩa freshness dựa trên tuổi của dữ liệu mới nhất.
-- Trả về 9999 nếu không xác định được, để alert có thể bắt được trạng thái xấu.
CREATE OR REPLACE FUNCTION monitoring.orders_data_age_hours()
RETURNS double precision
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    age_hours double precision;
BEGIN
    IF to_regclass('public.orders_production') IS NULL THEN
        RETURN 9999;
    END IF;

    EXECUTE '
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600.0
        FROM public.orders_production
    ' INTO age_hours;

    RETURN COALESCE(age_hours, 9999);
EXCEPTION WHEN others THEN
    -- Nếu schema đổi bất thường hoặc lỗi query, trả về giá trị rất lớn.
    -- Không để exporter crash.
    RETURN 9999;
END;
$$;

-- ----------------------------------------------------------------------------
-- 5. Function đếm row count
-- ----------------------------------------------------------------------------
-- Production warning:
-- COUNT(*) trên bảng lớn có thể gây tải. Trong production thật, hãy cân nhắc:
-- - pg_class.reltuples nếu chấp nhận ước lượng.
-- - Pipeline-emitted metric sau mỗi run.
CREATE OR REPLACE FUNCTION monitoring.orders_row_count()
RETURNS bigint
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    row_count bigint;
BEGIN
    IF to_regclass('public.orders_production') IS NULL THEN
        RETURN 0;
    END IF;

    EXECUTE '
        SELECT COUNT(*)
        FROM public.orders_production
    ' INTO row_count;

    RETURN COALESCE(row_count, 0);
EXCEPTION WHEN others THEN
    RETURN 0;
END;
$$;

-- ----------------------------------------------------------------------------
-- 6. Function kiểm tra data quality
-- ----------------------------------------------------------------------------
-- Đây là function nặng.
-- Lab chấp nhận vì dữ liệu nhỏ.
-- Production-grade thật: không scrape liên tục kiểu này trên bảng lớn.
CREATE OR REPLACE FUNCTION monitoring.orders_quality_counts()
RETURNS TABLE (
    null_count bigint,
    negative_amount_count bigint,
    duplicate_order_id_groups bigint
)
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    IF to_regclass('public.orders_production') IS NULL THEN
        RETURN QUERY
        SELECT 0::bigint, 0::bigint, 0::bigint;
        RETURN;
    END IF;

    RETURN QUERY EXECUTE $q$
        SELECT
            (
                SELECT COUNT(*)
                FROM public.orders_production
                WHERE order_id IS NULL
                   OR customer_id IS NULL
                   OR amount IS NULL
                   OR status IS NULL
                   OR created_at IS NULL
            ) AS null_count,
            (
                SELECT COUNT(*)
                FROM public.orders_production
                WHERE amount < 0
            ) AS negative_amount_count,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT order_id
                    FROM public.orders_production
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) duplicated_orders
            ) AS duplicate_order_id_groups
    $q$;
EXCEPTION WHEN others THEN
    -- Trả về giá trị sentinel để alert biết quality check không chạy được.
    RETURN QUERY
    SELECT 999999::bigint, 999999::bigint, 999999::bigint;
END;
$$;

-- ----------------------------------------------------------------------------
-- 7. Grants cho monitoring role
-- ----------------------------------------------------------------------------
-- Monitoring cần:
-- - USAGE trên schema.
-- - SELECT trên các bảng Data Plane để kiểm tra trạng thái.
-- - EXECUTE trên monitoring functions.
GRANT USAGE ON SCHEMA public TO monitoring;
GRANT USAGE ON SCHEMA monitoring TO monitoring;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitoring;

-- Để các bảng tương lai như orders_production_backup, orders_stg_* cũng readable.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO monitoring;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA monitoring TO monitoring;

ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring
GRANT EXECUTE ON FUNCTIONS TO monitoring;