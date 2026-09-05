-- ============================================================================
-- POSTGRES TARGET MONITORING SETUP (LIGHTWEIGHT ONLY)
-- ============================================================================
-- Database: analytics_dwh
--
-- Mục tiêu:
-- - Tạo monitoring role least privilege.
-- - Tạo các function giúp postgres_exporter scrape an toàn.
-- - CHỈ giữ lại lightweight functions (không query data rows).
-- - Data quality, row count, data age giờ do PIPELINE EMIT qua Pushgateway.
--
-- ARCHITECTURE CHANGE (Phase 1 - SRE Optimization):
-- - TRƯỚC: postgres_exporter gọi function nặng (COUNT(*), MAX(created_at))
--   → Noisy Neighbor, giết DB production khi scale
-- - SAU: Pipeline tự tính metric → Push lên Pushgateway
--   → Data Plane KHÔNG BỊ CHẠM bởi monitoring
--
-- FUNCTIONS GIỮ LẠI (lightweight):
-- - orders_production_exists(): query pg_catalog, không scan data
--
-- FUNCTIONS ĐÃ XÓA (chuyển sang pipeline-emitted):
-- - orders_data_age_hours(): pipeline tự tính từ MAX(created_at)
-- - orders_row_count(): pipeline tự đếm khi load staging
-- - orders_quality_counts(): pipeline tự check trên staging trước khi swap
--
-- Production note:
-- - File này giờ chỉ chứa metadata functions
-- - Nếu cần debug data quality, query trực tiếp DB (không qua exporter)
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
-- 3. Function kiểm tra orders_production có tồn tại không (LIGHTWEIGHT)
-- ----------------------------------------------------------------------------
-- SRE reasoning:
-- Monitoring không được fail chỉ vì production table chưa tồn tại.
-- Thay vào đó trả về 0 để Prometheus có metric và alert phù hợp.
--
-- WHY LIGHTWEIGHT?
-- - Dùng to_regclass() → query system catalog
-- - KHÔNG scan data rows
-- - Trả về ngay lập tức
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
-- 4. Grants cho monitoring role
-- ----------------------------------------------------------------------------
-- Monitoring cần:
-- - USAGE trên schema.
-- - SELECT trên các bảng Data Plane để kiểm tra trạng thái (cho future use).
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

-- ----------------------------------------------------------------------------
-- NOTES FOR FUTURE PIPELINES:
-- ----------------------------------------------------------------------------
-- Khi thêm pipeline mới (customers, inventory, etc.), bạn KHÔNG cần thêm
-- function nặng vào file này. Thay vào đó:
--
-- 1. Pipeline tự tính metric khi chạy xong
-- 2. Pipeline push metric lên Pushgateway với naming convention:
--    - dagster_pipeline_data_age_hours{pipeline_name="customers"}
--    - dagster_pipeline_row_count{pipeline_name="customers"}
--    - dagster_pipeline_quality_null_count{pipeline_name="customers"}
--
-- 3. Chỉ cần thêm lightweight function nếu cần metadata check
--    (table exists, backup count, orphan count)
--
-- Đây là "Self-serve Observability" pattern: Data Engineer có thể add
-- pipeline mới mà không cần Platform Engineer tạo monitoring function.
-- ----------------------------------------------------------------------------
