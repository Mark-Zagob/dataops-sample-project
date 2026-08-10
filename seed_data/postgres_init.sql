-- =================================================================
-- POSTGRESQL INITIALIZATION SCRIPT
-- =================================================================
-- Script này tự động chạy khi PostgreSQL container được tạo lần đầu.
-- Nó tạo các extension cần thiết và schema ban đầu.
-- =================================================================

-- Tạo extension uuid nếu cần (cho future use)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log để xác nhận script đã chạy
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL initialization completed. Database: %', current_database();
END $$;