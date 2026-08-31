-- ============================================================================
-- MYSQL MONITORING USER
-- ============================================================================
-- Mục tiêu:
-- - Tạo user riêng cho mysqld_exporter.
-- - Không dùng root hoặc application user để monitoring.
-- - Least privilege ở mức phù hợp với exporter.
--
-- Production note:
-- - MySQL exporter thường cần quyền global ở mức tối thiểu cho metrics.
-- - Trong môi trường strict, cần rà soát lại từng collector.
-- ============================================================================

-- Idempotent ở mức tạo user nếu chưa tồn tại.
CREATE USER IF NOT EXISTS 'monitoring'@'%'
IDENTIFIED BY '__MONITORING_MYSQL_PASSWORD__';

-- Nếu cần rotate password, nên chạy thêm ALTER USER thủ công hoặc script riêng.
-- ALTER USER 'monitoring'@'%' IDENTIFIED BY '<new-password>';

-- PROCESS: đọc global process list / status.
-- REPLICATION CLIENT: cần cho một số replication metrics.
-- SELECT: đọc các bảng metrics cần thiết.
GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO 'monitoring'@'%';

FLUSH PRIVILEGES;