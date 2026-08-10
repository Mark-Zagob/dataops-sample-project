-- =================================================================
-- MYSQL INITIALIZATION SCRIPT
-- =================================================================
-- Script này tự động chạy khi MySQL container được tạo lần đầu.
-- Nó tạo bảng "orders" và insert dữ liệu mẫu để test pipeline.
--
-- Lưu ý: Script này chỉ chạy khi volume mysql_data CHƯA tồn tại.
-- Nếu bạn muốn reset data, hãy chạy:
--   docker compose down -v  (xóa volumes)
--   docker compose up --build -d (tạo lại từ đầu)
-- =================================================================

-- Sử dụng database đã được tạo từ biến môi trường MYSQL_DATABASE
-- Mặc định là "sales_db"

-- Tạo bảng orders (Source Table)
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Index để tối ưu query theo customer và thời gian
    INDEX idx_customer_id (customer_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert dữ liệu mẫu
-- Dữ liệu này mô phỏng các đơn hàng trong 24h qua
INSERT INTO orders (customer_id, amount, status, created_at) VALUES
    (1001, 150.00, 'completed', NOW() - INTERVAL 2 HOUR),
    (1002, 89.99, 'completed', NOW() - INTERVAL 5 HOUR),
    (1003, 250.50, 'pending', NOW() - INTERVAL 8 HOUR),
    (1004, 42.00, 'completed', NOW() - INTERVAL 12 HOUR),
    (1005, 199.99, 'cancelled', NOW() - INTERVAL 15 HOUR),
    (1006, 75.25, 'completed', NOW() - INTERVAL 18 HOUR),
    (1007, 320.00, 'completed', NOW() - INTERVAL 20 HOUR),
    (1008, 55.50, 'pending', NOW() - INTERVAL 22 HOUR),
    (1009, 120.75, 'completed', NOW() - INTERVAL 23 HOUR),
    (1010, 88.00, 'completed', NOW() - INTERVAL 30 MINUTE);

-- Log để xác nhận script đã chạy
SELECT 'MySQL seed data loaded successfully!' AS status;