"""
=================================================================
DATA CONTRACT: Bảng Orders
=================================================================
Đây là "Hợp đồng dữ liệu" giữa Source System (MySQL Kinh Doanh)
và Data Platform (Chúng ta).

Bất kỳ thay đổi nào về schema từ phía Source ĐỀU PHẢI được thỏa
thuận và cập nhật trong file này TRƯỚC KHI deploy.

Internal Product Mindset: File này chính là "API Documentation"
của dữ liệu. Data Engineer nhìn vào đây để biết họ được phép
kỳ vọng những gì từ Source.
"""

from typing import Dict, List

# Định nghĩa schema kỳ vọng của bảng "orders" trong MySQL Source
# Key: Tên cột (column name)
# Value: Kiểu dữ liệu SQL tương ứng
EXPECTED_ORDERS_SCHEMA: Dict[str, str] = {
    "order_id": "int",
    "customer_id": "int",
    "amount": "decimal",
    "status": "varchar",
    "created_at": "datetime",
}

# Danh sách các cột bắt buộc phải có (Primary Key, Foreign Key, Metrics)
# Nếu thiếu bất kỳ cột nào trong danh sách này, Pipeline PHẢI fail-fast.
REQUIRED_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "amount",
    "created_at",
]

# Tên bảng trong MySQL Source
SOURCE_TABLE_NAME = "orders"

# Tên bảng Staging và Production trong PostgreSQL Target
STAGING_TABLE_NAME = "orders_staging"
PRODUCTION_TABLE_NAME = "orders_production"