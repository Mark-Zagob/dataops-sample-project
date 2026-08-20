"""
=================================================================
DATA CONTRACT: Bảng Orders
=================================================================
Đây là "Hợp đồng dữ liệu" giữa Source System (MySQL Kinh Doanh)
và Data Platform (Chúng ta).

Bất kỳ thay đổi nào về schema từ phía Source ĐỀU PHẢI được thỏa
thuận và cập nhật trong file này TRƯỚC KHI deploy.

Internal Product Mindset:
File này chính là "API Documentation" của dữ liệu.
Data Engineer nhìn vào đây để biết họ được phép kỳ vọng những gì từ Source.
"""

from typing import Dict, List


# =================================================================
# EXPECTED SOURCE SCHEMA
# =================================================================
# Định nghĩa schema kỳ vọng của bảng "orders" trong MySQL Source.
#
# Key: Tên cột
# Value: Kiểu dữ liệu SQL tương ứng (prefix check)
# =================================================================
EXPECTED_ORDERS_SCHEMA: Dict[str, str] = {
    "order_id": "int",
    "customer_id": "int",
    "amount": "decimal",
    "status": "varchar",
    "created_at": "datetime",
}


# =================================================================
# REQUIRED COLUMNS
# =================================================================
# Danh sách các cột bắt buộc phải có.
# Nếu thiếu bất kỳ cột nào trong danh sách này, Pipeline PHẢI fail-fast.
# =================================================================
REQUIRED_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "amount",
    "status",
    "created_at",
]


# =================================================================
# TARGET COLUMNS
# =================================================================
# Danh sách cột sẽ được load vào PostgreSQL staging/production.
# Thứ tự này được dùng cho SELECT, DataFrame và INSERT.
# =================================================================
TARGET_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "amount",
    "status",
    "created_at",
]


# =================================================================
# TABLE NAMES
# =================================================================
SOURCE_TABLE_NAME = "orders"
PRODUCTION_TABLE_NAME = "orders_production"

# Tên staging cố định của kiến trúc cũ.
# Giữ lại để cleanup nếu môi trường từng chạy phiên bản cũ.
LEGACY_STAGING_TABLE_NAME = "orders_staging"

# Prefix của staging table theo run.
# Ví dụ: orders_stg_20260616123045_ab12cd34
STAGING_TABLE_PREFIX = "orders_stg_"


# =================================================================
# CONCURRENCY / LOCKING
# =================================================================
# PostgreSQL advisory lock key.
# Giá trị này phải là BIGINT-compatible.
# Không dùng số ngẫu nhiên khó hiểu; chọn số dễ nhớ nội bộ.
# =================================================================
ORDERS_PIPELINE_LOCK_KEY = 906033


# =================================================================
# DATA FRESHNESS / COMPLETENESS GUARD
# =================================================================
# Số dòng tối thiểu kỳ vọng khi extract từ source.
#
# Lab hiện tại có 10 rows seed data, nên đặt là 1 để fail-fast
# nếu source trả về 0 rows bất thường.
#
# Nếu trong tương lai có ngày thực sự không có đơn hàng và bạn
# muốn pipeline chấp nhận bảng rỗng, hãy đổi thành 0.
# =================================================================
MIN_EXPECTED_ROWS = 1