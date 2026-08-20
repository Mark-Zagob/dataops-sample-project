"""
=================================================================
DAGSTER DEFINITIONS - Entry Point
=================================================================
File này tổng hợp tất cả Data Assets, Jobs, Schedules, Sensors
thành một đối tượng Definitions duy nhất để Dagster load.

Đây là nơi duy nhất mà workspace.yaml trỏ đến.
"""

from dagster import AssetSelection, Definitions, define_asset_job

from user_code.assets.orders import (
    validate_orders_schema,
    orders_staging,
    orders_production,
)


# =================================================================
# ORDERS PIPELINE JOB
# =================================================================
# Job này phục vụ cho:
# - Chạy toàn bộ orders pipeline từ Jobs tab.
# - Chuẩn bị cho schedule sau này.
# - Gắn tag pipeline=orders để tương lai có thể dùng
#   tag-based concurrency limits.
# =================================================================
orders_pipeline_job = define_asset_job(
    name="orders_pipeline_job",
    selection=AssetSelection.groups("orders_pipeline"),
    tags={"pipeline": "orders"},
)


defs = Definitions(
    assets=[
        validate_orders_schema,
        orders_staging,
        orders_production,
    ],
    jobs=[
        orders_pipeline_job,
    ],
    # Trong tương lai, chúng ta sẽ thêm:
    # schedules=[...],     # Lịch chạy tự động, ví dụ 06:00 AM hàng ngày
    # sensors=[...],       # Cảm biến phát hiện schema drift / freshness breach
    # resources={...},     # Cấu hình kết nối DB tập trung
)