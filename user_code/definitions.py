"""
=================================================================
DAGSTER DEFINITIONS - Entry Point
=================================================================
File này tổng hợp tất cả Data Assets, Schedules, Sensors
thành một đối tượng Definitions duy nhất để Dagster load.

Đây là nơi duy nhất mà workspace.yaml trỏ đến.
"""

from dagster import Definitions

# Import các Assets từ module assets
from user_code.assets.orders import (
    validate_orders_schema,
    orders_staging,
    orders_production,
)

# Tập hợp tất cả Assets vào Definitions
# Dagster sẽ tự động phân tích dependency giữa các assets
# dựa trên tham số `deps` mà chúng ta đã khai báo.
defs = Definitions(
    assets=[
        validate_orders_schema,
        orders_staging,
        orders_production,
    ],
    # Trong tương lai, chúng ta sẽ thêm:
    # schedules=[...],     # Lịch chạy tự động (ví dụ: 06:00 AM hàng ngày)
    # sensors=[...],       # Cảm biến phát hiện schema drift realtime
    # resources={...},     # Cấu hình kết nối DB tập trung
)