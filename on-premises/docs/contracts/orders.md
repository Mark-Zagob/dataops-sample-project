# Data Contract: `sales_db.orders`

## Contract Information

| Field | Value |
|-------|-------|
| Contract ID | `sales_db.orders.v1` |
| Version | `1.0.0` |
| Status | `active` |
| Domain | `sales` |
| Dataset | `orders` |
| Source System | MySQL `sales_db` |
| Source Table | `orders` |
| Production Table | `orders_production` |

## Ownership

| Role | Team | Contact |
|------|------|---------|
| Contract Owner | Platform Engineering | `#platform-support` |
| Source Producer | Sales Application Team | `#sales-app-team` |
| Primary Consumer | Data Engineering | `#data-engineering` |
| Business Consumer | Analytics/Executive | `#analytics-support` |

## Purpose

Bảng `orders` đại diện cho dữ liệu đơn hàng từ hệ thống Sales.

Dữ liệu này được dùng để:

- Xây dựng bảng production `orders_production`.
- Phục vụ dashboard đơn hàng hằng ngày.
- Cung cấp dữ liệu cho các phân tích downstream về doanh thu, trạng thái đơn hàng và hành vi khách hàng.

## Required Columns

| Column | Type | Required | Notes |
|--------|------|:--------:|-------|
| `order_id` | `int` | Yes | Primary key |
| `customer_id` | `int` | Yes | Foreign key tới customer |
| `amount` | `decimal(10,2)` | Yes | Giá trị đơn hàng |
| `status` | `varchar(50)` | Yes | Trạng thái đơn hàng |
| `created_at` | `datetime` | Yes | Thời điểm tạo đơn |

## Compatibility Policy

Contract này tuân theo policy: `backward_compatible_additive_only`

### Non-breaking changes

Các thay đổi sau được xem là non-breaking:

- Thêm cột mới nhưng không bắt buộc pipeline hiện tại phải dùng ngay.
- Tăng độ dài `varchar`.
- Tăng precision của `decimal` nhưng giữ nguyên scale.
- Thêm index.
- Thêm comment.
- Widen integer type an toàn, ví dụ `int` → `bigint`.

### Breaking changes

Các thay đổi sau được xem là breaking:

- Xóa cột bắt buộc.
- Đổi tên cột bắt buộc.
- Đổi kiểu dữ liệu không tương thích.
- Giảm độ dài `varchar`.
- Thay đổi scale của `decimal`.
- Thay đổi primary key semantics.
- Thay đổi nullability của cột bắt buộc.

### Deprecation Policy

- Thời gian deprecation mặc định: **30 ngày**.
- Cột deprecated phải được ghi trong `deprecated_columns`.
- Trước khi xóa cột khỏi source, phải có approval từ:
  - Contract Owner
  - Data Engineering Lead
  - Consumer teams

### Change Process

Mọi thay đổi đối với contract này phải tuân theo: `docs/processes/data-contract-change-process.md`

Tóm tắt:

1. Source Team đề xuất thay đổi.
2. Cập nhật contract trong code.
3. Cập nhật tài liệu contract.
4. Chạy contract tests.
5. Mở Pull Request.
6. Yêu cầu approval từ các bên liên quan.
7. Chỉ deploy sau khi được approve.

## Operational SLA

| Metric | Target |
|--------|--------|
| Freshness | Dữ liệu production nên sẵn sàng trước 07:30 hằng ngày |
| Max data age for healthcheck | 26 giờ |
| Minimum expected rows | 1 |
| Violation response SLA | 60 phút |

## Alerting

Khi contract bị vi phạm:

- Pipeline phải fail-fast.
- Không được để dữ liệu bẩn đi vào production.
- Cảnh báo nên được gửi tới: *(cần điền)*