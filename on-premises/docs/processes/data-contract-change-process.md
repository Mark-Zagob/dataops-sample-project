# Data Contract Change Process

## Mục tiêu

Đảm bảo mọi thay đổi schema từ Source System đều được kiểm soát như một thay đổi sản phẩm, không phải thay đổi kỹ thuật ngầm.

## Phạm vi

Áp dụng cho tất cả data contracts trong thư mục:

```text
user_code/contracts/
```

## Nguyên tắc

- Không thay đổi schema source mà không thông báo cho Data Platform.
- Không cập nhật pipeline trước khi cập nhật contract.
- Breaking changes phải có approval và migration plan.
- Additive changes vẫn cần được ghi nhận trong contract.
- Contract tests phải pass trước khi merge.

## Loại thay đổi

### Non-breaking changes

**Ví dụ:**

- Thêm cột mới.
- Tăng độ dài varchar.
- Tăng decimal precision nhưng giữ nguyên scale.
- Thêm index.
- Thêm comment.

**Hành vi:**

- Pipeline có thể tiếp tục chạy.
- Có thể xuất hiện warning nếu cột mới chưa được khai báo trong contract.
- Nên cập nhật contract trong thời gian sớm.

### Breaking changes

**Ví dụ:**

- Xóa cột bắt buộc.
- Đổi tên cột bắt buộc.
- Đổi kiểu dữ liệu không tương thích.
- Giảm độ dài varchar.
- Đổi decimal scale.
- Đổi primary key semantics.

**Hành vi:**

- Pipeline phải fail-fast.
- Không được đưa dữ liệu vào production.
- Cần incident/communication process rõ ràng.

## Quy trình chuẩn

### Bước 1: Đề xuất thay đổi

Source Team hoặc Data Team tạo đề xuất thay đổi, bao gồm:

- Lý do thay đổi.
- Cột bị ảnh hưởng.
- Kiểu thay đổi.
- Thời điểm deploy dự kiến.
- Impact đến consumers.

### Bước 2: Phân loại thay đổi

Platform Engineer và Data Engineer Lead phân loại:

- Non-breaking
- Breaking
- Deprecated
- Retired

### Bước 3: Cập nhật contract

Nếu thay đổi được chấp nhận:

- Cập nhật `user_code/contracts/order_contract.py`.
- Tăng version contract.
- Cập nhật `docs/contracts/orders.md`.
- Nếu có cột deprecated, thêm vào `deprecated_columns`.

### Bước 4: Chạy automated tests

Contract tests phải pass. Các test tối thiểu:

- Contract có đủ metadata.
- Registry hợp lệ.
- Schema hợp lệ pass.
- Missing required column fail.
- Type mismatch fail.
- Varchar length decrease fail.
- Decimal scale change fail.
- Unknown column warns but does not fail.

### Bước 5: Review và approve

Pull request phải được approve bởi:

- Platform Engineering
- Data Engineering Lead
- Source Owner
- Consumer representative *(nếu là breaking change)*

### Bước 6: Deploy

Chỉ deploy source change sau khi contract change đã được merge hoặc có rollout plan rõ ràng.

## Versioning

Áp dụng semantic versioning cho contract:

| Thay đổi | Version bump |
|---|---|
| Breaking change | `MAJOR` |
| Additive backward-compatible change | `MINOR` |
| Doc fix, metadata fix, không đổi behavior | `PATCH` |

**Ví dụ:**

- `1.0.0` → ban đầu
- `1.1.0` → thêm cột optional mới
- `2.0.0` → đổi tên cột bắt buộc

## SLA khi contract bị vi phạm

| Severity | Response Target |
|---|---|
| Breaking violation | 60 phút |
| Unknown additive column | 3 ngày làm việc |
| Deprecated column still in use | Theo deprecation timeline |

## Escalation

Nếu contract violation không được xử lý trong SLA:

1. Escalate lên Data Engineering Lead.
2. Escalate lên Platform Engineering Lead.
3. Nếu ảnh hưởng production, escalate sang SRE on-call.