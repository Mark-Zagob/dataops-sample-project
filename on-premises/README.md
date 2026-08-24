# 🚀 DataOps Platform Lab - Production-Grade

> **Sứ mệnh:** Cung cấp một môi trường Data Platform tự phục vụ (self-serve) cho Data Engineers, với tiêu chuẩn vận hành SRE: Đáng tin cậy, Dễ quan sát, và Dễ phục hồi.

---

## 📋 Mục lục

- [Tổng quan](#-tổng-quan)
- [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
- [Yêu cầu tiên quyết](#-yêu-cầu-tiên-quyết)
- [Khởi động nhanh (5 phút)](#-khởi-động-nhanh-5-phút)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Sử dụng hàng ngày](#-sử-dụng-hàng-ngày)
  - [Chạy Pipeline](#-chạy-pipeline)
  - [Xem Lineage & Logs](#-xem-lineage--logs)
  - [Thêm Data Asset mới](#-thêm-data-asset-mới)
  - [Thay đổi Data Contract](#-thay-đổi-data-contract)
- [Quản lý dữ liệu](#-quản-lý-dữ-liệu)
  - [Reset dữ liệu về trạng thái ban đầu](#-reset-dữ-liệu-về-trạng-thái-ban-đầu)
  - [Kết nối trực tiếp Database (Debug)](#-kết-nối-trực-tiếp-database-debug)
- [Troubleshooting](#-troubleshooting)
- [FAQ](#-faq)
- [Liên hệ & Hỗ trợ](#-liên-hệ--hỗ-trợ)

---

## 📖 Tổng quan

Dự án này mô phỏng một **Data Platform Production-grade** chạy trên môi trường On-prem sử dụng Docker Compose. Nó thể hiện các nguyên lý cốt lõi của DataOps:

| Nguyên lý | Hiện thực hóa trong Lab |
|---|---|
| Idempotency | Atomic Swap + run-scoped staging |
| Data Contracts | Fail-fast Schema Validation trước khi ETL chạy |
| Concurrency Control | QueuedRunCoordinator, max_concurrent_runs=1 |
| Observability | Dagster UI với Lineage, Logs, Metadata |
| Self-serve | Seed data tự động, không cần nhờ Platform Team |
| Blast Radius Control | Tách biệt Services, Healthchecks, Dependency Management |
| Recoverability | Backup một thế hệ và orphan cleanup |

> 📐 **Muốn hiểu sâu hơn?** Hãy đọc [ARCHITECTURE.md](./ARCHITECTURE.md) để biết chi tiết về các quyết định thiết kế và trade-offs.

---

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────┐
│                    Dagster UI (Port 3000)                    │
│              Lineage • Logs • Metadata • Run History         │
└────────────────────────────┬──────────────────────────────────┘
                              │
┌────────────────────────────▼──────────────────────────────────┐
│              Control Plane: Dagster Platform                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐    │
│  │  Contract    │──│  ETL Engine  │──│  Atomic Swap      │    │
│  │  Validator   │  │  (Staging)   │  │  (Production)     │    │
│  └──────────────┘  └──────────────┘  └──────────────────┘    │
└────────────────────────────┬──────────────────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               │                              │
┌──────────────▼──────────┐    ┌──────────────▼────────────┐
│  MySQL Source (3306)     │    │  PostgreSQL Target       │
│  sales_db.orders         │    │  analytics_dwh           │
│  (10 rows seed data)     │    │  orders_stg_<run_id>     │
│                          │    │  orders_production       |
│                          |    |  orders_production_backup|  
└──────────────────────────┘    └──────────────────────────┘
```

**Luồng dữ liệu:**

1. **Validate:** Kiểm tra schema MySQL có khớp Data Contract không.
2. **Extract & Load:** Đọc dữ liệu từ MySQL → Ghi vào bảng `orders_staging` trong PostgreSQL.
3. **Atomic Swap:** Đổi tên `orders_staging` → `orders_production` trong một transaction. Nếu lỗi, ROLLBACK, dữ liệu cũ vẫn an toàn.

### Control Plane Metadata Storage

Lab này sử dụng PostgreSQL riêng (`dagster-metadata`) để lưu trữ trạng thái vận hành của Dagster, bao gồm:

- Run history
- Event logs
- Schedule storage

Lý do:

- Hỗ trợ concurrent access tốt hơn SQLite.
- Dễ backup/restore hơn.
- Phù hợp hơn cho multi-user và vận hành production.
- Tách biệt Control Plane metadata khỏi Data Plane storage.  
>Nếu `dagster-metadata` gặp sự cố, Dagster UI và khả năng thực thi run mới có thể bị ảnh hưởng. Tuy nhiên, dữ liệu đã tồn tại trong `orders_production` không tự động bị mất. Đây là Control Plane outage, không phải Data Plane data corruption.
---

## ✅ Yêu cầu tiên quyết

Trước khi bắt đầu, hãy đảm bảo máy bạn đã cài đặt:

| Công cụ | Phiên bản tối thiểu | Kiểm tra bằng lệnh |
|---------|----------------------|----------------------|
| **Docker** | 24.0+ | `docker --version` |
| **Docker Compose** | 2.20+ | `docker compose version` |
| **Git** | 2.30+ | `git --version` |

> ⚠️ **Lưu ý:** Sử dụng `docker compose` (có dấu cách), không phải `docker-compose` (dấu gạch ngang). Phiên bản mới đã tích hợp vào Docker CLI.

**Kiểm tra tài nguyên:**

- RAM tối thiểu: **4 GB** (MySQL + PostgreSQL + Dagster)
- Disk: **5 GB** trống (cho Docker images và volumes)

---

## 🚀 Khởi động nhanh (5 phút)

### Bước 1: Clone repository

```bash
git clone <repository-url>
cd dataops-lab
```

### Bước 2: Tạo file `.env`

File `.env` chứa credentials và KHÔNG được commit lên Git. Chúng tôi cung cấp file mẫu để bạn bắt đầu.

```bash
# Copy file mẫu thành file .env thực tế
cp .env.example .env
```

> 📝 Nội dung file `.env` mặc định đã sẵn sàng để chạy Lab. Bạn không cần sửa gì thêm trừ khi muốn thay đổi credentials.

File `.env` chứa credentials và KHÔNG được commit lên Git.

Nội dung tối thiểu:

```dotenv
# MySQL Source Credentials
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=sales_db
MYSQL_USER=datauser
MYSQL_PASSWORD=datapassword

# PostgreSQL Target Credentials
POSTGRES_USER=datawarehouse
POSTGRES_PASSWORD=dwhpassword
POSTGRES_DB=analytics_dwh

# Dagster Metadata PostgreSQL Credentials
DAGSTER_METADATA_USER=dagster_metadata
DAGSTER_METADATA_PASSWORD=metadata_password_change_me
DAGSTER_METADATA_DB=dagster_metadata

# Healthcheck behavior
HEALTHCHECK_HTTP_TIMEOUT_SECONDS=5
HEALTHCHECK_DB_TIMEOUT_SECONDS=5
DAGSTER_DAEMON_HEARTBEAT_MAX_STALE_SECONDS=120

# Optional data plane healthcheck thresholds
DATA_HEALTH_MIN_EXPECTED_ROWS=1
DATA_HEALTH_MAX_AGE_HOURS=26
```

### Bước 3: Khởi động hệ thống

```bash
# Build images và start tất cả services
# --build: Bắt buộc build lại image (vì code được COPY vào image)
# -d: Chạy ở chế độ background
docker compose up --build -d
```

Lần đầu tiên chạy sẽ mất khoảng 3-5 phút để download base images và cài đặt Python packages. Các lần sau sẽ nhanh hơn nhờ Docker layer caching.

### Bước 4: Kiểm tra hệ thống đã sẵn sàng chưa

```bash
# Xem trạng thái các containers
docker compose ps
```

| NAME | STATUS | Vai trò |
|---|---|---|
| dataops-mysql-source | Up (healthy) | Source DB |
| dataops-postgres-target | Up (healthy) | Target DB |
| dataops-dagster-metadata | Up (healthy) | Dagster Metadata DB |
| dataops-dagster-platform | Up (healthy) | Dagster Webserver / UI |
| dataops-dagster-daemon | Up (healthy) | Dagster Daemon / Run Execution |

>💡 Service `dagster-daemon` không có healthcheck HTTP vì nó không chạy webserver. Docker chỉ báo `Up` (không có `(healthy)`), điều này là bình thường.  
> Ban đầu, một số service có thể mất thời gian để trở thành healthy do healthcheck có `start_period`. Nếu `dagster-daemon` chưa healthy ngay, hãy chờ hết `start_period` và kiểm tra log nếu nó vẫn unhealthy.
### Bước 5: Truy cập Dagster UI

Mở trình duyệt và vào:

```
http://localhost:3000
```

Bạn sẽ thấy giao diện Dagster với 3 Assets trong group `orders_pipeline`.

### Bước 6: Chạy Pipeline đầu tiên

> 💡 **Lưu ý về concurrency:**
> Hệ thống hiện tại đang cấu hình **single-flight execution**. Nếu bạn hoặc người khác trigger nhiều run cùng lúc:
> - Run đầu tiên sẽ chạy.
> - Các run tiếp theo có thể ở trạng thái `Queued`.
> - Đây là hành vi bình thường, không phải lỗi.
> - Daemon sẽ dequeue run kế tiếp sau khi run hiện tại kết thúc.
>
> Nếu một run nằm ở trạng thái `Queued` quá lâu, hãy kiểm tra service `dagster-daemon`.
1. Trong Dagster UI, vào tab **Assets** ở sidebar trái.
2. Chọn view **Asset Graph** (biểu tượng đồ thị, không phải view List).
3. Bạn sẽ thấy 3 asset được nối với nhau:
   `validate_orders_schema` → `orders_staging` → `orders_production`
4. Click nút **"Materialize all"** ở góc trên bên phải.

   > 💡 **Vì sao phải dùng "Materialize all"?**
   > Nếu bạn click riêng vào `orders_production` và bấm `Materialize`, Dagster chỉ chạy asset đó và cảnh báo "upstream has not been materialized". Atomic Swap sẽ fail vì bảng staging chưa tồn tại.
   > **"Materialize all" đảm bảo cả chuỗi chạy đúng thứ tự dependency.**
5. Quan sát Run mới xuất hiện ở tab **Runs**. Click vào Run để xem 3 step lần lượt chuyển sang trạng thái **Succeeded**.
✅ **Thành công khi bạn thấy:**

```text
✅ Data Contract validation PASSED!
✅ Loaded 10 rows into orders_staging.
✅ Atomic Swap COMPLETED! Dữ liệu mới đã sẵn sàng cho CEO.
```

---
## Healthcheck trong lab

Lab này sử dụng healthcheck nâng cao hơn mức kiểm tra container còn sống.

| Service | Healthcheck | Ý nghĩa |
|---|---|---|
| mysql-source | Authenticated `SELECT 1` | MySQL sống, user và database dùng được |
| postgres-target | Authenticated `SELECT 1` | PostgreSQL sống, user và database dùng được |
| dagster-metadata | Authenticated `SELECT 1` | Metadata DB sống, user Dagster dùng được |
| dagster-platform | HTTP `/health` + metadata DB check | Webserver sẵn sàng và metadata DB truy cập được |
| dagster-daemon | Daemon heartbeat freshness | Daemon còn sống và đang gửi heartbeat |

> Lưu ý:

- Container healthy không có nghĩa là dữ liệu chắc chắn mới.
- Container healthy không có nghĩa là SLO freshness đã đạt.
- Docker Compose healthcheck không tự động restart container nếu container bị unhealthy.
---

## 📁 Cấu trúc thư mục
```text
dataops-lab/
 ├── README.md                       # Tài liệu dành cho người dùng (Data Eng, Analyst)
 ├── ARCHITECTURE.md                 # Tài liệu kiến trúc dành cho Platform/SRE
 ├── CODEOWNERS                      # Quy định ownership và review policy (Git)
 ├── docker-compose.yml              # Định nghĩa hạ tầng
 ├── .env / .env.example             # Secrets (KHÔNG commit)
 ├── .gitignore / .dockerignore      # Loại trừ files khỏi Git / Docker image
 │
 ├── docs/                           # Tài liệu nghiệp vụ & quy trình
 │   ├── contracts/
 │   │   └── orders.md               # 📄 Bản hợp đồng dữ liệu (dành cho con người đọc)
 │   ├── processes/
 │   │   └── data-contract-change-process.md # 📋 Quy trình thay đổi contract
 │   └── reliability/
 │       └── mini-prr-orders-pipeline.md
 │
 ├── seed_data/                      # Dữ liệu khởi tạo tự động
 │   ├── mysql_init.sql
 │   └── postgres_init.sql
 │
 ├── platform/                       # [PLATFORM TEAM] Hạ tầng & Cấu hình
 │   └── dagster/
 │       ├── Dockerfile
 │       ├── requirements.txt
 │       ├── dagster.yaml
 │       ├── workspace.yaml
 │       └── healthchecks/
 │
 ├── tests/                          # Automated Tests
 │   └── contracts/
 │       └── test_order_contract.py  # Unit test cho Data Contract
 │
 └── user_code/                      # [DATA TEAM] Logic nghiệp vụ dữ liệu
     ├── __init__.py
     ├── definitions.py              # Entry point: Tổng hợp Assets & Jobs
     │
     ├── contracts/                  # Data Contracts (Internal Products)
     │   ├── __init__.py
     │   ├── order_contract.py       # Định nghĩa Contract (Metadata, Policy, Schema)
     │   └── registry.py             # Contract Registry (Danh mục hợp đồng)
     │
     └── assets/                     # Data Assets (Pipeline logic)
         ├── __init__.py
         ├── orders.py               # MySQL -> Staging -> Production
         └── contract_registry.py    # Asset kiểm tra tính hợp lệ của Registry

**Quy tắc:**

- Thư mục `platform/`: Chỉ Platform Team được phép sửa đổi.
- Thư mục `user_code/`: Data Team tự do phát triển. Không cần xin phép Platform Team để thêm asset mới.

---

## 💻 Sử dụng hàng ngày

### 🎯 Chạy Pipeline

**Cách 1: Qua Dagster UI (Khuyến nghị)**

1. Vào `http://localhost:3000`
2. Tab **Assets** → chọn group `orders_pipeline`
3. Bấm **Materialize all**

Hoặc chạy qua Job:

1. Vào tab **Jobs**
2. Chọn `orders_pipeline_job`
3. Bấm **Launch Run**

Khuyến nghị dùng **Materialize all** hoặc **Job** để đảm bảo toàn bộ pipeline chạy đúng thứ tự dependency.

**Cách 2: Qua CLI (Dành cho automation/testing)**

```bash
# Chạy một asset cụ thể bằng Dagster CLI
docker compose exec dagster-platform dagster asset materialize -m user_code -s orders_production
```

### 🔍 Xem Lineage & Logs

**Lineage (Phả hệ dữ liệu):**

- Trong Dagster UI, vào tab **"Asset Graph"**
- Bạn sẽ thấy sơ đồ: `validate_orders_schema` → `orders_staging` → `orders_production`
- Nếu một asset fail, các asset downstream sẽ hiển thị trạng thái **"At Risk"**
- Đây chính là cách xác định Blast Radius của sự cố

**Logs chi tiết:**

- Click vào một Run trong tab **"Runs"**
- Click vào từng Step để xem log chi tiết
- Logs hiển thị realtime khi pipeline đang chạy

### ➕ Thêm Data Asset mới

Để thêm một pipeline mới (ví dụ: pipeline cho bảng `customers`):

**Bước 1:** Tạo file asset mới

```
user_code/assets/customers.py
```

**Bước 2:** Định nghĩa asset trong file đó

```python
from dagster import asset

@asset
def customers_source():
    """Asset mới cho bảng customers."""
    pass
```

**Bước 3:** Đăng ký vào `definitions.py`

```python
# user_code/definitions.py
from user_code.assets.customers import customers_source

defs = Definitions(
    assets=[
        # ... assets cũ
        customers_source,  # Thêm asset mới vào đây
    ],
)
```

**Bước 4:** Build lại image (vì chúng ta dùng `COPY`, không mount volume)

```bash
docker compose up --build -d
```

> ⚠️ **Lưu ý quan trọng:** Vì chúng ta đang ở Production Mode (code `COPY` vào image), mọi thay đổi code đều yêu cầu build lại image. Nếu bạn muốn tăng tốc độ phát triển, hãy xem mục [FAQ: Làm sao để code nhanh hơn?](#faq)

### 📋 Thay đổi Data Contract

Data Contract trong hệ thống này không chỉ là một file cấu hình kỹ thuật, mà là một **Sản phẩm nội bộ** có Owner, Consumer, Version và Chính sách tương thích (Compatibility Policy).

Khi Source Database (MySQL) thay đổi schema, quy trình bắt buộc như sau:

1. **Phân loại thay đổi**:
   - **Non-breaking (Additive)**: Thêm cột mới, tăng độ dài `varchar`, tăng `decimal` precision (giữ nguyên scale). Pipeline sẽ vẫn chạy nhưng có thể xuất hiện Warning.
   - **Breaking**: Xóa cột bắt buộc, đổi tên cột, đổi kiểu dữ liệu không tương thích, giảm độ dài `varchar`. Pipeline sẽ **FAIL-FAST** ngay lập tức.

2. **Quy trình cập nhật**:
   - Đọc chi tiết quy trình tại: `docs/processes/data-contract-change-process.md`
   - Cập nhật định nghĩa trong `user_code/contracts/order_contract.py`.
   - Bump version contract (Semantic Versioning).
   - Cập nhật tài liệu dành cho con người tại `docs/contracts/orders.md`.
   - Đảm bảo `tests/contracts/test_order_contract.py` pass.
   - Mở Pull Request và yêu cầu review từ các bên liên quan (theo `CODEOWNERS`).

3. **Chạy kiểm tra Governance**:
   - Trong Dagster UI, chạy job `contract_governance_job` (hoặc asset `contract_registry`) để đảm bảo registry không bị lỗi metadata.
   - Build lại image và chạy pipeline để kiểm chứng.

> ⚠️ **CẢNH BÁO**: Không bao giờ thay đổi schema Source mà không thông báo và cập nhật Data Contract. Hệ thống được thiết kế để **Fail-Fast over Silent Corruption**. Việc pipeline chặn dữ liệu bẩn là hành vi có chủ đích để bảo vệ Data Analysts và CEO.
---

## 🗄️ Quản lý dữ liệu

### 🔄 Reset dữ liệu về trạng thái ban đầu

Khi bạn muốn "làm mới" hoàn toàn môi trường (xóa mọi dữ liệu đã insert, chạy lại seed data):

```bash
# Dừng containers và XÓA volumes
# Cờ -v rất quan trọng: nó xóa mysql_data và postgres_data
docker compose down -v

# Khởi động lại từ đầu
docker compose up --build -d
```

> ⚠️ **Cảnh báo:** Lệnh này sẽ XÓA TOÀN BỘ dữ liệu trong MySQL và PostgreSQL. Chỉ dùng trong môi trường Lab.

### 🔌 Kết nối trực tiếp Database (Debug)

> ⚠️ Chỉ dùng cho mục đích Debug trong Lab. Trong Production, không bao giờ kết nối trực tiếp vào Database.

**MySQL Source:**

```bash
# Kết nối qua Docker exec
docker exec -it dataops-mysql-source mysql -udatauser -pdatapassword sales_db

# Hoặc dùng DBeaver/MySQL Workbench với thông tin:
# Host: localhost
# Port: 3306
# User: datauser
# Password: datapassword
# Database: sales_db
```

**PostgreSQL Target:**

```bash
# Kết nối qua Docker exec
docker exec -it dataops-postgres-target psql -U datawarehouse -d analytics_dwh

# Hoặc dùng DBeaver/pgAdmin với thông tin:
# Host: localhost
# Port: 5432
# User: datawarehouse
# Password: dwhpassword
# Database: analytics_dwh
```

**Kiểm tra dữ liệu sau khi chạy pipeline:**

```sql
-- Xem các bảng hiện có
\dt

-- Kỳ vọng:
-- orders_production
-- orders_production_backup (có thể có nếu đã có production trước đó)

-- Kiểm tra dữ liệu trong bảng production
SELECT COUNT(*) FROM orders_production;

SELECT *
FROM orders_production
ORDER BY created_at DESC
LIMIT 10;
```

Sau một run thành công:

- Không nên còn bảng `orders_stg_*`.
- Có thể còn bảng `orders_production_backup`.
- `orders_production_backup` là dữ liệu của thế hệ production trước.

---

## 🔧 Troubleshooting

### Lỗi: Connection Refused khi Dagster kết nối MySQL/PostgreSQL

**Nguyên nhân:** Dagster khởi động trước khi Database sẵn sàng.

**Giải pháp:**

1. Đảm bảo bạn đã cấu hình `healthcheck` và `depends_on` với `condition: service_healthy` trong `docker-compose.yml`.
2. Kiểm tra logs của Database:

```bash
   docker compose logs mysql-source
   docker compose logs postgres-target
```

3. Chờ đến khi thấy dòng `ready for connections` trong logs.

### Lỗi: DATA CONTRACT VIOLATION khi chạy pipeline

**Nguyên nhân:** Schema trong MySQL Source không khớp với Data Contract.

**Giải pháp:**

1. Đọc kỹ thông báo lỗi trong Dagster UI để biết cột nào bị sai lệch.
2. Kiểm tra schema thực tế:

```bash
   docker exec -it dataops-mysql-source mysql -udatauser -pdatapassword sales_db -e "DESCRIBE orders;"
```

3. So sánh với `EXPECTED_ORDERS_SCHEMA` trong `user_code/contracts/order_contract.py`.
4. Nếu Source thay đổi hợp lệ, cập nhật Data Contract. Nếu Source thay đổi sai, liên hệ team Source để rollback.

### Lỗi: Port 3000 already in use

**Nguyên nhân:** Một ứng dụng khác đang chạy trên port 3000.

**Giải pháp:**

```bash
# Tìm process đang chiếm port 3000
lsof -i :3000

# Hoặc thay đổi port trong docker-compose.yml
# Sửa dòng: - "3000:3000" thành - "3001:3000"
# Sau đó truy cập http://localhost:3001
```

### Lỗi: Seed data không được load

**Nguyên nhân:** Volume đã tồn tại từ trước, nên MySQL không chạy lại init script.

**Giải pháp:**

```bash
# Xóa volumes để force chạy lại init script
docker compose down -v
docker compose up --build -d
```

### Pipeline chạy thành công nhưng không có dữ liệu trong `orders_production`

**Nguyên nhân có thể:**

- Bảng `orders` trong MySQL không có dữ liệu.
- Atomic Swap bị fail giữa chừng.

**Giải pháp:**

1. Kiểm tra MySQL có dữ liệu không:

```bash
   docker exec -it dataops-mysql-source mysql -udatauser -pdatapassword sales_db -e "SELECT COUNT(*) FROM orders;"
```

2. Kiểm tra logs của asset `orders_production` trong Dagster UI.
3. Kiểm tra PostgreSQL có bảng staging orphan hoặc legacy staging không:

```bash
   docker exec -it dataops-postgres-target psql -U datawarehouse -d analytics_dwh -c "\dt
```
4. Ngoài ra kiểm tra bảng legacy cũ:
```bash
   docker exec -it dataops-postgres-target psql -U datawarehouse -d analytics_dwh -c "\dt orders_staging"
```
5. Sau một run thành công, không nên còn bảng staging.

### Run ở trạng thái Queued

**Hiện tượng:** Bấm Materialize, run xuất hiện nhưng ở trạng thái `Queued`.

Có hai trường hợp:

#### Trường hợp 1: Queued bình thường

Nếu đã có một run khác đang chạy, run mới sẽ chờ. Đây là hành vi đúng vì hệ thống đang cấu hình single-flight execution:

```yaml
max_concurrent_runs: 1
```

Chờ run hiện tại hoàn thành, daemon sẽ dequeue run kế tiếp.

#### Trường hợp 2: Queued bất thường

Nếu không có run nào đang chạy nhưng run vẫn `Queued` lâu hơn vài phút.

**Nguyên nhân có thể:**

- `dagster-daemon` không chạy.
- Daemon bị crash.
- Daemon mất kết nối với metadata database.
- Metadata database unhealthy.

**Giải pháp:**

```bash
docker compose ps
docker compose logs dagster-daemon --tail=50
docker compose logs dagster-metadata --tail=50
```

**Kỳ vọng:**

- Container `dataops-dagster-daemon` đang `Up`.
- Log daemon không có lỗi fatal.
- Không có cảnh báo split-brain kéo dài.

---

### Lỗi: `'cryptography' package is required for caching_sha2_password`

**Hiện tượng:** Asset `validate_orders_schema` fail ngay khi kết nối MySQL, các asset downstream không chạy.

**Nguyên nhân:** MySQL 8 mặc định dùng auth plugin `caching_sha2_password`. PyMySQL (driver Python) cần thư viện `cryptography` để thực hiện handshake RSA trên kết nối non-TLS. Nếu `requirements.txt` thiếu package này, connect sẽ fail.

**Giải pháp:**

1. Kiểm tra `platform/dagster/requirements.txt` có dòng:

```text
   cryptography==42.0.8
```

2. Nếu thiếu, thêm vào và build lại image:

```bash
   docker compose up -d --build dagster-platform dagster-daemon
```

> ⚠️ **Không khuyến nghị** đổi MySQL user sang `mysql_native_password`. Plugin này đã deprecated từ MySQL 8.0 và bị gỡ ở MySQL 8.4. Sửa đúng là bổ sung dependency phía client.
---
### Lỗi: Dagster vẫn dùng SQLite sau khi đã chuyển sang PostgreSQL

Nguyên nhân:

- Volume `dagster_storage` cũ vẫn còn chứa `dagster.yaml` SQLite.
- Container chưa được build/recreate sau khi đổi config.
- Biến môi trường Dagster metadata chưa được truyền đủ vào cả `dagster-platform` và `dagster-daemon`.

Giải pháp:

1. Đảm bảo `platform/dagster/dagster.yaml` đang dùng PostgreSQL storage.
2. Đảm bảo `docker-compose.yml` có service `dagster-metadata`.
3. Đảm bảo cả `dagster-platform` và `dagster-daemon` có các biến:
   - `DAGSTER_METADATA_HOST`
   - `DAGSTER_METADATA_USER`
   - `DAGSTER_METADATA_PASSWORD`
   - `DAGSTER_METADATA_DB`
4. Xóa volume `dagster_storage` nếu nghi ngờ config cũ còn tồn tại.
5. Build và recreate lại toàn bộ lab.

### Xuất hiện bảng orders_stg_* trong PostgreSQL
**Hiện tượng:** Trong PostgreSQL target có bảng dạng:
```
orders_stg_20260616123045_ab12cd34
```
Đây là bảng staging theo run.

**Hành vi bình thường:**

- Trong khi run đang chạy, bảng staging có thể tồn tại.
- Sau khi run thành công, bảng staging được rename thành `orders_production`.
- Sau run kế tiếp, staging orphan từ run cũ sẽ được cleanup.

**Hành vi bất thường:**

- Nếu run đã `succeeded` nhưng vẫn còn bảng `orders_stg_*`, cần kiểm tra lại log run.
- Nếu run `failed` và staging còn tồn tại, run kế tiếp sẽ cleanup.
- Không nên xóa tay bảng staging khi chưa xác định run liên quan, trừ khi bạn chắc chắn đó là orphan.


---
### Xuất hiện bảng `orders_production_backup`

Đây là hành vi có chủ đích.

Hệ thống giữ lại một thế hệ backup của `orders_production` để hỗ trợ rollback ngắn hạn.

Bảng này:

- Không phải bảng production.
- Không nên được dùng trực tiếp cho dashboard.
- Sẽ bị drop ở lần swap kế tiếp.

Nếu bạn cần rollback khẩn cấp, tham khảo phần Disaster Recovery trong `ARCHITECTURE.md`.

### Lỗi: `dagster-metadata` unhealthy

Triệu chứng:

- Container `dataops-dagster-metadata` bị unhealthy.
- Các service phụ thuộc như `dagster-platform` và `dagster-daemon` không start được.
- Xuất hiện lỗi `dependency failed to start`.

Nguyên nhân thường gặp:

1. Healthcheck đang dùng biến môi trường không tồn tại bên trong container.
2. Volume `dagster_metadata_data` đã được khởi tạo từ trước với credentials khác.
3. PostgreSQL metadata chưa sẵn sàng trong giai đoạn khởi động.
4. User hoặc database được khai báo trong `.env` không khớp với volume hiện tại.

Cách xử lý:

- Kiểm tra log của `dagster-metadata` để biết lỗi thật.
- Đảm bảo service `dagster-metadata` có các biến môi trường cần thiết cho healthcheck.
- Nếu log cho thấy lỗi authentication, role không tồn tại hoặc database không tồn tại, khả năng cao volume cũ không khớp với credentials hiện tại.
- Trong lab, có thể reset volume để khởi tạo lại từ đầu. Trong môi trường có dữ liệu quan trọng, phải backup trước khi reset.


### Lỗi: Contract registry validation failed
**Hiện tượng**: Asset `contract_registry` hoặc job `contract_governance_job` bị fail.  
**Nguyên nhân:** 
- Contract thiếu các metadata bắt buộc (owner, consumer, version, policy...).
- Registry bị trùng `contract_id`.
- Khai báo registry không khớp với `contract_id` bên trong file contract.  
**Giải pháp:**
Đọc log chi tiết trong Dagster UI để biết field nào đang bị thiếu hoặc sai. Sửa lại `user_code/contracts/order_contract.py` hoặc `registry.py`.

### Lỗi: DATA CONTRACT VIOLATION (Breaking Change)
**Hiện tượng:** Pipeline fail ở step `validate_orders_schema`. Log hiển thị `❌ DATA CONTRACT VIOLATION!`.  
**Nguyên nhân:** Source schema có thay đổi breaking (ví dụ: mất cột `amount`, đổi `varchar(50)` thành `varchar(20)`).  
**Giải pháp:**
1. Đọc log để biết chính xác cột và kiểu dữ liệu bị vi phạm.
2. Liên hệ Source Team (Producer) để xác nhận xem đây là lỗi accident hay thay đổi có chủ đích.
3. Nếu là thay đổi có chủ đích, thực hiện quy trình "Thay đổi Data Contract" để bump version và cập nhật policy.
---

## ❓ FAQ

**Q: Tại sao tôi phải build lại image mỗi khi sửa code?**

A: Vì chúng ta đang chạy ở Production Mode. Code được `COPY` vào Docker image để đảm bảo tính bất biến (Immutability). Điều này giúp môi trường chạy luôn nhất quán và có thể rollback dễ dàng.

**Q: Làm sao để code nhanh hơn trong quá trình phát triển?**

A: Bạn có thể chuyển sang Development Mode bằng cách dùng Bind Mount. Mở `docker-compose.yml`, tìm service `dagster-platform`, và thêm:

```yaml
volumes:
  - ./user_code:/opt/dagster/user_code
  - ./platform:/opt/dagster/platform
```

Sau đó comment phần `build:` lại và thay bằng image có sẵn. Lưu ý: Chỉ dùng cách này trong quá trình phát triển. Khi deploy Production, phải quay lại Production Mode.

**Q: Tôi có thể chạy pipeline tự động theo lịch không?**

A: Có. Dagster hỗ trợ Schedules. Hiện tại Lab này chưa cấu hình schedule, nhưng bạn có thể thêm vào `definitions.py`. Tham khảo [Dagster Schedules Documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules).

**Q: Làm sao để thêm một Source Database mới?**

A: Bạn cần:

1. Thêm service mới vào `docker-compose.yml` (ví dụ: `postgres-source-2`).
2. Tạo Data Contract mới trong `user_code/contracts/`.
3. Tạo Assets mới trong `user_code/assets/`.
4. Đăng ký vào `definitions.py`.
5. Build lại image.

**Q: Dữ liệu có bị mất khi tôi tắt máy không?**

A: Không, nếu bạn chỉ tắt máy thông thường. Dữ liệu được lưu trong Docker Named Volumes. Tuy nhiên, nếu bạn chạy `docker compose down -v`, volumes sẽ bị xóa.

**Q: Tôi muốn xem lịch sử các lần chạy pipeline?**

A: Vào Dagster UI → Tab **"Runs"**. Tất cả lịch sử chạy, logs, thời gian, trạng thái đều được lưu ở đây.


**Q: Vì sao run của tôi ở trạng thái Queued?**

A: Vì hệ thống đang cấu hình single-flight execution. Nếu đã có một run đang chạy, run mới sẽ chờ. Đây là hành vi bình thường. Nếu không có run nào chạy mà run vẫn Queued lâu, hãy kiểm tra `dagster-daemon`.


**Q: Vì sao có bảng `orders_production_backup`?**

A: Hệ thống giữ một thế hệ backup của bảng production để hỗ trợ rollback ngắn hạn. Bảng này sẽ bị thay thế hoặc drop ở lần swap kế tiếp.


**Q: Vì sao có bảng `orders_stg_*`?**

A: Đây là bảng staging theo run. Mỗi pipeline run tạo một staging table riêng để tránh conflict và hỗ trợ forensic. Sau run thành công, staging được swap thành production. Nếu run crash, staging orphan sẽ được cleanup ở run kế tiếp.


**Q: Tôi có nên retry run cũ không?**

A: Nếu run cũ `failed` và chưa có run mới thay thế, bạn có thể retry. Nhưng nếu đã có run mới chạy thành công, không nên retry run cũ. Hãy tạo một run mới để tránh dùng staging cũ không rõ nguồn gốc.

**Q: Vì sao source trả về 0 rows làm pipeline fail?**

A: Hiện tại lab cấu hình `MIN_EXPECTED_ROWS = 1` để fail-fast khi source bất thường. Nếu nghiệp vụ cho phép source rỗng, hãy đổi `MIN_EXPECTED_ROWS = 0` trong `user_code/contracts/order_contract.py`.


**Q: Vì sao `dagster-daemon` bây giờ có trạng thái healthy?**

Vì daemon đã có healthcheck dựa trên heartbeat trong metadata database. Healthcheck này không dùng HTTP, mà kiểm tra xem daemon còn gửi heartbeat mới hay không.

**Q: Vì sao healthcheck database không dùng `ping` hoặc `pg_isready` nữa?**

Vì các lệnh đó chỉ kiểm tra process/database có đang sống. Chúng không kiểm tra được user, password, database và quyền query tối thiểu. Healthcheck mới dùng authenticated query để gần với hành vi thật của ứng dụng hơn.

**Q: Container unhealthy có tự động restart không?**

Trong Docker Compose, healthcheck chủ yếu dùng để theo dõi trạng thái và hỗ trợ startup dependency. Nó không tự động restart container chỉ vì container bị unhealthy. Nếu cần tự phục hồi mạnh hơn, cần watchdog bên ngoài hoặc Kubernetes probes.

**Q: Vì sao `.env` có biến nhưng container vẫn không thấy biến đó?**

Vì `.env` chỉ cung cấp giá trị cho Docker Compose interpolation. Biến chỉ xuất hiện bên trong container nếu được khai báo trong phần `environment` của service tương ứng.

**Q: Khi nào cần reset volumes?**

Khi volume cũ được khởi tạo với credentials khác, hoặc khi bạn muốn đưa lab về trạng thái seed data ban đầu. Reset volumes sẽ xóa dữ liệu hiện tại, chỉ nên dùng trong lab hoặc khi đã backup.
  
**Q: Data Contract khác gì với Schema Validation thông thường?**  

Schema Validation chỉ là kỹ thuật (so khớp cột và kiểu dữ liệu). Data Contract là một thỏa thuận giữ


**Q: Vì sao Source thêm cột mới mà Pipeline chỉ Warning chứ không Fail?**  

Hệ thống tuân theo policy `backward_compatible_additive_only`. Việc Source thêm cột mới không làm hỏng logic ETL hiện tại. Tuy nhiên, Warning được sinh ra để nhắc nhở Data Engineer cập nhật Contract nếu cột mới đó cần được đưa vào Production.

**Q: Ai là người sở hữu (Owner) Data Contract?**  
 
Contract là tài sản chung. Source Team sở hữu dữ liệu gốc, Platform Team sở hữu hạ tầng thực thi, và Data Engineering / Business sở hữu logic tiêu thụ. Mọi thay đổi breaking đều cần sự đồng thuận (thể hiện qua quy trình Review trong `CODEOWNERS`).

---

## 🤝 Liên hệ & Hỗ trợ

| Vai trò | Trách nhiệm | Liên hệ |
|---------|-------------|---------|
| Platform Team | Hạ tầng, Docker, Dagster, Database | `#platform-support` (Slack) |
| Data Engineering Lead | Data Contracts, Asset definitions, Business Logic | `#data-engineering` (Slack) |
| SRE On-call | Sự cố Production, SLO breach | `#sre-oncall` (Slack) |

**Quy trình báo lỗi:**

1. Kiểm tra [Troubleshooting](#-troubleshooting) và [FAQ](#-faq) trước.
2. Nếu không giải quyết được, chụp màn hình Dagster UI (tab Runs, logs chi tiết).
3. Đăng vào kênh Slack phù hợp với mô tả lỗi, kèm theo:
   - Lệnh bạn đã chạy
   - Thông báo lỗi đầy đủ
   - Kỳ vọng của bạn

---

## 📚 Tài liệu tham khảo

- [ARCHITECTURE.md](./ARCHITECTURE.md) - Chi tiết kiến trúc và các quyết định thiết kế
- [mini-prr-orders-pipeline.md](./docs/reliability/mini-prr-orders-pipeline.md) - Mini-PRR cho orders pipeline
- [Dagster Documentation](https://docs.dagster.io/)
- [Docker Compose Specification](https://docs.docker.com/compose/)
- DataOps Principles

---

*Cập nhật lần cuối: 08/2026 | Duy trì bởi Platform Engineering Team*