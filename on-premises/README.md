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
|-----------|--------------------------|
| **Idempotency** | Atomic Swap (Pattern C) - Zero-downtime data deployment |
| **Data Contracts** | Fail-fast Schema Validation trước khi ETL chạy |
| **Observability** | Dagster UI với Lineage, Logs, Metadata |
| **Self-serve** | Seed data tự động, không cần nhờ Platform Team |
| **Blast Radius Control** | Tách biệt Services, Healthchecks, Dependency Management |

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
┌──────────────▼──────────┐    ┌──────────────▼──────────┐
│  MySQL Source (3306)     │    │  PostgreSQL Target      │
│  sales_db.orders         │    │  analytics_dwh          │
│  (10 rows seed data)     │    │  orders_staging         │
│                          │    │  orders_production      │
└──────────────────────────┘    └──────────────────────────┘
```

**Luồng dữ liệu:**

1. **Validate:** Kiểm tra schema MySQL có khớp Data Contract không.
2. **Extract & Load:** Đọc dữ liệu từ MySQL → Ghi vào bảng `orders_staging` trong PostgreSQL.
3. **Atomic Swap:** Đổi tên `orders_staging` → `orders_production` trong một transaction. Nếu lỗi, ROLLBACK, dữ liệu cũ vẫn an toàn.

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

Nếu bạn cần tạo thủ công, nội dung file `.env` như sau:

```bash
# MySQL Source Credentials
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=sales_db
MYSQL_USER=datauser
MYSQL_PASSWORD=datapassword

# PostgreSQL Target Credentials
POSTGRES_USER=datawarehouse
POSTGRES_PASSWORD=dwhpassword
POSTGRES_DB=analytics_dwh
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

| NAME                        | STATUS                     | Vai trò                                  |
|-----------------------------|----------------------------|------------------------------------------|
| dataops-mysql-source        | Up (healthy)               | Source DB                                |
| dataops-postgres-target     | Up (healthy)               | Target DB                                |
| dataops-dagster-platform    | Up (healthy)               | Dagster Webserver (UI)                   |
| dataops-dagster-daemon      | Up                         | Dagster Daemon (thực thi runs)           |

> 💡 Service `dagster-daemon` không có healthcheck HTTP vì nó không chạy
> webserver. Docker chỉ báo `Up` (không có `(healthy)`), điều này là bình thường.

### Bước 5: Truy cập Dagster UI

Mở trình duyệt và vào:

```
http://localhost:3000
```

Bạn sẽ thấy giao diện Dagster với 3 Assets trong group `orders_pipeline`.

### Bước 6: Chạy Pipeline đầu tiên

1. Trong Dagster UI, vào tab **Assets** ở sidebar trái.
2. Chọn view **Asset Graph** (biểu tượng đồ thị, không phải view List).
3. Bạn sẽ thấy 3 asset được nối với nhau:
   `validate_orders_schema` → `orders_staging` → `orders_production`
4. Click nút **"Materialize all"** ở góc trên bên phải.
   > 💡 **Vì sao phải dùng "Materialize all"?**
   > Nếu bạn click riêng vào `orders_production` và bấm `Materialize`,
   > Dagster chỉ chạy asset đó và cảnh báo "upstream has not been materialized".
   > Atomic Swap sẽ fail vì bảng staging chưa tồn tại.
   > **"Materialize all" đảm bảo cả chuỗi chạy đúng thứ tự dependency.**
5. Quan sát Run mới xuất hiện ở tab **Runs**. Click vào Run để xem 3 step
   lần lượt chuyển sang trạng thái **Succeeded**.

✅ **Thành công khi bạn thấy:**

```
✅ Data Contract validation PASSED!
✅ Loaded 10 rows into orders_staging.
✅ Atomic Swap COMPLETED! Dữ liệu mới đã sẵn sàng cho CEO.
```

---

## 📁 Cấu trúc thư mục

```
dataops-lab/
├── README.md                       # File bạn đang đọc
├── ARCHITECTURE.md                 # Tài liệu kiến trúc chi tiết
├── docker-compose.yml              # Định nghĩa hạ tầng
├── .env                            # Secrets (KHÔNG commit)
├── .env.example                    # Mẫu để tạo .env
├── .gitignore                      # Loại trừ files khỏi Git
├── .dockerignore                   # Loại trừ files khỏi Docker image
│
├── seed_data/                      # Dữ liệu khởi tạo tự động
│   ├── mysql_init.sql              # Tạo bảng + data mẫu cho MySQL
│   └── postgres_init.sql           # Khởi tạo schema cho PostgreSQL
│
├── platform/                       # [PLATFORM TEAM] Hạ tầng & Cấu hình
│   └── dagster/
│       ├── Dockerfile              # Build Dagster image
│       ├── requirements.txt        # Python dependencies
│       ├── dagster.yaml            # Cấu hình Dagster Instance
│       └── workspace.yaml          # Khai báo User Code location
│
└── user_code/                      # [DATA TEAM] Logic nghiệp vụ dữ liệu
    ├── __init__.py
    ├── definitions.py              # Entry point: Tổng hợp tất cả Assets
    │
    ├── contracts/                  # Data Contracts
    │   ├── __init__.py
    │   └── order_contract.py       # Schema kỳ vọng cho bảng orders
    │
    └── assets/                     # Data Assets (Pipeline logic)
        ├── __init__.py
        └── orders.py               # MySQL -> Staging -> Production
```

**Quy tắc:**

- Thư mục `platform/`: Chỉ Platform Team được phép sửa đổi.
- Thư mục `user_code/`: Data Team tự do phát triển. Không cần xin phép Platform Team để thêm asset mới.

---

## 💻 Sử dụng hàng ngày

### 🎯 Chạy Pipeline

**Cách 1: Qua Dagster UI (Khuyến nghị)**

1. Vào `http://localhost:3000`
2. Tab **Assets** → Chọn asset cần chạy
3. Bấm **"Materialize"**
4. Quan sát logs và lineage trong UI

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

Khi Source Database thay đổi schema, bạn **BẮT BUỘC** phải cập nhật Data Contract trước.

1. Mở file `user_code/contracts/order_contract.py`
2. Cập nhật `EXPECTED_ORDERS_SCHEMA` và `REQUIRED_COLUMNS` để phản ánh schema mới.
3. Build lại và chạy pipeline để kiểm tra.

> ⚠️ **CẢNH BÁO:** Không bao giờ thay đổi schema Source mà không cập nhật Data Contract. Pipeline sẽ FAIL-FAST và chặn toàn bộ dữ liệu bẩn vào Production. Đây là hành vi có chủ đích, không phải bug.

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
-- Trong PostgreSQL
-- Xem các bảng hiện có
\dt

-- Kiểm tra dữ liệu trong bảng production
SELECT COUNT(*) FROM orders_production;
SELECT * FROM orders_production ORDER BY created_at DESC LIMIT 10;
```

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
3. Kiểm tra PostgreSQL có bảng `orders_staging` còn tồn tại không (nếu còn, nghĩa là Swap chưa hoàn thành):

```bash
   docker exec -it dataops-postgres-target psql -U datawarehouse -d analytics_dwh -c "\dt"
```

### Lỗi: Run stuck ở trạng thái "Queued" mãi mãi

**Hiện tượng:** Bấm Materialize, Run được tạo nhưng không bao giờ chuyển sang Running. Tab **Deployment** có icon cảnh báo ⚠️ màu vàng.

**Nguyên nhân:** Dagster Control Plane gồm 2 thành phần độc lập:

- **Webserver** (`dagster-platform`): chỉ nhận yêu cầu từ UI và ghi run vào queue.
- **Daemon** (`dagster-daemon`): vòng lặp nền lấy run từ queue ra để thực thi.

Nếu thiếu Daemon, mọi run sẽ nằm trong queue vĩnh viễn.

**Giải pháp:**

1. Đảm bảo service `dagster-daemon` tồn tại trong `docker-compose.yml`.
2. Kiểm tra logs daemon:

```bash
   docker compose logs dagster-daemon --tail=30
```

3. Kỳ vọng thấy các dòng heartbeat INFO, không có cảnh báo "Another daemon is still sending heartbeats".

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
- [Dagster Documentation](https://docs.dagster.io/)
- [Docker Compose Specification](https://docs.docker.com/compose/)
- DataOps Principles

---

*Cập nhật lần cuối: 08/2026 | Duy trì bởi Platform Engineering Team*