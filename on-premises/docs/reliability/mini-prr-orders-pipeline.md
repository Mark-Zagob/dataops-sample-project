# Mini Production Readiness Review — Orders Pipeline

## 1. Document Control

| Mục | Nội dung |
|-----|----------|
| Tài liệu | Mini Production Readiness Review — Orders Pipeline |
| Pipeline | `orders_pipeline` |
| Môi trường | On-prem Docker Compose |
| Phiên bản | 0.1 |
| Trạng thái | Draft — Pending Review |
| Owner | Platform Engineering Team |
| Co-owner | Data Engineering Lead |
| Reviewer | SRE On-call |
| Ngày cập nhật | Cần điền khi review |
| Lần Game Day gần nhất | Chưa có |

### Mục đích của tài liệu

Tài liệu này dùng để đánh giá mức độ sẵn sàng vận hành production của pipeline orders, bao gồm:

- `validate_orders_schema`
- `orders_staging`
- `orders_production`

Mini-PRR này trả lời các câu hỏi:

- Pipeline này có đủ an toàn để vận hành như một data product không?
- Khi có sự cố, hệ thống fail như thế nào?
- Ai phát hiện sự cố?
- Blast radius là gì?
- Có rollback/recovery không?
- Có SLO/SLI không?
- Những rủi ro nào đã biết nhưng chưa được xử lý?
- Có nên cho pipeline này go-live ở mức business-critical không?

---

## 2. Readiness Statement

### Đánh giá tổng quan

Pipeline orders hiện tại đã thể hiện được nhiều nguyên lý DataOps production-grade:

- Có Data Contract fail-fast.
- Có staging trước production.
- Có Atomic Swap để bảo vệ dữ liệu production khỏi data blackout.
- Có lineage, logs và run history trong Dagster UI.
- Có tách biệt tương đối giữa Platform Team và Data Team.
- Có healthcheck cho database và webserver.
- Có tách Dagster Webserver và Dagster Daemon.

Tuy nhiên, hệ thống vẫn còn nhiều khoảng trống quan trọng trước khi có thể xem là production thật cho dữ liệu business-critical.

### Mức độ sẵn sàng

| Mức sử dụng | Kết luận | Lý do |
|-------------|---------|-------|
| Lab học tập | Go | Phù hợp để học DataOps, Docker Compose, Dagster, Atomic Swap, Data Contract |
| Internal non-critical | Conditional Go | Có thể dùng nội bộ nếu có người giám sát và chấp nhận rủi ro |
| Business-critical / CEO dashboard | No-Go cho đến khi đóng các P0 gaps | Thiếu data quality gate, alerting, backup/restore, retry an toàn và freshness monitoring |

### Tuyên bố readiness

> Pipeline orders hiện đạt mức production-grade foundation, nhưng chưa đạt production-ready đầy đủ cho dữ liệu business-critical. Cần bổ sung data quality gate, xử lý empty source an toàn, retry policy, alerting, backup/restore và runbook trước khi go-live chính thức.

---

## 3. Data Product Definition

Pipeline này không chỉ là một ETL job. Nó tạo ra một data product nội bộ.

| Mục | Nội dung |
|-----|----------|
| Tên data product | `orders_production` |
| Nguồn dữ liệu | Bảng `orders` trong MySQL `sales_db` |
| Nơi chứa dữ liệu production | PostgreSQL `analytics_dwh` |
| Khách hàng nội bộ | CEO, Business Analyst, Data Analyst, Data Scientist |
| Mục đích business | Cung cấp dữ liệu đơn hàng đáng tin cậy cho báo cáo và dashboard |
| Data contract | `user_code/contracts/order_contract.py` |
| Data owner | Data Engineering Lead |
| Platform owner | Platform Engineering Team |
| SRE support | SRE On-call |

### Kỳ vọng business

Dữ liệu `orders_production` cần:

- Đúng schema theo data contract.
- Không bị trống bất thường.
- Không bị trùng khóa chính.
- Không chứa giá trị tài chính âm nếu business không cho phép.
- Được cập nhật đúng hạn trước giờ báo cáo.
- Có thể truy vết nguồn gốc và lịch sử chạy.

### Tác động nếu dữ liệu sai hoặc trễ

| Sự cố | Tác động business |
|-------|---------------------|
| Dữ liệu production bị trống | Dashboard CEO không có dữ liệu, mất niềm tin vào data platform |
| Dữ liệu bị sai | Báo cáo sai, quyết định business sai |
| Dữ liệu bị trễ | Báo cáo không kịp giờ, vi phạm freshness SLO |
| Không biết dữ liệu cũ hay mới | Người dùng không thể tin tưởng dữ liệu |
| Không có lineage | Khó debug, khó audit, khó tuân thủ |

---

## 4. Scope

### Trong phạm vi

Mini-PRR này bao gồm:

- Pipeline `orders_pipeline` trong Dagster.
- Asset `validate_orders_schema`.
- Asset `orders_staging`.
- Asset `orders_production`.
- Data contract cho bảng `orders`.
- Atomic Swap từ `orders_staging` sang `orders_production`.
- Docker Compose environment hiện tại.
- Các service: MySQL source, PostgreSQL target, Dagster Webserver, Dagster Daemon.
- Named volumes: `mysql_data`, `postgres_data`, `dagster_storage`.
- Cấu hình Dagster instance trong `platform/dagster/dagster.yaml`.
- Luồng vận hành thủ công qua Dagster UI.

### Ngoài phạm vi

Các chủ đề sau chưa được đánh giá đầy đủ trong Mini-PRR này:

- CDC realtime.
- Kubernetes.
- AWS ECS/EKS.
- CI/CD hoàn chỉnh.
- Multi-team governance.
- Data catalog.
- Chargeback/showback.
- Backup/restore enterprise.
- Alerting production hoàn chỉnh.
- Authentication/authorization cho Dagster UI.
- Network segmentation nâng cao.
- Secret manager production.

---

## 5. Architecture Summary

### Luồng dữ liệu hiện tại

Pipeline hiện tại gồm ba bước chính:

1. **Contract Validation**
   - Kiểm tra schema bảng `orders` trong MySQL.
   - So sánh với data contract.
   - Fail-fast nếu thiếu cột bắt buộc hoặc sai kiểu dữ liệu cơ bản.
2. **Extract & Load to Staging**
   - Đọc dữ liệu từ MySQL.
   - Ghi vào bảng `orders_staging` trong PostgreSQL.
3. **Atomic Swap**
   - Đổi tên `orders_staging` thành `orders_production`.
   - Nếu có bảng production cũ, production cũ được rename thành backup trong transaction.
   - Nếu swap thành công, backup bị xóa.
   - Nếu lỗi, transaction rollback và production cũ được bảo vệ.

### Thành phần chính

| Thành phần | Vai trò |
|------------|---------|
| `mysql-source` | Source database giả lập hệ thống kinh doanh |
| `postgres-target` | Target database chứa staging và production |
| `dagster-platform` | Dagster Webserver, UI, API |
| `dagster-daemon` | Thành phần thực thi run, schedule, sensor |
| `dagster_storage` | Volume lưu trạng thái Dagster, run history, event logs |
| `user_code` | Logic pipeline, data contract, asset definitions |
| `platform` | Cấu hình Dagster, Dockerfile, requirements |
| `seed_data` | Dữ liệu khởi tạo ban đầu cho lab |

### Pattern production-grade đang có

| Pattern | Hiện trạng |
|---------|-----------|
| Fail-fast data contract | Đã có ở mức schema |
| Staging before production | Đã có |
| Atomic Swap | Đã có |
| Zero-downtime data deployment | Có về mặt thiết kế |
| Same image different command | Đã có cho webserver và daemon |
| Healthcheck | Có cho MySQL, PostgreSQL, Dagster Webserver |
| Separation of concerns | Có giữa `platform/` và `user_code/` |
| Observability | Có qua Dagster UI |
| Restart policy | Có `restart: always` |
| Dependency health gating | Có `depends_on` với `service_healthy` |

---

## 6. SLO / SLI Design

Phần này định nghĩa SLO/SLI ở mức khái niệm. Hiện tại lab chưa có monitoring tự động, nhưng vẫn cần định nghĩa để làm chuẩn cho Game Day và cải tiến tương lai.

### 6.1. Data Freshness SLO

| SLI | Mục tiêu | Ghi chú |
|-----|---------|---------|
| Thời điểm dữ liệu `orders_production` sẵn sàng | Trước 07:30 sáng | Áp dụng cho ngày làm việc |
| Tỷ lệ ngày đạt freshness | 95% mỗi tháng | Nếu trễ sau 07:30 được tính là breach |
| Cơ chế đo lường | Thời điểm asset `orders_production` materialize thành công | Hiện tại phải xem thủ công trong Dagster UI |
| Alert đề xuất | Cảnh báo nếu pipeline chưa hoàn tất trước 07:15 | Chưa triển khai |

### 6.2. Data Correctness SLO

| SLI | Mục tiêu | Ghi chú |
|-----|---------|---------|
| Schema đúng data contract | 100% | Đã có contract validation |
| `order_id` không null | 100% | Chưa có quality check |
| `order_id` không trùng | 100% | Chưa có quality check |
| `amount` không âm | 100% nếu business không cho phép âm | Chưa có quality check |
| `created_at` hợp lệ | Không ở tương lai quá xa | Chưa có quality check |
| Row count không giảm bất thường | Không giảm quá ngưỡng cho phép | Chưa có anomaly check |

### 6.3. Data Completeness SLO

| SLI | Mục tiêu | Ghi chú |
|-----|---------|---------|
| Source không rỗng | Phải có dữ liệu hoặc có chính sách rỗng hợp lệ | Hiện tại empty source chỉ warning và có thể gây hành vi không an toàn |
| Staging không rỗng trước swap | Bắt buộc nếu production không được phép rỗng | Chưa có gate |
| Production không bị blackout | 100% | Atomic Swap hỗ trợ, nhưng quality gate chưa đầy đủ |

### 6.4. Platform Availability SLO

| SLI | Mục tiêu | Ghi chú |
|-----|---------|---------|
| Dagster UI khả dụng | 99.5% trong giờ làm việc | UI down không nhất thiết làm pipeline fail |
| Dagster Daemon hoạt động | 99.9% | Daemon down có thể làm run queued |
| Run queued quá lâu | Không quá 5 phút | Chưa có alert |
| Daemon heartbeat freshness | Không quá 2 phút mất heartbeat | Có thể quan sát một phần qua Dagster UI |

### 6.5. Severity Model

| Severity | Định nghĩa | Ví dụ |
|----------|-----------|-------|
| Sev1 | Ảnh hưởng trực tiếp đến dữ liệu production hoặc báo cáo business-critical | `orders_production` rỗng, sai, hoặc trễ SLO nghiêm trọng |
| Sev2 | Pipeline fail nhưng production cũ vẫn an toàn, hoặc freshness có nguy cơ bị ảnh hưởng | Swap fail, daemon down, DB unhealthy |
| Sev3 | Lỗi bị chặn sớm, chưa ảnh hưởng production | Schema drift bị contract chặn |
| Sev4 | Giảm khả năng quan sát hoặc DevEx | UI down nhưng pipeline vẫn chạy |

---

## 7. Component Inventory and Dependencies

### 7.1. Danh sách thành phần

| Thành phần | Loại | Owner | Nếu fail thì ảnh hưởng gì? |
|------------|------|-------|------------------------------|
| `mysql-source` | Source database | Platform Team / Source Team giả lập | Không extract được dữ liệu |
| `postgres-target` | Target database | Platform Team | Không load staging, không swap được |
| `dagster-platform` | Control plane UI | Platform Team | Người dùng không materialize hoặc xem log được |
| `dagster-daemon` | Execution engine | Platform Team | Run queued, pipeline không chạy |
| `dagster_storage` | Stateful volume | Platform Team | Mất run history, có thể ảnh hưởng coordination |
| `dataops-network` | Docker network | Platform Team | Services không giao tiếp được |
| `.env` | Secret/config | Platform Team | Pipeline không kết nối DB nếu sai credential |
| `user_code` | Data logic | Data Team | Pipeline logic sai hoặc contract sai |
| `platform/dagster` | Platform config | Platform Team | Dagster không khởi động đúng |
| `seed_data` | Init data | Data/Platform Team | Lab không có dữ liệu mẫu |

### 7.2. Dependency map

| Phụ thuộc | Bên phụ thuộc | Nếu dependency fail |
|-----------|----------------|------------------------|
| MySQL source | Contract validation, staging load | Pipeline fail ở bước đầu |
| PostgreSQL target | Staging load, atomic swap | Pipeline fail trước khi cập nhật production |
| Dagster storage | Webserver, daemon | Run history/queue có thể bị ảnh hưởng |
| Docker network | Tất cả service | Connection refused |
| `.env` | MySQL, PostgreSQL, Dagster env | Authentication failure |
| Data contract | Contract validation | Nếu contract sai, validation có thể sai theo |

### 7.3. Stateful components

Các thành phần stateful cần được quan tâm đặc biệt:

| Volume | Dữ liệu chứa | Rủi ro chính |
|--------|---------------|----------------|
| `mysql_data` | Dữ liệu MySQL source | Mất source lab nếu volume bị xóa/hỏng |
| `postgres_data` | Dữ liệu PostgreSQL target | Mất staging/production nếu không có backup |
| `dagster_storage` | Run history, event logs, schedule state | Mất lịch sử vận hành, có thể ảnh hưởng daemon coordination |

---

## 8. Failure Mode Matrix

Dưới đây là các failure mode quan trọng cần được xem xét trong quá trình vận hành pipeline orders.

### FM-01 — MySQL Source không khả dụng

| Mục | Nội dung |
|-----|----------|
| Scenario | MySQL source bị down, chưa sẵn sàng, hoặc không nhận kết nối |
| Business impact | Pipeline không thể validate hoặc extract dữ liệu |
| Detection | Healthcheck MySQL fail, asset fail, log connection error |
| Blast radius | Toàn bộ pipeline orders |
| Severity | Sev2 nếu production cũ vẫn còn; Sev1 nếu gây trễ SLO |
| Current control | Docker `restart: always`, healthcheck, `depends_on` healthy |
| Gap | Chưa có alert chủ động |
| Recovery | Kiểm tra MySQL service, chờ healthy, chạy lại pipeline từ `validate_orders_schema` |
| Prevention | Monitoring DB availability, alert khi DB unhealthy |
| Owner | Platform Team |

### FM-02 — Schema drift từ MySQL Source

| Mục | Nội dung |
|-----|----------|
| Scenario | Source đổi tên cột, xóa cột, đổi kiểu dữ liệu |
| Business impact | Nếu không chặn, downstream có thể nhận dữ liệu sai hoặc fail muộn |
| Detection | Asset `validate_orders_schema` fail |
| Blast radius | Pipeline dừng, production cũ an toàn |
| Severity | Sev2 hoặc Sev3 tùy ảnh hưởng freshness |
| Current control | Data contract fail-fast |
| Gap | Chưa có alert; chưa có contract change workflow; `status` không nằm trong `REQUIRED_COLUMNS` |
| Recovery | Nếu source đổi sai: yêu cầu source rollback. Nếu đổi hợp lệ: cập nhật data contract sau khi review |
| Prevention | Contract review process, breaking change notification, cập nhật `REQUIRED_COLUMNS` nếu cột cần thiết |
| Owner | Data Team + Source Team |

> **Ghi chú quan trọng về data contract hiện tại:** Trong data contract hiện tại, `EXPECTED_ORDERS_SCHEMA` có cột `status`. Nhưng `REQUIRED_COLUMNS` không có `status`. Điều này có nghĩa là nếu source thiếu cột `status`, validation có thể không fail theo danh sách cột bắt buộc. Nếu `status` là cột quan trọng với business hoặc downstream, đây là một gap cần xem xét.

### FM-03 — Source trả về dữ liệu rỗng

| Mục | Nội dung |
|-----|----------|
| Scenario | Bảng `orders` trong MySQL không có row nào |
| Business impact | Có thể dẫn đến production rỗng hoặc swap dữ liệu staging cũ |
| Detection | Hiện tại chỉ có log warning |
| Blast radius | CEO/Analyst có thể thấy báo cáo trống |
| Severity | Sev1 nếu production bị rỗng |
| Current control | Asset `orders_staging` ghi warning khi không có dữ liệu |
| Gap | Asset vẫn trả về success; không có quality gate chặn swap; hành vi sau đó phụ thuộc vào trạng thái staging cũ |
| Recovery | Không swap nếu staging rỗng hoặc không hợp lệ; giữ production cũ |
| Prevention | Thêm data quality gate: row count policy, empty source policy, staging existence check |
| Owner | Data Team + Platform Team |

> **Rủi ro cụ thể trong triển khai hiện tại:** Nếu source rỗng: Asset `orders_staging` có thể trả về success với `rows_loaded: 0`. Nếu bảng `orders_staging` chưa tồn tại từ trước, asset `orders_production` có thể fail vì không có staging để swap. Nếu bảng `orders_staging` cũ vẫn tồn tại, asset `orders_production` có thể swap dữ liệu cũ vào production. Đây là một rủi ro production-grade cần được xử lý rõ ràng.

### FM-04 — Staging không tồn tại hoặc staging cũ không hợp lệ

| Mục | Nội dung |
|-----|----------|
| Scenario | Asset `orders_production` chạy nhưng `orders_staging` không tồn tại, hoặc là staging cũ từ lần chạy trước |
| Business impact | Có thể fail hoặc đưa dữ liệu không đúng vào production |
| Detection | Cần kiểm tra Dagster run history và trạng thái bảng trong PostgreSQL |
| Blast radius | Production data có thể bị sai hoặc pipeline fail |
| Severity | Sev1 nếu dữ liệu sai được đưa vào production |
| Current control | README khuyến nghị dùng `Materialize all` |
| Gap | Hệ thống chưa bắt buộc về mặt kỹ thuật; vẫn phụ thuộc vào hành vi người dùng |
| Recovery | Kiểm tra nguồn gốc staging; chạy lại toàn bộ pipeline từ validation nếu nghi ngờ |
| Prevention | Thiết kế swap asset kiểm tra staging freshness, run identity, hoặc không cho swap nếu staging không hợp lệ |
| Owner | Platform Team + Data Team |

### FM-05 — Retry sai cách gây đưa dữ liệu staging cũ vào production

| Mục | Nội dung |
|-----|----------|
| Scenario | Người dùng chỉ retry asset `orders_production` trong khi `orders_staging` không được tạo lại hoặc đã cũ |
| Business impact | Production có thể nhận dữ liệu staging cũ, thiếu hoặc không nhất quán |
| Detection | Run history, lineage, metadata |
| Blast radius | Production data |
| Severity | Sev1 nếu dữ liệu sai ảnh hưởng báo cáo |
| Current control | Tài liệu khuyến nghị `Materialize all` |
| Gap | Thiếu guardrail kỹ thuật để ngăn thao tác không an toàn |
| Recovery | Xác minh staging thuộc cùng một run hợp lệ; nếu không chắc chắn, chạy lại toàn bộ pipeline |
| Prevention | Bổ sung metadata run identity, staging timestamp, hoặc quality check trước swap |
| Owner | Platform Team + Data Team |

### FM-06 — PostgreSQL fail trong lúc load staging

| Mục | Nội dung |
|-----|----------|
| Scenario | PostgreSQL bị crash hoặc mất kết nối khi đang tạo/insert staging |
| Business impact | Pipeline fail, production cũ an toàn |
| Detection | Asset `orders_staging` fail |
| Blast radius | Lần cập nhật dữ liệu mới bị hoãn |
| Severity | Sev2 |
| Current control | Transaction rollback trong asset staging |
| Gap | Chưa rõ retry path nếu orchestrator state và database state lệch nhau |
| Recovery | Chạy lại pipeline từ validation hoặc staging nếu cần |
| Prevention | Monitoring PostgreSQL health, alert khi DB unhealthy |
| Owner | Platform Team |

### FM-07 — PostgreSQL fail trong lúc Atomic Swap

| Mục | Nội dung |
|-----|----------|
| Scenario | PostgreSQL crash hoặc mất kết nối giữa lúc swap |
| Business impact | Pipeline fail, nhưng production cũ có thể được bảo vệ nếu transaction rollback |
| Detection | Asset `orders_production` fail |
| Blast radius | Lần cập nhật dữ liệu mới bị hoãn |
| Severity | Sev2 |
| Current control | PostgreSQL transactional DDL, rollback khi lỗi |
| Gap | Chưa có quy trình retry an toàn; chưa có retained backup sau swap thành công |
| Recovery | Kiểm tra trạng thái production và staging; chạy lại pipeline an toàn |
| Prevention | Thêm runbook, giữ previous production version trong thời gian ngắn, monitoring DB |
| Owner | Platform Team + Data Team |

### FM-08 — Swap thành công nhưng dữ liệu bên trong sai

| Mục | Nội dung |
|-----|----------|
| Scenario | Schema đúng, staging được tạo, swap thành công, nhưng dữ liệu business sai |
| Business impact | Production có dữ liệu sai mà không có lỗi kỹ thuật rõ ràng |
| Detection | Hiện tại khó phát hiện nếu không có data quality check |
| Blast radius | Báo cáo, dashboard, quyết định business |
| Severity | Sev1 |
| Current control | Contract validation chỉ kiểm tra schema |
| Gap | Thiếu data correctness checks: null PK, duplicate PK, amount range, row count anomaly |
| Recovery | Cần restore hoặc chạy lại pipeline từ source nếu source đúng |
| Prevention | Thêm data quality gate trước swap |
| Owner | Data Team |

### FM-09 — Dagster Daemon không chạy

| Mục | Nội dung |
|-----|----------|
| Scenario | Daemon crash hoặc không được khởi động |
| Business impact | Run bị queued, pipeline không thực thi |
| Detection | Run stuck queued, cảnh báo trong Dagster UI nếu có |
| Blast radius | Mọi run, schedule, sensor |
| Severity | Sev2 |
| Current control | Docker `restart: always` |
| Gap | Chưa có alert khi run queued quá lâu |
| Recovery | Kiểm tra daemon, restart/recreate service, retry run |
| Prevention | Monitor daemon heartbeat, alert khi run queued quá ngưỡng |
| Owner | Platform Team |

### FM-10 — Split-brain hoặc nhiều daemon process

| Mục | Nội dung |
|-----|----------|
| Scenario | Có nhiều hơn một daemon process cố gắng thực thi run |
| Business impact | Có thể gây tranh chấp lock, log lỗi, run không ổn định |
| Detection | Log daemon, Dagster UI deployment warning |
| Blast radius | Execution layer |
| Severity | Sev2 |
| Current control | Daemon heartbeat/leader election trong Dagster |
| Gap | Chưa có monitoring tự động cho số lượng daemon process |
| Recovery | Đảm bảo chỉ có một daemon chính thức; restart/recreate container liên quan |
| Prevention | Không tạo shadow process; vận hành qua container lifecycle chuẩn |
| Owner | Platform Team |

### FM-11 — Dagster storage bị corrupt hoặc mất

| Mục | Nội dung |
|-----|----------|
| Scenario | Volume `dagster_storage` bị hỏng hoặc dữ liệu Dagster bị corrupt |
| Business impact | Mất run history, có thể ảnh hưởng queue/schedule state |
| Detection | UI lỗi, daemon lỗi, run state bất thường |
| Blast radius | Observability và execution coordination |
| Severity | Sev2 hoặc Sev1 nếu ảnh hưởng execution |
| Current control | Named volume |
| Gap | Chưa có backup cho Dagster storage; SQLite chưa phải lựa chọn production mạnh |
| Recovery | Khôi phục từ backup nếu có; nếu không, tái tạo environment và chấp nhận mất lịch sử |
| Prevention | Cân nhắc PostgreSQL cho Dagster instance storage ở môi trường production thật |
| Owner | Platform Team |

### FM-12 — Secret trong `.env` bị sai hoặc bị lộ

| Mục | Nội dung |
|-----|----------|
| Scenario | Credential sai, hết hạn, hoặc bị chia sẻ ra ngoài |
| Business impact | Pipeline không chạy hoặc rủi ro bảo mật |
| Detection | Authentication error, connection error |
| Blast radius | Pipeline và database access |
| Severity | Sev2 cho availability; Sev1 nếu secret production bị lộ |
| Current control | `.env` không commit, có `.env.example` |
| Gap | Chưa có secret manager, rotation, least privilege |
| Recovery | Cập nhật credential, restart/recreate service liên quan, rotate nếu lộ |
| Prevention | Secret manager, rotation policy, không đưa secret vào log/chat |
| Owner | Platform Team + Security |

### FM-13 — Named volume bị xóa nhầm

| Mục | Nội dung |
|-----|----------|
| Scenario | Volume `mysql_data`, `postgres_data` hoặc `dagster_storage` bị xóa |
| Business impact | Mất dữ liệu source/target hoặc mất trạng thái Dagster |
| Detection | DB unhealthy, dữ liệu biến mất, init script chạy lại |
| Blast radius | Storage layer |
| Severity | Sev1 |
| Current control | Named volumes giúp dữ liệu sống qua restart |
| Gap | Chưa có backup/restore |
| Recovery | Nếu có backup thì restore; nếu không, rebuild từ source hoặc seed data |
| Prevention | Backup job, restore test, quy trình bảo vệ volume |
| Owner | Platform Team |

### FM-14 — Resource exhaustion khi dữ liệu lớn

| Mục | Nội dung |
|-----|----------|
| Scenario | Bảng `orders` tăng trưởng lớn, pipeline đọc toàn bộ dữ liệu vào bộ nhớ |
| Business impact | Pipeline chậm, OOM, ảnh hưởng source/target |
| Detection | Logs, container metrics, pipeline duration |
| Blast radius | Pipeline và có thể cả database |
| Severity | Sev2 hoặc Sev1 nếu gây SLO breach |
| Current control | Chưa có |
| Gap | Chưa có batch loading, pagination, partitioning, timeout, resource limits |
| Recovery | Giảm phạm vi dữ liệu, chạy lại theo batch nếu được thiết kế |
| Prevention | Thiết kế incremental load, cost guardrails, resource limits |
| Owner | Data Team + Platform Team |

---

## 9. Data Quality Gate Readiness

### 9.1. Quality gate hiện tại

Hiện tại pipeline có:

- Schema contract validation.
- Kiểm tra thiếu cột bắt buộc.
- Kiểm tra kiểu dữ liệu cơ bản.
- Atomic Swap để bảo vệ production khỏi lỗi kỹ thuật trong quá trình swap.

### 9.2. Quality gate còn thiếu

| Gate | Mục đích | Trạng thái |
|------|----------|-----------|
| Row count > 0 | Không cho production rỗng | Chưa có |
| Row count anomaly | Phát hiện giảm dữ liệu bất thường | Chưa có |
| Primary key not null | Chặn khóa chính null | Chưa có |
| Primary key unique | Chặn trùng khóa chính | Chưa có |
| `amount` non-negative | Chặn giá trị tài chính âm nếu business không cho phép | Chưa có |
| `created_at` valid | Chặn timestamp vô lý | Chưa có |
| Staging existence check | Đảm bảo staging tồn tại trước swap | Chưa có ở mức production asset |
| Staging freshness check | Đảm bảo staging thuộc run hiện tại | Chưa có |
| Empty source policy | Xử lý rõ ràng khi source rỗng | Chưa có |
| Target schema contract | Đảm bảo schema PostgreSQL sau load đúng kỳ vọng | Chưa có |

### 9.3. Nguyên tắc quality gate

> Chỉ swap sang production khi dữ liệu staging đáp ứng các quality gate bắt buộc. Nếu quality gate fail, pipeline phải giữ nguyên production cũ.

### 9.4. Khuyến nghị

Cần có một bước quality check rõ ràng giữa staging và production. Luồng mong muốn:

1. Validate source schema.
2. Extract vào staging.
3. Validate data quality trên staging.
4. Chỉ swap nếu tất cả gate pass.
5. Nếu gate fail, không swap và giữ production cũ.

---

## 10. Observability and Alerting Readiness

### 10.1. Observability hiện tại

| Khả năng | Hiện trạng |
|----------|-----------|
| Lineage | Có trong Dagster UI |
| Step logs | Có trong Dagster UI |
| Run history | Có trong Dagster UI |
| Asset status | Có trong Dagster UI |
| Metadata output | Có ở một số asset |
| Database healthcheck | Có cho MySQL và PostgreSQL |
| Webserver healthcheck | Có |
| Daemon healthcheck | Không có HTTP healthcheck; dựa vào restart và heartbeat concept |

### 10.2. Alerting hiện tại

| Alert | Hiện trạng |
|-------|-----------|
| Contract validation failed | Chưa có alert chủ động |
| Data quality gate failed | Chưa có vì chưa có quality gate đầy đủ |
| Pipeline failed | Chưa có alert chủ động |
| Run queued quá lâu | Chưa có alert |
| Daemon heartbeat missing | Chưa có alert tự động |
| Freshness SLO breach | Chưa có alert |
| Database unhealthy | Chưa có alert |
| Volume missing/corrupt | Chưa có alert |

### 10.3. Alert đề xuất

| Alert | Severity | Kích hoạt khi |
|-------|---------|-----------------|
| Contract validation failed | Sev2/Sev3 | `validate_orders_schema` fail |
| Data quality gate failed | Sev1 nếu ảnh hưởng production freshness | Quality check trước swap fail |
| Empty source detected | Sev1 hoặc Sev2 tùy policy | Source row count = 0 |
| Run queued > 5 phút | Sev2 | Run không chuyển sang running |
| Daemon heartbeat missing > 2 phút | Sev2 | Daemon không còn heartbeat |
| Pipeline chưa hoàn tất trước 07:15 | Sev2 | Freshness at-risk |
| Pipeline chưa hoàn tất trước 07:30 | Sev1 | Freshness SLO breach |
| Database unhealthy | Sev1 hoặc Sev2 | MySQL/PostgreSQL healthcheck fail |
| Atomic Swap failed | Sev2 | `orders_production` fail |
| Production row count giảm bất thường | Sev1 | Sau swap hoặc sau quality check |

---

## 11. Recovery, Rollback, and Retry Policy

### 11.1. Nguyên tắc retry an toàn

Không nên retry riêng asset `orders_production` nếu không chắc chắn rằng `orders_staging` là mới và hợp lệ.

**Chính sách khuyến nghị:**

| Tình huống | Hành động an toàn |
|-----------|---------------------|
| Contract validation fail | Không retry production; sửa source hoặc contract trước |
| Staging load fail | Chạy lại từ staging sau khi DB ổn định |
| Swap fail | Kiểm tra production cũ; chạy lại toàn bộ pipeline từ validation nếu cần |
| Source empty | Không swap trừ khi business chấp nhận production rỗng một cách tường minh |
| Không rõ trạng thái staging | Chạy lại toàn bộ pipeline từ validation |
| Production đã bị swap nhầm dữ liệu xấu | Cần rollback strategy rõ ràng; hiện tại backup table bị xóa sau swap nên rollback không còn đơn giản |

### 11.2. Rollback hiện tại

Atomic Swap hiện tại bảo vệ production trong trường hợp swap fail giữa chừng.

Tuy nhiên:

- Sau khi swap thành công, bảng backup bị xóa.
- Do đó, nếu dữ liệu mới sai về mặt business nhưng swap vẫn thành công, việc quay lại dữ liệu cũ không còn dễ dàng.
- Hiện tại chưa có retained previous version.

### 11.3. Khuyến nghị rollback production-grade

Để production thật, cần cân nhắc:

- Giữ lại previous production table trong một khoảng thời gian.
- Hoặc có snapshot/backup trước khi swap.
- Hoặc có cơ chế versioned table.
- Hoặc có quy trình restore từ backup.

### 11.4. Recovery theo thành phần

| Thành phần | Recovery strategy |
|------------|---------------------|
| MySQL source | Kiểm tra service, volume, healthcheck; chạy lại pipeline sau khi source healthy |
| PostgreSQL target | Kiểm tra service, volume, healthcheck; chạy lại pipeline an toàn |
| Dagster webserver | Restart/recreate service; không nhất thiết ảnh hưởng run nếu daemon còn sống |
| Dagster daemon | Restart/recreate service; kiểm tra run queued |
| Dagster storage | Restore từ backup nếu có; nếu không, tái tạo trạng thái |
| `.env` sai | Cập nhật credential, recreate service liên quan |
| Volume bị xóa | Restore từ backup nếu có; nếu không, rebuild từ đầu |

---

## 12. Security and Secret Handling

### 12.1. Hiện trạng

| Mục | Hiện trạng |
|-----|-----------|
| `.env` | Được dùng để chứa credential |
| `.gitignore` | Được kỳ vọng không commit `.env` |
| `.env.example` | Có để hướng dẫn |
| Secret trong compose | Được đọc từ biến môi trường |
| Port database | Đang expose cho lab |
| Dagster UI | Không có authentication trong lab |
| Secret rotation | Chưa có |
| Least privilege DB user | Chưa được đánh giá đầy đủ |

### 12.2. Rủi ro bảo mật chính

| Rủi ro | Tác động |
|--------|----------|
| `.env` bị commit | Lộ credential |
| `.env` bị chia sẻ trong chat/ticket | Lộ credential |
| Expose DB port | Tăng attack surface |
| Credential mặc định | Dễ bị đoán nếu dùng ra môi trường thật |
| Không có UI auth | Ai cũng có thể materialize pipeline nếu truy cập được UI |
| Không có rotation | Khó phản ứng khi credential bị lộ |

### 12.3. Khuyến nghị production

- Không đưa secret vào log, tài liệu, ticket, chat.
- Không dùng credential lab cho production.
- Hạn chế expose port database.
- Dùng secret manager hoặc cơ chế secret phù hợp với on-prem.
- Áp dụng least privilege cho database users.
- Thêm authentication/authorization cho Dagster UI nếu dùng cho nhiều người.
- Có rotation policy.

---

## 13. Change Management and Deployment

### 13.1. Hiện trạng

| Mục | Hiện trạng |
|-----|-----------|
| Code thay đổi | Cần build lại image vì code được copy vào image |
| Immutability | Có ở mức thiết kế |
| Versioning artifact | Chưa rõ ràng |
| CI/CD | Chưa có |
| Environment promotion | Chưa có |
| Rollback artifact | Chưa có quy trình rõ ràng |
| Data contract change | Có tài liệu hướng dẫn nhưng chưa có workflow đầy đủ |

### 13.2. Rủi ro

- Không có version image rõ ràng sẽ khó biết environment đang chạy artifact nào.
- Không có CI/CD khiến lỗi chỉ được phát hiện khi chạy thủ công.
- Không có environment promotion làm tăng rủi ro khác biệt giữa lab và production.
- Thay đổi data contract không có approval workflow có thể gây silent breaking change.

### 13.3. Khuyến nghị

Cần định nghĩa:

- Mỗi thay đổi code tạo ra một artifact mới.
- Artifact có version hoặc tag.
- Có test cho contract và pipeline logic.
- Có quy trình promote từ lab sang staging sang production.
- Có rollback artifact.
- Có approval cho data contract change.

---

## 14. Go/No-Go Criteria

### 14.1. Checklist hiện tại

| Tiêu chí | Bắt buộc cho production? | Hiện trạng | Kết luận |
|----------|----------------------------|-----------|----------|
| Pipeline chạy happy path | Có | Đạt | Pass |
| Data contract fail-fast | Có | Đạt một phần | Pass with note |
| Atomic Swap bảo vệ production khi swap fail | Có | Đạt | Pass |
| Staging existence check trước swap | Có | Chưa đạt | Fail |
| Empty source policy | Có | Chưa đạt | Fail |
| Data quality gate trước swap | Có | Chưa đạt | Fail |
| Retry an toàn | Có | Chưa đạt | Fail |
| Alert khi pipeline fail | Nên có | Chưa đạt | Fail |
| Alert khi freshness SLO breach | Có | Chưa đạt | Fail |
| Backup/restore PostgreSQL | Có cho production thật | Chưa đạt | Fail |
| Runbook cho failure modes chính | Có | Chưa đạt | Fail |
| Secret hardening | Có | Chưa đạt | Fail |
| Resource guardrails | Nên có | Chưa đạt | Fail |
| Game Day verification | Nên có | Chưa đạt | Fail |

### 14.2. Kết luận Go/No-Go

| Môi trường | Quyết định |
|------------|-----------|
| Lab học tập | Go |
| Internal non-critical | Conditional Go |
| Business-critical | No-Go cho đến khi đóng các P0 gaps |

---

## 15. Known Gaps and Risk Acceptance

| ID | Gap | Rủi ro | Mức ưu tiên | Trạng thái |
|----|-----|--------|-------------|-----------|
| GAP-01 | Chưa có data quality gate trước swap | Dữ liệu sai có thể vào production | P0 | Open |
| GAP-02 | Empty source chỉ warning, không có policy rõ ràng | Có thể swap staging cũ hoặc production rỗng | P0 | Open |
| GAP-03 | Retry riêng production asset có thể không an toàn | Đưa staging cũ vào production | P0 | Open |
| GAP-04 | Backup table bị xóa ngay sau swap thành công | Khó rollback khi dữ liệu mới sai | P1 | Open |
| GAP-05 | Chưa có alerting | Phát hiện sự cố muộn | P1 | Open |
| GAP-06 | Chưa có backup/restore PostgreSQL | Mất dữ liệu nếu volume hỏng | P0 cho production thật | Open |
| GAP-07 | Chưa có freshness monitoring | Không biết SLO breach | P1 | Open |
| GAP-08 | `status` không nằm trong `REQUIRED_COLUMNS` | Có thể thiếu cột quan trọng mà không fail | P1 | Open |
| GAP-09 | Dynamic DDL từ pandas cho staging | Target schema có thể drift | P2 | Open |
| GAP-10 | SQLite cho Dagster storage | Chưa tối ưu cho production đa người dùng | P2 | Open |
| GAP-11 | Chưa có resource limits | Rủi ro OOM hoặc noisy neighbor | P2 | Open |
| GAP-12 | Chưa có authentication cho Dagster UI | Rủi ro vận hành nếu expose rộng | P1 nếu dùng nhiều người | Open |
| GAP-13 | Chưa có CI/CD | Thiếu test và promotion an toàn | P2 | Open |
| GAP-14 | Chưa có schedule | Freshness phụ thuộc thao tác tay | P1 nếu cần chạy hàng ngày | Open |

---

## 16. Game Day Plan

Mini-PRR này nên được kiểm chứng bằng Game Day. Mỗi Game Day cần có mục tiêu, cách kích hoạt, kỳ vọng và kết quả thực tế.

### GD-01 — Schema drift

| Mục | Nội dung |
|-----|----------|
| Scenario | Source thay đổi schema không hợp lệ |
| Kỳ vọng | `validate_orders_schema` fail; staging và production không bị cập nhật |
| Thành công khi | Production cũ an toàn, lỗi hiển thị rõ trong Dagster UI |
| Gap cần quan sát | Thông báo lỗi có đủ rõ không? Có biết cột nào sai không? |

### GD-02 — Source rỗng

| Mục | Nội dung |
|-----|----------|
| Scenario | Bảng `orders` trong MySQL không có dữ liệu |
| Kỳ vọng hiện tại | Asset staging có thể warning và success; hành vi swap có thể không an toàn |
| Kỳ vọng production-grade | Pipeline phải fail-fast hoặc có policy rõ ràng, không swap staging cũ/rỗng vào production |
| Thành công khi | Production không bị blackout hoặc không bị thay bằng dữ liệu sai |

### GD-03 — PostgreSQL fail trong lúc swap

| Mục | Nội dung |
|-----|----------|
| Scenario | PostgreSQL mất kết nối hoặc crash giữa lúc swap |
| Kỳ vọng | Transaction rollback; production cũ an toàn |
| Thành công khi | Production cũ còn nguyên; lỗi hiển thị rõ; retry an toàn |
| Gap cần quan sát | Người vận hành có biết nên retry từ đâu không? |

### GD-04 — Daemon down

| Mục | Nội dung |
|-----|----------|
| Scenario | Dagster daemon không chạy |
| Kỳ vọng | Run queued; UI hoặc log cho thấy vấn đề |
| Thành công khi | Người vận hành phát hiện daemon là nguyên nhân, không nhầm thành lỗi pipeline |
| Gap cần quan sát | Có cảnh báo run queued quá lâu không? |

### GD-05 — Volume bị xóa

| Mục | Nội dung |
|-----|----------|
| Scenario | Volume PostgreSQL hoặc MySQL bị xóa trong lab |
| Kỳ vọng | Hệ thống cho thấy dữ liệu bị mất hoặc init lại |
| Thành công khi | Người học hiểu rằng không có backup thì không có DR |
| Gap cần quan sát | Có runbook restore không? |

### GD-06 — Sai credential

| Mục | Nội dung |
|-----|----------|
| Scenario | Credential trong `.env` sai |
| Kỳ vọng | Pipeline fail với lỗi kết nối/authentication rõ ràng |
| Thành công khi | Người vận hành biết lỗi thuộc cấu hình secret, không nhầm thành lỗi logic |
| Gap cần quan sát | Log có đủ rõ nhưng không lộ secret không? |

---

## 17. Action Plan

Dưới đây là các hành động đề xuất để nâng pipeline lên mức production-ready hơn.

| ID | Hành động | Ưu tiên | Owner | Tiêu chí chấp nhận |
|----|-----------|---------|-------|----------------------|
| PRR-01 | Định nghĩa empty source policy rõ ràng | P0 | Data Team | Source rỗng không dẫn đến swap staging cũ hoặc production rỗng ngoài ý muốn |
| PRR-02 | Thêm staging existence/freshness check trước swap | P0 | Platform + Data | `orders_production` không swap nếu staging không hợp lệ |
| PRR-03 | Thêm data quality gate trên staging | P0 | Data Team | Null PK, duplicate PK, amount invalid, row count bất thường bị chặn |
| PRR-04 | Chuẩn hóa retry policy | P0 | Platform Team | Người dùng biết khi nào cần chạy lại toàn bộ chuỗi, khi nào không |
| PRR-05 | Thêm alert cho pipeline failure | P1 | Platform Team | Pipeline fail được thông báo chủ động |
| PRR-06 | Thêm freshness SLO monitoring | P1 | Platform + SRE | Biết được pipeline có nguy cơ trễ trước 07:30 |
| PRR-07 | Thiết kế backup/restore PostgreSQL | P0 cho production thật | Platform Team | Có thể restore và đã test restore |
| PRR-08 | Giữ previous production version hoặc backup trước swap | P1 | Data Team + Platform | Có thể rollback khi dữ liệu mới sai |
| PRR-09 | Rà soát lại `REQUIRED_COLUMNS` | P1 | Data Team | Các cột business-critical đều bắt buộc |
| PRR-10 | Thêm schedule nếu pipeline cần chạy hàng ngày | P1 | Data Team | Pipeline không phụ thuộc hoàn toàn vào thao tác tay |
| PRR-11 | Thêm resource guardrails | P2 | Platform Team | Tránh OOM khi dữ liệu lớn |
| PRR-12 | Cân nhắc PostgreSQL cho Dagster instance storage | P2 | Platform Team | Phù hợp hơn cho production đa người dùng |
| PRR-13 | Thêm CI/CD ở giai đoạn sau | P2 | Platform Team | Artifact có version, có test, có promotion |
| PRR-14 | Tổ chức Game Day đầu tiên | P1 | Platform + Data | Các scenario chính được kiểm chứng |

---

## 18. Approval

| Vai trò | Người phê duyệt | Ngày | Ghi chú |
|---------|-------------------|------|---------|
| Platform Engineering Lead | Cần điền | Cần điền | Xác nhận kiến trúc hạ tầng |
| Data Engineering Lead | Cần điền | Cần điền | Xác nhận data product và quality rules |
| SRE On-call | Cần điền | Cần điền | Xác nhận SLO, alert, runbook |
| Product/Platform PM | Cần điền | Cần điền | Xác nhận mức độ phục vụ khách hàng nội bộ |

---

## 19. Appendix — Evidence from Current Repository

Các bằng chứng hiện tại trong repository:

- `README.md` mô tả luồng chạy, troubleshooting và nguyên lý DataOps.
- `ARCHITECTURE.md` mô tả Atomic Swap, Data Contract, SLO, daemon heartbeat và split-brain.
- `docker-compose.yml` thể hiện service decomposition, healthchecks, volumes, dependencies.
- `platform/dagster/dagster.yaml` cho thấy Dagster đang dùng SQLite storage và `DefaultRunLauncher` trong lab.
- `platform/dagster/Dockerfile` thể hiện immutability, layer caching, healthcheck cho webserver.
- `user_code/contracts/order_contract.py` thể hiện data contract hiện tại.
- `user_code/assets/orders.py` thể hiện pipeline validation, staging load và atomic swap.
- `seed_data` cung cấp dữ liệu mẫu cho lab.

---

## 20. Open Questions

Các câu hỏi cần được trả lời trước khi nâng pipeline lên production thật:

1. Business có chấp nhận production rỗng trong bất kỳ trường hợp nào không?
2. Nếu source rỗng, nên fail-fast hay giữ production cũ?
3. `status` có phải là cột bắt buộc không?
4. `amount` có được phép âm không? Nếu có, trong trường hợp nào?
5. Dữ liệu production cần giữ lại bao lâu?
6. Có cần giữ previous production table sau swap không?
7. Ai nhận alert khi pipeline fail hoặc trễ SLO?
8. SLO freshness chính thức là 07:30 hay một mốc khác?
9. Pipeline sẽ chạy theo lịch nào?
10. Khi dữ liệu lớn, có cần incremental load hoặc partitioning không?
11. Có cần tách Dagster instance storage sang PostgreSQL không?
12. Có cần backup cả `dagster_storage` không?
13. Ai chịu trách nhiệm chính nếu dữ liệu production sai?
14. Có cần audit trail cho thay đổi data contract không?
15. Có cần authentication cho Dagster UI không?