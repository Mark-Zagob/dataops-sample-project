# Phase 1 Migration Guide: Push-based Observability

## 📋 Tóm tắt thay đổi

**Mục tiêu:** Chuyển từ "Pull heavy queries từ DB production" sang "Pipeline-emitted metrics via Pushgateway" để bảo vệ Data Plane khi scale.

**Thời gian thực hiện:** Phase 1 - SRE Core Optimization

---

## 🏗️ Kiến trúc mới

### TRƯỚC (Anti-pattern):
```
Prometheus (mỗi 5 phút)
    ↓ scrape
postgres-exporter
    ↓ query nặng
SELECT COUNT(*) FROM orders_production  ← GIẾT DB PRODUCTION!
SELECT MAX(created_at) FROM orders_production
```

**Vấn đề:**
- Mỗi 5 phút = 1 lần full table scan trên bảng 50M+ rows
- IOPS của DB target bị "ăn" bởi monitoring
- Noisy Neighbor: monitoring giết chính Data Plane nó đang giám sát
- Resource contention với user queries

### SAU (Production-grade):
```
Pipeline (Dagster) chạy xong
    ↓ tự tính metric
    ↓ push
Prometheus Pushgateway (ephemeral metrics store)
    ↑ scrape
Prometheus (mỗi 1 phút)
```

**Lợi ích:**
- ✅ Data Plane KHÔNG BỊ CHẠM bởi monitoring
- ✅ Blast radius: Pushgateway down → pipeline vẫn OK, chỉ mất metric tạm thời
- ✅ Decoupled observability: monitoring là side-effect, không phải dependency
- ✅ Compute once, emit once: pipeline đã tính metric khi chạy, không cần tính lại

---

## 📂 Files đã thay đổi

### 1. Infrastructure

#### `docker-compose.monitoring.yml`
- **Thêm:** Pushgateway service (image: prom/pushgateway:v1.11.1)
- **Port:** 9091 (lab only)
- **Volume:** pushgateway_data (persistence qua restart)

#### `docker-compose.yml`
- **Thêm env var:** `PROMETHEUS_PUSHGATEWAY_URL=pushgateway:9091` cho:
  - dagster-platform container
  - dagster-daemon container

### 2. Monitoring Config

#### `monitoring/prometheus/prometheus.yml`
- **Thêm scrape job:** `pushgateway` (scrape_interval: 1m)
- **Config đặc biệt:** `honor_labels: true` để giữ labels gốc từ pipeline

#### `monitoring/prometheus/rules/dataops-alerts.yml`
- **Update alert rules** để query metric mới:
  - `OrdersDataTooOldWarning`: `dagster_pipeline_data_age_hours{pipeline_name="orders"}`
  - `OrdersQuality*`: `dagster_pipeline_quality_*{pipeline_name="orders"}`
- **Thêm alert mới:**
  - `OrdersPipelineLastRunFailed`: khi `dagster_pipeline_last_run_success == 0`
  - `OrdersPipelineNoRecentRun`: khi timestamp không cập nhật trong 25h

#### `monitoring/postgres-exporter/target-queries.yaml`
- **XÓA:** `orders_health_data_age_hours` (chuyển sang Pushgateway)
- **XÓA:** `orders_row_count` (chuyển sang Pushgateway)
- **XÓA:** `orders_quality` query (chuyển sang Pushgateway)
- **GIỮ LẠI:** `orders_health_metadata` (lightweight queries):
  - `production_exists` (query pg_catalog)
  - `backup_count` (query pg_catalog)
  - `staging_orphan_count` (query pg_catalog LIKE pattern)

#### `monitoring/sql/postgres_target_monitoring.sql`
- **XÓA functions nặng:**
  - `orders_data_age_hours()` 
  - `orders_row_count()`
  - `orders_quality_counts()`
- **GIỮ LẠI:** `orders_production_exists()` (lightweight)

### 3. Application Code

#### `platform/dagster/requirements.txt`
- **Thêm:** `prometheus-client==0.21.0` (để push metrics)

#### `user_code/observability/__init__.py` (NEW)
- Module mới cho observability
- Export `PipelineMetricsEmitter` class

#### `user_code/observability/metrics_emitter.py` (NEW)
- **Class:** `PipelineMetricsEmitter`
- **Methods:**
  - `emit_pipeline_success()`: push metrics khi pipeline thành công
  - `emit_pipeline_failure()`: push metrics khi pipeline fail
- **Metrics emitted:**
  - `dagster_pipeline_last_run_success{pipeline_name="orders"}`
  - `dagster_pipeline_last_run_timestamp{pipeline_name="orders"}`
  - `dagster_pipeline_rows_processed{pipeline_name="orders"}`
  - `dagster_pipeline_data_age_hours{pipeline_name="orders"}`
  - `dagster_pipeline_swap_duration_seconds{pipeline_name="orders"}`
  - `dagster_pipeline_quality_null_count{pipeline_name="orders"}`
  - `dagster_pipeline_quality_negative_count{pipeline_name="orders"}`
  - `dagster_pipeline_quality_duplicate_count{pipeline_name="orders"}`
  - `dagster_pipeline_last_run_info{pipeline_name="orders", run_id="...", status="success/failed"}`

#### `user_code/assets/orders.py`
- **Import:** `PipelineMetricsEmitter`
- **Asset `orders_staging`:** 
  - Return dict thay vì string
  - Dict chứa: `{staging_table, row_count, null_count, negative_amount_count, duplicate_count}`
- **Asset `orders_production`:**
  - Nhận dict từ `orders_staging`
  - Tính `swap_duration_seconds` (track thời gian atomic swap)
  - Tính `data_age_hours` từ `MAX(created_at)` của bảng production mới
  - **Call:** `emitter.emit_pipeline_success()` sau khi swap thành công
  - **Call:** `emitter.emit_pipeline_failure()` trong except block nếu có lỗi

### 4. Visualization

#### `monitoring/grafana/dashboards/dataops-overview.json`
- **Update panels:**
  - "Orders Data Age": query `dagster_pipeline_data_age_hours{pipeline_name="orders"}`
  - "Orders Data Quality Issues": query `dagster_pipeline_quality_*`
- **Thêm panels mới:**
  - "Orders Last Run Status" (1 = Success, 0 = Failed)
  - "Orders Rows Processed (Last Run)"
  - "Orders Swap Duration (Last Run)"
  - "Orders Last Run Time" (timestamp)
  - "Orders Production Exists" (metadata check)
- **Update "Total Scrape Targets Up":** threshold từ 6 → 7 (do thêm Pushgateway)

---

## 🧪 Cách test

### 1. Rebuild và restart

```bash
cd on-premises

# Rebuild Dagster image (vì đã thêm prometheus-client vào requirements.txt)
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml build

# Restart toàn bộ stack
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

### 2. Kiểm tra Pushgateway

```bash
# Check Pushgateway container đang chạy
docker ps | grep pushgateway

# Truy cập Pushgateway UI
# http://localhost:9091

# Ban đầu sẽ không có metrics (chưa có pipeline nào chạy)
```

### 3. Chạy pipeline orders

```bash
# Vào Dagster UI: http://localhost:3000
# Materialize asset: orders_pipeline -> validate_orders_schema -> orders_staging -> orders_production

# HOẶC chạy qua CLI (nếu có):
# dagster job execute -j orders_pipeline
```

### 4. Verify metrics đã được push

```bash
# Check Pushgateway UI: http://localhost:9091
# Sẽ thấy các metrics:
# - dagster_pipeline_last_run_success{pipeline_name="orders"} = 1
# - dagster_pipeline_data_age_hours{pipeline_name="orders"} = <số giờ>
# - dagster_pipeline_rows_processed{pipeline_name="orders"} = <số rows>
# - dagster_pipeline_quality_null_count{pipeline_name="orders"} = 0
# - etc.
```

### 5. Verify Prometheus scrape

```bash
# Truy cập Prometheus UI: http://localhost:9090

# Query metrics:
# dagster_pipeline_data_age_hours{pipeline_name="orders"}
# dagster_pipeline_last_run_success{pipeline_name="orders"}

# Check targets: Status -> Targets
# Sẽ thấy job "pushgateway" với state UP
```

### 6. Check Grafana dashboard

```bash
# Truy cập Grafana: http://localhost:3001
# Login: admin / change_me_grafana_admin (hoặc password trong .env)

# Mở dashboard "DataOps Overview"
# Sẽ thấy:
# - Orders Data Age: có giá trị (không còn "No data")
# - Orders Last Run Status: SUCCESS (màu xanh)
# - Orders Rows Processed: có số
# - Orders Data Quality Issues: tất cả = 0 (nếu data OK)
```

### 7. Test failure scenario (optional)

```bash
# Giả lập pipeline fail:
# - Stop MySQL source: docker stop dataops-mysql-source
# - Chạy pipeline orders → sẽ fail ở bước validate_orders_schema
# - Check Grafana: Orders Last Run Status sẽ = FAILED (màu đỏ)
# - Restart MySQL: docker start dataops-mysql-source
```

---

## 🎓 Mental Models học được

### 1. Control Plane vs Data Plane Separation
- **Control Plane:** Dagster metadata, orchestration logic
- **Data Plane:** Production tables (orders_production, etc.)
- **Monitoring:** KHÔNG được chạm vào Data Plane nặng
- **Principle:** Observability là side-effect, không phải dependency

### 2. Blast Radius Thinking
- **Câu hỏi:** "Nếu component X down, hệ thống bị ảnh hưởng thế nào?"
- **Pushgateway down:** Pipeline vẫn OK, chỉ mất metric tạm thời
- **DB production down:** Pipeline fail, data không có
- **Trade-off:** Chấp nhận mất metric tạm thời để bảo vệ data

### 3. Noisy Neighbor Anti-pattern
- **Vấn đề:** Monitoring queries cạnh tranh resource với user queries
- **Symptom:** DB chậm khi có nhiều monitoring queries
- **Solution:** Pipeline-emitted metrics (compute once, emit once)

### 4. Decoupled Observability
- **Pattern:** Monitoring không được làm system chậm hoặc fail
- **Implementation:** Push metrics, không pull heavy queries
- **Benefit:** Monitoring failures không cascade vào business logic

### 5. Idempotency in Monitoring
- **Question:** "Nếu push metrics 2 lần, có bị duplicate không?"
- **Answer:** Không, vì Pushgateway overwrite metric cũ
- **Design:** Retry an toàn, không side effect

---

## 🚀 Next Steps (Phase 2 & 3)

### Phase 2: Self-serve Observability (Platform Engineer)
- **Mental Model:** Platform as a Product
- **Task:** 
  - Thêm templating `$data_product` vào Grafana dashboard
  - Persona-driven layout (Traffic Light Hero Row)
  - Dashboard as Code (Git-managed)
- **Benefit:** Data Engineer có thể add pipeline mới mà không cần Platform Engineer

### Phase 3: Data Contract Observability (Data PM)
- **Mental Model:** Error Budget cho Data Products
- **Task:**
  - Build panel Error Budget
  - SLO compliance timeline
  - Data Contract health visualization
- **Benefit:** Data Consumers biết data có đáng tin không

---

## 📚 Tài liệu tham khảo

- [Prometheus Pushgateway Documentation](https://github.com/prometheus/pushgateway)
- [Prometheus Push vs Pull](https://prometheus.io/docs/introduction/faq/#why-do-you-pull-rather-than-push?)
- [Google SRE Book - Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Dagster Asset Materialization](https://docs.dagster.io/concepts/assets)

---

## ❓ FAQ

### Q: Tại sao không dùng Prometheus Pushgateway cho tất cả metrics?
**A:** Chỉ dùng cho batch job metrics (pipeline runs). Infra metrics (CPU, memory, disk) vẫn dùng Pull vì:
- Infra metrics là continuous stream
- Pushgateway không phù hợp cho high-frequency metrics
- Exporter pattern vẫn tốt cho infra

### Q: Nếu Pushgateway down, pipeline có fail không?
**A:** KHÔNG. Pipeline vẫn chạy xong, chỉ log warning. Đây là design intentional: observability failures không được cascade vào business logic.

### Q: Metrics trong Pushgateway có bị stale không?
**A:** CÓ, nếu pipeline ngừng chạy. Giải pháp:
- Alert `OrdersPipelineNoRecentRun` sẽ catch
- Hoặc dùng Pushgateway API DELETE để reset metrics cũ

### Q: Tại sao không dùng Dagster metadata DB để lấy metrics?
**A:** Dagster metadata DB là Control Plane, không phải Data Plane. Query từ đó chỉ cho biết "run đã chạy xong", không cho biết "data có tươi không". Data freshness phải tính từ Data Plane.

### Q: Làm sao biết metric đến từ Pushgateway hay postgres-exporter?
**A:** Dùng label `job`:
- `job="pushgateway"` → từ Pushgateway
- `job="postgres-target"` → từ postgres-exporter
- Hoặc query `dagster_pipeline_*` (chỉ có từ Pushgateway)

---

## 🔧 Troubleshooting

### Vấn đề: Pushgateway container không start
**Giải pháp:**
```bash
# Check logs
docker logs dataops-pushgateway

# Check port conflict
docker ps | grep 9091
# Nếu có container khác dùng port 9091, đổi port trong docker-compose.monitoring.yml
```

### Vấn đề: Pipeline chạy xong nhưng không thấy metrics trong Prometheus
**Giải pháp:**
```bash
# 1. Check Pushgateway UI: http://localhost:9091
# Có thấy metrics không? Nếu KHÔNG:
#   - Check pipeline logs: docker logs dataops-dagster-daemon
#   - Tìm "Successfully pushed metrics" hoặc "Failed to push metrics"

# 2. Nếu Pushgateway có metrics nhưng Prometheus không thấy:
#   - Check Prometheus targets: http://localhost:9090/targets
#   - Job "pushgateway" có state UP không?
#   - Check Prometheus config: http://localhost:9090/config
#   - Đảm bảo có job_name: pushgateway với honor_labels: true
```

### Vấn đề: Grafana dashboard show "No data"
**Giải pháp:**
```bash
# 1. Check Prometheus có metric không:
# http://localhost:9090
# Query: dagster_pipeline_data_age_hours{pipeline_name="orders"}

# 2. Nếu Prometheus có data nhưng Grafana không thấy:
#   - Check Grafana datasource: http://localhost:3001/datasources
#   - Đảm bảo Prometheus datasource đã được add

# 3. Nếu Prometheus không có data:
#   - Pipeline đã chạy chưa?
#   - Pushgateway có metrics không?
```

### Vấn đề: Alert không fire khi data quá cũ
**Giải pháp:**
```bash
# 1. Check alert rules: http://localhost:9090/rules
# Tìm rule "OrdersDataTooOldWarning"

# 2. Check alert state: http://localhost:9090/alerts
# Rule có state "firing" không?

# 3. Check metric value:
# dagster_pipeline_data_age_hours{pipeline_name="orders"}
# Nếu value > 26, alert nên fire sau 10 phút (for: 10m)
```

---

## 📝 Notes

- **Migration này chỉ áp dụng cho orders pipeline.** Các pipeline khác (customers, inventory) cần được update tương tự.
- **Pushgateway là ephemeral.** Nếu cần long-term storage, dùng Prometheus TSDB (đã có retention 15 ngày).
- **Metrics từ Pushgateway không có timestamp chính xác như Pull.** Prometheus sẽ gán timestamp khi scrape, không phải khi pipeline push.

---

**Last updated:** 2026-09-05  
**Author:** Lead Data Platform Engineer (AI Mentor)  
**Version:** 1.0
