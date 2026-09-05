"""
============================================================================
PIPELINE METRICS EMITTER - Push-based Observability Pattern
============================================================================

ARCHITECTURE PATTERN: Push-based Observability
----------------------------------------------------------------------------
Tại sao không scrape trực tiếp DB production?

ANTI-PATTERN (đã từng có trong codebase):
  Prometheus → postgres-exporter → SELECT COUNT(*) FROM orders_production
  Mỗi 5 phút = 1 lần full table scan trên bảng 50M rows

VẤN ĐỀ KHI LÊN PRODUCTION:
  - IOPS của DB target bị "ăn" bởi monitoring queries
  - Noisy Neighbor: monitoring giết chính Data Plane nó đang giám sát
  - Schema Drift có thể làm query crash (dù đã có EXCEPTION handling)
  - Resource contention: monitoring queries cạnh tranh với user queries

PATTERN (mới):
  Pipeline chạy xong → tự tính metric → PUSH lên Pushgateway
  Prometheus → Pull từ Pushgateway (nhẹ, không chạm DB)

BLAST RADIUS:
  - Nếu Pushgateway down: Pipeline vẫn chạy xong. Monitoring chỉ mất metric
    vài phút. Data Plane HOÀN TOÀN KHÔNG BỊ ẢNH HƯỞNG.
  - Đây là nguyên lý "decoupled observability" cực kỳ quan trọng.

IDEAL METRICS NAMING CONVENTION (cho multi-pipeline):
  - dagster_pipeline_data_age_hours{pipeline_name="orders"}
  - dagster_pipeline_row_count{pipeline_name="orders"}
  - dagster_pipeline_quality_null_count{pipeline_name="orders"}
  - dagster_pipeline_last_run_success{pipeline_name="orders"}
  - dagster_pipeline_last_run_duration_seconds{pipeline_name="orders"}
  → Dễ scale cho customers, inventory, marketing funnels sau này.
============================================================================
"""

import os
import time
from typing import Optional, Dict, Any
from dagster import AssetExecutionContext
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


class PipelineMetricsEmitter:
    """
    Responsible for pushing pipeline-level metrics to Prometheus Pushgateway.
    
    SINGLE RESPONSIBILITY: Emit metrics. KHÔNG BAO GIỜ fail the pipeline.
    Nếu push fail → log warning và tiếp tục. Monitoring là side-effect,
    không phải dependency của business logic.
    
    USAGE:
        emitter = PipelineMetricsEmitter()
        emitter.emit_pipeline_success(
            context=context,
            pipeline_name="orders",
            rows_processed=1000,
            data_age_hours=12.5,
            quality_metrics={
                "null_count": 0,
                "negative_amount_count": 0,
                "duplicate_count": 0
            }
        )
    """
    
    def __init__(self):
        """
        Initialize emitter with Pushgateway URL from environment.
        
        FALLBACK PATTERN:
        - Default URL: 'pushgateway:9091' (Docker service DNS)
        - Override via PROMETHEUS_PUSHGATEWAY_URL env var
        - Production thật có thể dùng internal load balancer URL
        """
        self.pushgateway_url = os.environ.get(
            'PROMETHEUS_PUSHGATEWAY_URL', 
            'pushgateway:9091'
        )
        
        # IDIOM: Mỗi push dùng một CollectorRegistry mới.
        # Lý do: Nếu dùng global registry, metric cũ sẽ bị "ghost" và 
        # Prometheus thấy giá trị stale giữa các runs.
        # Registry riêng = mỗi push là atomic, không có side effect.
        self._registry = CollectorRegistry()
    
    def _create_gauge(
        self, 
        name: str, 
        documentation: str, 
        labelnames: Optional[list] = None
    ) -> Gauge:
        """
        Helper để tạo Gauge với registry riêng của emitter.
        
        Tại sao không dùng global Prometheus registry?
        - Pipeline chạy xong thì tắt process
        - Global registry sẽ mất state khi process exit
        - Dùng CollectorRegistry riêng cho mỗi push = clean state
        - Prometheus Pull từ Pushgateway, không Pull từ pipeline process
        """
        if labelnames is None:
            labelnames = []
        return Gauge(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            registry=self._registry
        )
    
    def emit_pipeline_success(
        self,
        context: AssetExecutionContext,
        pipeline_name: str,
        rows_processed: int,
        data_age_hours: float,
        swap_duration_seconds: Optional[float] = None,
        quality_metrics: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Push all pipeline-run-level metrics in one atomic batch sau khi 
        pipeline chạy THÀNH CÔNG.
        
        PARAMETERS:
        - context: Dagster context để lấy run_id và logger
        - pipeline_name: Tên pipeline (vd: "orders", "customers")
        - rows_processed: Số rows đã process trong run này
        - data_age_hours: Tuổi của dữ liệu mới nhất (hours) sau khi swap
        - swap_duration_seconds: Thời gian atomic swap (optional)
        - quality_metrics: Dict chứa data quality counts (optional)
          {
              "null_count": 0,
              "negative_amount_count": 0, 
              "duplicate_count": 0
          }
        
        IDEMPOTENCY NOTE:
        - Mỗi run push với grouping_key = {pipeline_name: ..., run_id: ...}
        - Nếu cùng run_id push lại (retry), metric sẽ bị OVERWRITE (đúng ý đồ)
        - Nếu push mới cho run khác, tạo metric series mới
        - Đây là idempotent design: retry an toàn, không duplicate metric
        
        BLAST RADIUS:
        - Nếu push fail → chỉ log warning, không raise exception
        - Pipeline vẫn hoàn thành, production data vẫn an toàn
        - Trade-off: Mất metric tạm thời, đổi lại pipeline resilient
        """
        if quality_metrics is None:
            quality_metrics = {}
        
        try:
            # ---------------------------------------------------------
            # METRIC 1: Run Success Status (binary 1 = success)
            # ---------------------------------------------------------
            # Dùng cho alert: "Pipeline fail liên tiếp 3 lần → Sev1"
            # Label pipeline_name cho phép query: 
            #   dagster_pipeline_last_run_success{pipeline_name="orders"}
            # → Dễ scale cho nhiều pipeline sau này
            success_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_success',
                documentation='1 if last run succeeded, 0 if failed',
                labelnames=['pipeline_name']
            )
            success_gauge.labels(pipeline_name=pipeline_name).set(1)
            
            # ---------------------------------------------------------
            # METRIC 2: Run ID (string as label, not a metric)
            # ---------------------------------------------------------
            # Dùng cho debugging: biết run nào đã push metric này
            # Dùng Info metric pattern (value = 1, labels chứa metadata)
            run_info_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_info',
                documentation='Metadata about the last pipeline run',
                labelnames=['pipeline_name', 'run_id', 'status']
            )
            run_info_gauge.labels(
                pipeline_name=pipeline_name,
                run_id=context.run_id[:8],  # truncate để series không explode
                status='success'
            ).set(1)
            
            # ---------------------------------------------------------
            # METRIC 3: Rows Processed (lightweight, tính từ pipeline state)
            # ---------------------------------------------------------
            # TẠI SAO không dùng COUNT(*) trên DB?
            # → Pipeline đã biết số row khi load staging. Không cần scan lại.
            # → Đây là "compute once, reuse everywhere" pattern.
            rows_gauge = self._create_gauge(
                name='dagster_pipeline_rows_processed',
                documentation='Rows processed in this pipeline run',
                labelnames=['pipeline_name']
            )
            rows_gauge.labels(pipeline_name=pipeline_name).set(rows_processed)
            
            # ---------------------------------------------------------
            # METRIC 4: Data Age at time of run (business freshness)
            # ---------------------------------------------------------
            # Thay vì scrape DB mỗi 5 phút để tính age, pipeline tự tính
            # khi nó materialize data mới. Age chỉ có nghĩa khi có data mới.
            # Label pipeline_name cho phép multi-pipeline queries.
            age_gauge = self._create_gauge(
                name='dagster_pipeline_data_age_hours',
                documentation='Age of the freshest data after this run (hours)',
                labelnames=['pipeline_name']
            )
            age_gauge.labels(pipeline_name=pipeline_name).set(data_age_hours)
            
            # ---------------------------------------------------------
            # METRIC 5: Swap Duration (DB performance indicator)
            # ---------------------------------------------------------
            # Detect performance regression: nếu swap đột nhiên chậm 10x,
            # có thể DB có vấn đề (lock contention, disk pressure, etc.)
            if swap_duration_seconds is not None:
                duration_gauge = self._create_gauge(
                    name='dagster_pipeline_swap_duration_seconds',
                    documentation='Atomic swap duration in seconds',
                    labelnames=['pipeline_name']
                )
                duration_gauge.labels(
                    pipeline_name=pipeline_name
                ).set(swap_duration_seconds)
            
            # ---------------------------------------------------------
            # METRIC 6: Data Quality Counts (from staging checks)
            # ---------------------------------------------------------
            # Pipeline đã chạy quality checks trên staging trước khi swap.
            # Kết quả đã có sẵn trong context, không cần query lại DB.
            # Đây là "emit what you already computed" pattern.
            if quality_metrics:
                # Null count in required columns
                null_count = quality_metrics.get('null_count', 0)
                null_gauge = self._create_gauge(
                    name='dagster_pipeline_quality_null_count',
                    documentation='Rows with NULL in required columns',
                    labelnames=['pipeline_name']
                )
                null_gauge.labels(pipeline_name=pipeline_name).set(null_count)
                
                # Negative amount count
                negative_count = quality_metrics.get('negative_amount_count', 0)
                negative_gauge = self._create_gauge(
                    name='dagster_pipeline_quality_negative_count',
                    documentation='Rows with negative amount',
                    labelnames=['pipeline_name']
                )
                negative_gauge.labels(pipeline_name=pipeline_name).set(negative_count)
                
                # Duplicate count
                duplicate_count = quality_metrics.get('duplicate_count', 0)
                duplicate_gauge = self._create_gauge(
                    name='dagster_pipeline_quality_duplicate_count',
                    documentation='Duplicate primary key groups',
                    labelnames=['pipeline_name']
                )
                duplicate_gauge.labels(pipeline_name=pipeline_name).set(duplicate_count)
            
            # ---------------------------------------------------------
            # METRIC 7: Run Timestamp (when this push happened)
            # ---------------------------------------------------------
            # Dùng cho alert: "No recent run in last X hours"
            # Nếu metric này stale, biết pipeline đã ngừng chạy
            timestamp_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_timestamp',
                documentation='Unix timestamp of last successful run',
                labelnames=['pipeline_name']
            )
            timestamp_gauge.labels(pipeline_name=pipeline_name).set(time.time())
            
            # ---------------------------------------------------------
            # ATOMIC PUSH to Pushgateway
            # ---------------------------------------------------------
            # push_to_gateway vs pushadd_to_gateway:
            # - push_to_gateway: REPLACE toàn bộ metric của job này 
            #   (dùng cho single-run metrics, tránh stale)
            # - pushadd_to_gateway: ADDITIVE, giữ metric cũ 
            #   (dùng cho long-running service metrics)
            # Với pipeline run, dùng push_to_gateway để tránh stale.
            push_to_gateway(
                gateway=self.pushgateway_url,
                job=f'dagster_pipeline_{pipeline_name}',
                registry=self._registry,
                # grouping_key phân biệt metric giữa các pipeline khác nhau
                # (khi có nhiều pipeline cùng push vào 1 Pushgateway)
                grouping_key={'pipeline_name': pipeline_name},
                timeout=5  # Timeout ngắn: nếu Pushgateway chậm, không block pipeline
            )
            
            context.log.info(
                f"📊 Successfully pushed metrics for run {context.run_id[:8]} "
                f"to Pushgateway"
            )
            
        except Exception as e:
            # ==============================================================
            # CRITICAL SRE RULE: Observability failures MUST NOT cascade 
            # into business logic failures.
            # ==============================================================
            # Nếu push fail (Pushgateway down, network issue):
            #   ❌ KHÔNG raise exception
            #   ❌ KHÔNG làm pipeline fail
            #   ✅ Chỉ log warning
            #   ✅ Pipeline vẫn hoàn thành, production data vẫn an toàn
            #
            # Trade-off: Chúng ta mất metric tạm thời, đổi lại pipeline 
            # resilient. Đây là trade-off ĐÚNG trong observability design.
            # ==============================================================
            context.log.warning(
                f"⚠️ Failed to push metrics (non-fatal, pipeline continues): {e}"
            )
    
    def emit_pipeline_failure(
        self,
        context: AssetExecutionContext,
        pipeline_name: str,
        error_message: str,
    ) -> None:
        """
        Push metrics khi pipeline FAIL.
        
        Alert có thể dựa vào:
        - dagster_pipeline_last_run_success{pipeline_name="orders"} == 0
        - dagster_pipeline_last_run_timestamp không cập nhật
        
        PARAMETERS:
        - context: Dagster context
        - pipeline_name: Tên pipeline
        - error_message: Error message để debug
        
        IDEMPOTENCY:
        - Overwrite metric cũ của cùng pipeline
        - Alert sẽ thấy success = 0 ngay lập tức
        """
        try:
            # Run status = 0 (failure)
            failure_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_success',
                documentation='1 if last run succeeded, 0 if failed',
                labelnames=['pipeline_name']
            )
            failure_gauge.labels(pipeline_name=pipeline_name).set(0)
            
            # Run info with error
            run_info_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_info',
                documentation='Metadata about the last pipeline run',
                labelnames=['pipeline_name', 'run_id', 'status']
            )
            run_info_gauge.labels(
                pipeline_name=pipeline_name,
                run_id=context.run_id[:8],
                status='failed'
            ).set(1)
            
            # Timestamp vẫn cập nhật để biết pipeline vẫn đang chạy (dù fail)
            timestamp_gauge = self._create_gauge(
                name='dagster_pipeline_last_run_timestamp',
                documentation='Unix timestamp of last run (success or fail)',
                labelnames=['pipeline_name']
            )
            timestamp_gauge.labels(pipeline_name=pipeline_name).set(time.time())
            
            # Push lên Pushgateway
            push_to_gateway(
                gateway=self.pushgateway_url,
                job=f'dagster_pipeline_{pipeline_name}',
                registry=self._registry,
                grouping_key={'pipeline_name': pipeline_name},
                timeout=5
            )
            
            context.log.info(
                f"📊 Pushed failure metrics for run {context.run_id[:8]} "
                f"(error: {error_message[:100]}...)"
            )
            
        except Exception as e:
            # Vẫn theo nguyên tắc: observability failure không được cascade
            context.log.warning(
                f"⚠️ Failed to push failure metrics (non-fatal): {e}"
            )
