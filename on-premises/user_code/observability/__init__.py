"""
============================================================================
OBSERVABILITY MODULE - Pipeline-Emitted Metrics
============================================================================

This module implements the "Push-based Observability" pattern for DataOps.

ARCHITECTURE DECISION:
- Pipeline KHÔNG expose /metrics endpoint (batch job, không phải service)
- Pipeline PUSH metrics lên Prometheus Pushgateway sau khi chạy xong
- Prometheus scrape từ Pushgateway (vẫn giữ Pull model)

WHY NOT SCRAPE DB PRODUCTION?
- Anti-pattern: Prometheus → postgres-exporter → SELECT COUNT(*) FROM orders_production
- Vấn đề khi scale (50M+ rows):
  + Mỗi 5 phút scrape = 1 lần full table scan
  + IOPS của DB target bị "ăn" bởi monitoring
  + Noisy Neighbor: monitoring giết chính Data Plane nó đang giám sát
- Solution: Pipeline tự tính metric khi chạy xong, push lên Pushgateway
  + Data Plane KHÔNG BỊ CHẠM bởi monitoring
  + Blast radius: Nếu Pushgateway down → pipeline vẫn OK, chỉ mất metric tạm thời
  + Decoupled observability: monitoring là side-effect, không phải dependency

USAGE:
    from user_code.observability.metrics_emitter import PipelineMetricsEmitter
    
    emitter = PipelineMetricsEmitter()
    emitter.emit_pipeline_success(
        context=context,
        pipeline_name="orders",
        rows_processed=1000,
        data_age_hours=12.5,
        swap_duration_seconds=2.3,
        quality_metrics={...}
    )
"""

from user_code.observability.metrics_emitter import PipelineMetricsEmitter

__all__ = ["PipelineMetricsEmitter"]
