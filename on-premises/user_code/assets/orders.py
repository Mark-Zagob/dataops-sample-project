"""
=================================================================
DATA ASSETS: Orders Pipeline
=================================================================
Pipeline này thể hiện các bước cốt lõi của DataOps Production-grade:

1. Contract Validation (Fail-Fast)
2. ETL: Extract from MySQL -> Load to run-scoped PostgreSQL Staging
3. Data Quality Checks on Staging
4. Atomic Swap: Staging -> Production

Các cải tiến chính trong phiên bản này:

- QueuedRunCoordinator giới hạn 1 run chạy đồng thời ở Control Plane.
- Staging table được đặt tên theo run để tránh conflict.
- Cleanup staging orphan ở đầu bước staging.
- Data quality check fail-fast trước khi swap.
- Atomic Swap dùng PostgreSQL advisory lock để giảm rủi ro concurrent swap.
- Giữ lại một bảng backup để tăng khả năng phục hồi ngắn hạn.
"""

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
import pymysql
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from dagster import asset, AssetExecutionContext, MetadataValue

from user_code.contracts.order_contract import (
    ORDER_CONTRACT,
    classify_schema_change,
    SOURCE_TABLE_NAME,
    PRODUCTION_TABLE_NAME,
    STAGING_TABLE_PREFIX,
    LEGACY_STAGING_TABLE_NAME,
    TARGET_COLUMNS,
    ORDERS_PIPELINE_LOCK_KEY,
    MIN_EXPECTED_ROWS,
)


# =================================================================
# Helper Functions
# =================================================================

def get_mysql_connection():
    """
    Tạo kết nối đến MySQL Source.
    """
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "datauser"),
        password=os.environ.get("MYSQL_PASSWORD", "datapassword"),
        database=os.environ.get("MYSQL_DATABASE", "sales_db"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_postgres_connection():
    """
    Tạo kết nối đến PostgreSQL Target.
    """
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "datawarehouse"),
        password=os.environ.get("POSTGRES_PASSWORD", "dwhpassword"),
        database=os.environ.get("POSTGRES_DATABASE", "analytics_dwh"),
    )


def _run_scoped_staging_table(run_id: str) -> str:
    """
    Tạo tên bảng staging theo run.

    Ví dụ:
        orders_stg_20260616123045_ab12cd34

    Lợi ích:
    - Tránh conflict nếu có nhiều run chạy đồng thời do cấu hình sai.
    - Dễ forensic.
    - Cleanup orphan rõ ràng hơn.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    safe_run_id = re.sub(r"[^a-z0-9]", "", run_id.lower())[-8:]

    if not safe_run_id:
        safe_run_id = "unknown"

    return f"{STAGING_TABLE_PREFIX}{timestamp}_{safe_run_id}"


def _table_exists(cursor, table_name: str) -> bool:
    """
    Kiểm tra bảng có tồn tại trong schema public hay không.
    """
    cursor.execute("SELECT to_regclass(%s)", (table_name,))
    return cursor.fetchone()[0] is not None


def _drop_orphan_staging_tables(cursor) -> None:
    """
    Dọn dẹp các bảng staging orphan.

    Giả định an toàn:
    - Control Plane đang giới hạn 1 run chạy đồng thời.
    - Nếu có bảng staging tồn tại trước khi run mới bắt đầu,
      đó là orphan từ run trước hoặc bảng test.

    Nếu sau này bạn cho phép nhiều pipeline chạy song song,
    không được cleanup mạnh tay như thế này mà cần lock/ownership
    chi tiết hơn theo run hoặc theo pipeline.
    """
    cursor.execute(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE %s
        """,
        (STAGING_TABLE_PREFIX + "%",),
    )

    orphan_tables = [row[0] for row in cursor.fetchall()]

    for table_name in orphan_tables:
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
        )

    # Cleanup bảng staging cố định của kiến trúc cũ, nếu còn.
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(LEGACY_STAGING_TABLE_NAME)
        )
    )


# =================================================================
# ASSET 1: Contract Validation
# =================================================================

@asset(
    name="validate_orders_schema",
    description=(
        "Kiểm tra schema MySQL Source có khớp với Data Contract hay không.  "
        "Contract được quản lý như một sản phẩm nội bộ: có owner, version, "
        "consumer, compatibility policy và alert channels."
    ),
    group_name="orders_pipeline",
    tags={
        "pipeline": "orders",
        "stage": "contract",
    },
)
def validate_orders_schema(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Kết nối MySQL, đọc schema thực tế của bảng orders,
    sau đó so sánh với Data Contract.

    Hành vi:
    - Breaking violation: fail-fast.
    - Additive/unknown column: warning.
    - Deprecated column: warning.
    - Ghi metadata contract để dễ audit trong Dagster UI.
    """

    context.log.info(
        "🔍 Bắt đầu kiểm tra Data Contract cho bảng '%s'...",
        SOURCE_TABLE_NAME,
    )

    conn = get_mysql_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {SOURCE_TABLE_NAME}")

            actual_schema = {
                row["Field"].strip(): row["Type"].strip()
                for row in cursor.fetchall()
            }

        context.log.info(
            "📋 Schema thực tế từ Source: %s",
            sorted(actual_schema.keys()),
        )

        errors, warnings, compatibility = classify_schema_change(
            actual_schema,
            ORDER_CONTRACT,
        )

        # ---------------------------------------------------------
        # Warnings: không chặn pipeline, nhưng phải được ghi nhận
        # ---------------------------------------------------------
        for warning in warnings:
            context.log.warning("⚠️ %s", warning)

        # ---------------------------------------------------------
        # Errors: breaking changes -> fail-fast
        # ---------------------------------------------------------
        if errors:
            error_message = (
                "❌ DATA CONTRACT VIOLATION! "
                + "; ".join(errors)
            )

            context.log.error(error_message)
            context.log.error(
                "Owner: %s (%s)",
                ORDER_CONTRACT["owner_team"],
                ORDER_CONTRACT["owner_contact"],
            )
            context.log.error(
                "Producer: %s (%s)",
                ORDER_CONTRACT["producer_team"],
                ORDER_CONTRACT["producer_contact"],
            )
            context.log.error(
                "Alert channels: %s",
                ORDER_CONTRACT["alert_channels"],
            )
            context.log.error(
                "Violation response SLA: %s minutes",
                ORDER_CONTRACT["violation_response_sla_minutes"],
            )

            raise ValueError(error_message)

        context.log.info(
            "✅ Data Contract validation PASSED! "
            "Contract ID: %s, version: %s",
            ORDER_CONTRACT["contract_id"],
            ORDER_CONTRACT["version"],
        )

        context.add_output_metadata(
            {
                "contract_id": MetadataValue.text(
                    ORDER_CONTRACT["contract_id"]
                ),
                "contract_version": MetadataValue.text(
                    ORDER_CONTRACT["version"]
                ),
                "contract_status": MetadataValue.text(
                    ORDER_CONTRACT["status"]
                ),
                "owner_team": MetadataValue.text(
                    ORDER_CONTRACT["owner_team"]
                ),
                "owner_contact": MetadataValue.text(
                    ORDER_CONTRACT["owner_contact"]
                ),
                "producer_team": MetadataValue.text(
                    ORDER_CONTRACT["producer_team"]
                ),
                "producer_contact": MetadataValue.text(
                    ORDER_CONTRACT["producer_contact"]
                ),
                "compatibility": MetadataValue.text(compatibility),
                "warnings": MetadataValue.json(warnings),
                "alert_channels": MetadataValue.json(
                    ORDER_CONTRACT["alert_channels"]
                ),
                "validated_columns": MetadataValue.json(
                    sorted(actual_schema.keys())
                ),
            }
        )

        return {
            "status": "passed",
            "contract_id": ORDER_CONTRACT["contract_id"],
            "contract_version": ORDER_CONTRACT["version"],
            "columns_validated": len(actual_schema),
            "warnings": len(warnings),
        }

    except pymysql.Error as exc:
        context.log.error(
            "❌ Không thể kết nối MySQL Source để validate contract: %s",
            exc,
        )
        raise

    finally:
        conn.close()


# =================================================================
# ASSET 2: ETL - Extract & Load to Run-scoped Staging
# =================================================================

@asset(
    name="orders_staging",
    description=(
        "Extract dữ liệu từ MySQL và Load vào bảng staging theo run trong PostgreSQL. "
        "Bao gồm cleanup orphan staging và data quality checks."
    ),
    group_name="orders_pipeline",
    tags={"pipeline": "orders", "stage": "staging"},
)
def orders_staging(context: AssetExecutionContext) -> str:
    """
    1. Extract dữ liệu từ MySQL.
    2. Cleanup staging orphan.
    3. Tạo bảng staging mới theo run.
    4. INSERT dữ liệu vào staging.
    5. Chạy data quality checks.
    6. Trả về tên bảng staging cho asset production.
    """
    staging_table = _run_scoped_staging_table(context.run_id)

    context.log.info(
        "🚀 Bắt đầu ETL: MySQL -> PostgreSQL Staging. Staging table: %s",
        staging_table,
    )

    # -------------------------------------------------------------
    # Step 1: Extract từ MySQL
    # -------------------------------------------------------------
    mysql_conn = get_mysql_connection()

    try:
        with mysql_conn.cursor() as cursor:
            select_columns = ", ".join(TARGET_COLUMNS)
            cursor.execute(
                f"SELECT {select_columns} FROM {SOURCE_TABLE_NAME}"
            )
            rows = cursor.fetchall()

    except pymysql.Error as e:
        context.log.error(f"❌ Lỗi khi Extract từ MySQL: {e}")
        raise

    finally:
        mysql_conn.close()

    if len(rows) < MIN_EXPECTED_ROWS:
        error_msg = (
            f"❌ Source trả về {len(rows)} rows, "
            f"nhỏ hơn ngưỡng tối thiểu MIN_EXPECTED_ROWS={MIN_EXPECTED_ROWS}. "
            "Nếu việc source rỗng là hợp lệ, hãy cấu hình MIN_EXPECTED_ROWS=0 "
            "trong order_contract.py."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    df = pd.DataFrame(rows, columns=TARGET_COLUMNS)

    # Kiểm tra duplicate primary key trước khi load.
    duplicate_order_ids = int(df["order_id"].duplicated().sum())

    if duplicate_order_ids > 0:
        error_msg = (
            f"❌ Phát hiện {duplicate_order_ids} order_id bị trùng trong source. "
            "Pipeline fail-fast để tránh corrupt dữ liệu production."
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    # -------------------------------------------------------------
    # Step 2: Load vào PostgreSQL Staging
    # -------------------------------------------------------------
    pg_conn = get_postgres_connection()

    try:
        with pg_conn.cursor() as cur:
            # Advisory lock giúp serialize bước staging nếu có race bất ngờ.
            # Lưu ý: lock này chỉ giữ trong transaction của asset này,
            # không thay thế hoàn toàn single-flight ở Control Plane.
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (ORDERS_PIPELINE_LOCK_KEY,),
            )

            # Cleanup orphan staging từ run trước.
            _drop_orphan_staging_tables(cur)
            context.log.info("🧹 Đã cleanup các bảng staging orphan (nếu có).")

            # Tạo bảng staging mới.
            staging_ddl = sql.SQL(
                """
                CREATE TABLE {} (
                    order_id BIGINT NOT NULL PRIMARY KEY,
                    customer_id BIGINT NOT NULL,
                    amount NUMERIC(10,2) NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
                """
            ).format(sql.Identifier(staging_table))

            cur.execute(staging_ddl)
            context.log.info("🏗️ Đã tạo bảng staging mới: %s", staging_table)

            # Chuẩn bị dữ liệu.
            data_tuples = [
                tuple(row)
                for row in df.itertuples(index=False, name=None)
            ]

            insert_sql = sql.SQL(
                "INSERT INTO {} ({}) VALUES %s"
            ).format(
                sql.Identifier(staging_table),
                sql.SQL(", ").join(
                    [sql.Identifier(column) for column in TARGET_COLUMNS]
                ),
            ).as_string(cur)

            execute_values(
                cur,
                insert_sql,
                data_tuples,
                page_size=1000,
            )

            context.log.info(
                "✅ Đã INSERT %s rows vào %s.",
                len(data_tuples),
                staging_table,
            )

            # ---------------------------------------------------------
            # Step 3: Data Quality Checks
            # ---------------------------------------------------------

            # Check 1: NULL values ở các cột quan trọng.
            null_check_sql = sql.SQL(
                """
                SELECT COUNT(*)
                FROM {}
                WHERE order_id IS NULL
                   OR customer_id IS NULL
                   OR amount IS NULL
                   OR status IS NULL
                   OR created_at IS NULL
                """
            ).format(sql.Identifier(staging_table))

            cur.execute(null_check_sql)
            null_count = cur.fetchone()[0]

            if null_count > 0:
                error_msg = (
                    f"❌ Data Quality Check failed: có {null_count} rows chứa NULL "
                    "ở các cột bắt buộc trong bảng staging."
                )
                context.log.error(error_msg)
                raise ValueError(error_msg)

            # Check 2: Doanh thu âm.
            negative_amount_sql = sql.SQL(
                """
                SELECT COUNT(*)
                FROM {}
                WHERE amount < 0
                """
            ).format(sql.Identifier(staging_table))

            cur.execute(negative_amount_sql)
            negative_count = cur.fetchone()[0]

            if negative_count > 0:
                error_msg = (
                    f"❌ Data Quality Check failed: có {negative_count} rows "
                    "có amount < 0 trong bảng staging."
                )
                context.log.error(error_msg)
                raise ValueError(error_msg)

            # Check 3: Row count.
            row_count_sql = sql.SQL(
                "SELECT COUNT(*) FROM {}"
            ).format(sql.Identifier(staging_table))

            cur.execute(row_count_sql)
            row_count = cur.fetchone()[0]

            if row_count < MIN_EXPECTED_ROWS:
                error_msg = (
                    f"❌ Data Quality Check failed: bảng staging chỉ có {row_count} rows, "
                    f"nhỏ hơn MIN_EXPECTED_ROWS={MIN_EXPECTED_ROWS}."
                )
                context.log.error(error_msg)
                raise ValueError(error_msg)

        pg_conn.commit()

        context.add_output_metadata(
            {
                "staging_table": MetadataValue.text(staging_table),
                "rows_loaded": MetadataValue.text(str(row_count)),
                "run_id": MetadataValue.text(context.run_id),
                "pipeline_lock_key": MetadataValue.text(str(ORDERS_PIPELINE_LOCK_KEY)),
            }
        )

        context.log.info(
            "✅ Staging sẵn sàng: %s với %s rows.",
            staging_table,
            row_count,
        )

        return staging_table

    except (psycopg2.Error, ValueError) as e:
        pg_conn.rollback()
        context.log.error(f"❌ Lỗi khi Load vào PostgreSQL Staging: {e}")
        raise

    finally:
        pg_conn.close()


# =================================================================
# ASSET 3: Atomic Swap - Staging -> Production
# =================================================================

@asset(
    name="orders_production",
    description=(
        "Atomic Swap: Đổi tên bảng staging theo run thành production. "
        "Zero-downtime. Có advisory lock và giữ backup một thế hệ."
    ),
    group_name="orders_pipeline",
    tags={"pipeline": "orders", "stage": "production"},
)
def orders_production(
    context: AssetExecutionContext,
    orders_staging: str,
) -> Dict[str, Any]:
    """
    Nhận tên bảng staging từ asset orders_staging.

    Thực hiện Atomic Swap trong một transaction:

    1. Khóa advisory để serialize swap.
    2. Drop backup cũ nếu có.
    3. Rename production hiện tại -> backup.
    4. Rename staging -> production.
    5. Commit.

    Trong phiên bản này, bảng backup được GIỮ LẠI một thế hệ để hỗ trợ
    phục hồi nhanh. Nếu bạn muốn theo đúng nguyên bản "DROP backup ngay
    sau swap", hãy uncomment đoạn DROP backup ở cuối transaction.
    """
    staging_table = orders_staging
    backup_table = f"{PRODUCTION_TABLE_NAME}_backup"

    context.log.info(
        "🔄 Bắt đầu Atomic Swap: %s -> %s",
        staging_table,
        PRODUCTION_TABLE_NAME,
    )

    pg_conn = get_postgres_connection()

    try:
        with pg_conn.cursor() as cur:
            # Advisory lock transaction-scoped.
            # Nếu có transaction khác đang giữ lock, transaction này sẽ chờ.
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (ORDERS_PIPELINE_LOCK_KEY,),
            )

            if not _table_exists(cur, staging_table):
                error_msg = (
                    f"❌ Không tìm thấy bảng staging {staging_table}. "
                    "Có thể run cũ đã bị cleanup hoặc bạn đang cố materialize "
                    "asset production đơn lẻ mà không chạy toàn pipeline."
                )
                context.log.error(error_msg)
                raise ValueError(error_msg)

            production_existed = _table_exists(cur, PRODUCTION_TABLE_NAME)

            # Drop backup cũ từ lần swap trước.
            if _table_exists(cur, backup_table):
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(backup_table)
                    )
                )
                context.log.info("🗑️ Đã drop backup cũ: %s", backup_table)

            # Rename production hiện tại thành backup.
            if production_existed:
                cur.execute(
                    sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                        sql.Identifier(PRODUCTION_TABLE_NAME),
                        sql.Identifier(backup_table),
                    )
                )
                context.log.info(
                    "📦 Renamed %s -> %s",
                    PRODUCTION_TABLE_NAME,
                    backup_table,
                )

            # Rename staging thành production.
            cur.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    sql.Identifier(staging_table),
                    sql.Identifier(PRODUCTION_TABLE_NAME),
                )
            )

            context.log.info(
                "✨ Renamed %s -> %s",
                staging_table,
                PRODUCTION_TABLE_NAME,
            )

            # -----------------------------------------------------
            # OPTION 1: Giữ backup một thế hệ (đang bật)
            # -----------------------------------------------------
            # Backup được giữ lại để có thể rollback ngắn hạn.
            # Backup này sẽ được drop ở lần swap kế tiếp.
            #
            # OPTION 2: Drop backup ngay sau swap (giống kiến trúc gốc)
            # Nếu bạn muốn drop ngay, uncomment đoạn sau:
            #
            # if production_existed:
            #     cur.execute(
            #         sql.SQL("DROP TABLE IF EXISTS {}").format(
            #             sql.Identifier(backup_table)
            #         )
            #     )
            #     context.log.info("🗑️ Dropped backup table: %s", backup_table)
            # -----------------------------------------------------

        pg_conn.commit()

        context.add_output_metadata(
            {
                "swap_status": MetadataValue.text("success"),
                "production_table": MetadataValue.text(PRODUCTION_TABLE_NAME),
                "staging_table_used": MetadataValue.text(staging_table),
                "kept_backup_table": MetadataValue.text(
                    backup_table if production_existed else "none"
                ),
            }
        )

        context.log.info(
            "✅ Atomic Swap COMPLETED! Dữ liệu mới đã sẵn sàng cho CEO."
        )

        return {
            "swap_status": "success",
            "production_table": PRODUCTION_TABLE_NAME,
            "staging_table_used": staging_table,
            "kept_backup_table": backup_table if production_existed else None,
        }

    except (psycopg2.Error, ValueError) as e:
        pg_conn.rollback()
        context.log.error(f"❌ Lỗi khi Atomic Swap: {e}")
        context.log.info(
            "⏪ Transaction ROLLED BACK. Bảng production cũ vẫn an toàn nếu swap chưa commit."
        )
        raise

    finally:
        pg_conn.close()