"""
=================================================================
DATA ASSETS: Orders Pipeline
=================================================================
Pipeline này thể hiện 3 bước cốt lõi của DataOps Production-grade:
1. Contract Validation (Fail-Fast)
2. ETL: Extract from MySQL -> Load to PostgreSQL Staging
3. Atomic Swap: Staging -> Production

Mỗi @asset đại diện cho một "Tài sản dữ liệu" (Data Asset).
Dagster sẽ tự động quản lý dependency giữa chúng.
"""

import os
from typing import Dict, Any

import pandas as pd
import pymysql
import psycopg2
from dagster import asset, AssetExecutionContext, MetadataValue

# Import Data Contract
from user_code.contracts.order_contract import (
    EXPECTED_ORDERS_SCHEMA,
    REQUIRED_COLUMNS,
    SOURCE_TABLE_NAME,
    STAGING_TABLE_NAME,
    PRODUCTION_TABLE_NAME,
)


# =================================================================
# Helper Functions (Các hàm tiện ích)
# =================================================================

def get_mysql_connection():
    """Tạo kết nối đến MySQL Source."""
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "datauser"),
        password=os.environ.get("MYSQL_PASSWORD", "datapassword"),
        database=os.environ.get("MYSQL_DATABASE", "sales_db"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_postgres_connection():
    """Tạo kết nối đến PostgreSQL Target."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "datawarehouse"),
        password=os.environ.get("POSTGRES_PASSWORD", "dwhpassword"),
        database=os.environ.get("POSTGRES_DATABASE", "analytics_dwh"),
    )


# =================================================================
# ASSET 1: Contract Validation (Chốt chặn Schema)
# =================================================================
# Asset này KHÔNG tạo ra dữ liệu. Nó tạo ra "Sự an tâm".
# Nếu validation fail, Dagster sẽ dừng toàn bộ pipeline ngay lập tức.
# Đây là hiện thực hóa của khái niệm "Fail-Fast over Silent Corruption".

@asset(
    name="validate_orders_schema",
    description="Kiểm tra schema MySQL Source có khớp với Data Contract không. Fail-fast nếu sai lệch.",
    group_name="orders_pipeline",
)
def validate_orders_schema(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Kết nối MySQL, đọc schema thực tế của bảng orders,
    so sánh với EXPECTED_ORDERS_SCHEMA trong Data Contract.
    """
    context.log.info("🔍 Bắt đầu kiểm tra Data Contract cho bảng '%s'...", SOURCE_TABLE_NAME)
    
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            # Lấy thông tin schema thực tế từ MySQL
            cursor.execute(f"DESCRIBE {SOURCE_TABLE_NAME}")
            actual_schema = {row["Field"]: row["Type"] for row in cursor.fetchall()}
        
        context.log.info(f"📋 Schema thực tế từ Source: {list(actual_schema.keys())}")
        
        # Kiểm tra 1: Tất cả REQUIRED_COLUMNS phải tồn tại
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in actual_schema]
        if missing_columns:
            error_msg = (
                f"❌ DATA CONTRACT VIOLATION! "
                f"Thiếu các cột bắt buộc: {missing_columns}. "
                f"Schema thực tế: {list(actual_schema.keys())}"
            )
            context.log.error(error_msg)
            # Raise exception để Dagster đánh dấu asset này FAILED
            # và KHÔNG chạy các asset downstream
            raise ValueError(error_msg)
        
        # Kiểm tra 2: Kiểu dữ liệu phải khớp (kiểm tra cơ bản)
        type_mismatches = []
        for col, expected_type in EXPECTED_ORDERS_SCHEMA.items():
            if col in actual_schema:
                actual_type = actual_schema[col].lower()
                # Kiểm tra đơn giản: xem expected_type có phải là prefix của actual_type không
                # Ví dụ: expected "int" khớp với actual "int(11)"
                if not actual_type.startswith(expected_type):
                    type_mismatches.append(
                        f"Cột '{col}': Kỳ vọng '{expected_type}', Thực tế '{actual_type}'"
                    )
        
        if type_mismatches:
            error_msg = (
                f"❌ DATA CONTRACT VIOLATION! "
                f"Sai lệch kiểu dữ liệu: {type_mismatches}"
            )
            context.log.error(error_msg)
            raise ValueError(error_msg)
        
        context.log.info("✅ Data Contract validation PASSED! Schema khớp với hợp đồng.")
        
        # Ghi metadata vào Dagster UI để Data Engineer có thể quan sát
        context.add_output_metadata({
            "validated_columns": MetadataValue.json(list(actual_schema.keys())),
            "contract_version": MetadataValue.text("v1.0"),
            "source_table": MetadataValue.text(SOURCE_TABLE_NAME),
        })
        
        return {"status": "passed", "columns_validated": len(actual_schema)}
    
    except pymysql.Error as e:
        context.log.error(f"❌ Không thể kết nối MySQL Source: {e}")
        raise
    finally:
        conn.close()


# =================================================================
# ASSET 2: ETL - Extract & Load to Staging
# =================================================================
# Asset này phụ thuộc vào validate_orders_schema.
# Chỉ chạy khi validation PASSED.

@asset(
    name="orders_staging",
    deps=[validate_orders_schema],  # Dependency: Chạy sau khi validation pass
    description="Extract dữ liệu từ MySQL và Load vào bảng Staging trong PostgreSQL.",
    group_name="orders_pipeline",
)
def orders_staging(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    1. Kết nối MySQL, SELECT toàn bộ dữ liệu từ bảng orders.
    2. Kết nối PostgreSQL, DROP bảng staging cũ (nếu có).
    3. CREATE bảng staging mới.
    4. INSERT dữ liệu vào bảng staging.
    """
    context.log.info("🚀 Bắt đầu ETL: MySQL -> PostgreSQL Staging...")
    
    # Step 1: Extract từ MySQL
    mysql_conn = get_mysql_connection()
    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {SOURCE_TABLE_NAME}")
            rows = cursor.fetchall()
        
        context.log.info(f"📦 Extracted {len(rows)} rows from MySQL.")
        
        if not rows:
            context.log.warning("⚠️ Không có dữ liệu nào từ Source. Bảng Staging sẽ trống.")
            return {"rows_loaded": 0}
        
        # Chuyển đổi thành DataFrame để dễ xử lý
        df = pd.DataFrame(rows)
        
    except pymysql.Error as e:
        context.log.error(f"❌ Lỗi khi Extract từ MySQL: {e}")
        raise
    finally:
        mysql_conn.close()
    
    # Step 2 & 3 & 4: Load vào PostgreSQL Staging
    pg_conn = get_postgres_connection()
    try:
        with pg_conn.cursor() as cursor:
            # DROP bảng staging cũ (nếu có)
            # Đây là bước chuẩn bị cho Atomic Swap
            cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME}")
            context.log.info(f"🗑️ Dropped bảng staging cũ (nếu có).")
            
            # CREATE bảng staging mới dựa trên DataFrame schema
            # Trong production, bạn nên định nghĩa DDL rõ ràng thay vì dynamic như thế này
            columns_ddl = []
            for col in df.columns:
                # Mapping đơn giản từ pandas dtype sang PostgreSQL type
                if pd.api.types.is_integer_dtype(df[col]):
                    pg_type = "BIGINT"
                elif pd.api.types.is_float_dtype(df[col]):
                    pg_type = "NUMERIC"
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    pg_type = "TIMESTAMP"
                else:
                    pg_type = "TEXT"
                columns_ddl.append(f"{col} {pg_type}")
            
            create_stmt = f"CREATE TABLE {STAGING_TABLE_NAME} ({', '.join(columns_ddl)})"
            cursor.execute(create_stmt)
            context.log.info(f"🏗️ Created bảng staging mới: {STAGING_TABLE_NAME}")
            
            # INSERT dữ liệu vào bảng staging
            # Sử dụng executemany để tối ưu performance
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_stmt = f"INSERT INTO {STAGING_TABLE_NAME} ({', '.join(df.columns)}) VALUES ({placeholders})"
            
            # Chuyển DataFrame thành list of tuples
            data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
            cursor.executemany(insert_stmt, data_tuples)
            
            # Commit transaction
            pg_conn.commit()
            context.log.info(f"✅ Loaded {len(data_tuples)} rows into {STAGING_TABLE_NAME}.")
        
        return {"rows_loaded": len(data_tuples), "staging_table": STAGING_TABLE_NAME}
    
    except psycopg2.Error as e:
        context.log.error(f"❌ Lỗi khi Load vào PostgreSQL: {e}")
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


# =================================================================
# ASSET 3: Atomic Swap - Staging -> Production
# =================================================================
# Đây là bước cuối cùng, thể hiện Pattern C (Atomic Swap).
# Nếu bước này fail, bảng Production cũ vẫn nguyên vẹn.

@asset(
    name="orders_production",
    deps=[orders_staging],  # Dependency: Chạy sau khi staging load xong
    description="Atomic Swap: Đổi tên bảng Staging thành Production. Zero-downtime.",
    group_name="orders_pipeline",
)
def orders_production(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Thực hiện Atomic Swap trong một transaction:
    1. Đổi tên bảng Production cũ thành _backup.
    2. Đổi tên bảng Staging thành Production.
    3. Xóa bảng _backup.
    
    Nếu bất kỳ bước nào fail, toàn bộ transaction sẽ ROLLBACK.
    Bảng Production cũ vẫn còn nguyên vẹn -> Không có Data Blackout.
    """
    context.log.info("🔄 Bắt đầu Atomic Swap: Staging -> Production...")
    
    pg_conn = get_postgres_connection()
    try:
        with pg_conn.cursor() as cursor:
            # Bắt đầu transaction
            # PostgreSQL DDL là transactional, nên chúng ta có thể
            # ROLLBACK cả lệnh ALTER TABLE RENAME nếu có lỗi.
            
            # Kiểm tra bảng Production cũ có tồn tại không
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{PRODUCTION_TABLE_NAME}'
                )
            """)
            production_exists = cursor.fetchone()[0]
            
            backup_table = f"{PRODUCTION_TABLE_NAME}_backup"
            
            if production_exists:
                # Drop bảng backup cũ nếu có (từ lần swap trước bị fail giữa chừng)
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
                
                # Step 1: Rename Production -> Backup
                cursor.execute(f"ALTER TABLE {PRODUCTION_TABLE_NAME} RENAME TO {backup_table}")
                context.log.info(f"📦 Renamed {PRODUCTION_TABLE_NAME} -> {backup_table}")
            
            # Step 2: Rename Staging -> Production
            cursor.execute(f"ALTER TABLE {STAGING_TABLE_NAME} RENAME TO {PRODUCTION_TABLE_NAME}")
            context.log.info(f"✨ Renamed {STAGING_TABLE_NAME} -> {PRODUCTION_TABLE_NAME}")
            
            # Step 3: Drop bảng backup (cleanup)
            if production_exists:
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
                context.log.info(f"🗑️ Dropped backup table: {backup_table}")
            
            # Commit transaction
            # Nếu mọi thứ thành công, commit để apply changes
            pg_conn.commit()
            context.log.info("✅ Atomic Swap COMPLETED! Dữ liệu mới đã sẵn sàng cho CEO.")
        
        # Ghi metadata để quan sát trên Dagster UI
        context.add_output_metadata({
            "swap_status": MetadataValue.text("success"),
            "production_table": MetadataValue.text(PRODUCTION_TABLE_NAME),
            "previous_production_backed_up": MetadataValue.bool(production_exists),
        })
        
        return {"swap_status": "success", "production_table": PRODUCTION_TABLE_NAME}
    
    except psycopg2.Error as e:
        context.log.error(f"❌ Lỗi khi Atomic Swap: {e}")
        # ROLLBACK toàn bộ transaction
        # Bảng Production cũ vẫn còn nguyên vẹn
        pg_conn.rollback()
        context.log.info("⏪ Transaction ROLLED BACK. Bảng Production cũ vẫn an toàn.")
        raise
    finally:
        pg_conn.close()