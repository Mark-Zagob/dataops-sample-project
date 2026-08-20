# platform/dagster/healthchecks/data_plane_healthcheck.py

import os
import sys


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def main() -> int:
    db_timeout = int(os.environ.get("HEALTHCHECK_DB_TIMEOUT_SECONDS", "5"))
    min_expected_rows = int(os.environ.get("DATA_HEALTH_MIN_EXPECTED_ROWS", "1"))
    max_age_hours = float(os.environ.get("DATA_HEALTH_MAX_AGE_HOURS", "26"))

    required_env_vars = [
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DATABASE",
    ]

    missing_env_vars = [
        name for name in required_env_vars if not os.environ.get(name)
    ]

    if missing_env_vars:
        eprint(
            "Data plane healthcheck failed: missing env vars: "
            f"{missing_env_vars}"
        )
        return 1

    try:
        import psycopg2
    except Exception as exc:
        eprint(
            f"Data plane healthcheck failed: cannot import psycopg2: {exc}"
        )
        return 1

    connection = None

    try:
        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DATABASE"],
            connect_timeout=db_timeout,
        )

        with connection.cursor() as cursor:
            # -----------------------------------------------------
            # 1. Kiểm tra bảng production có tồn tại không
            # -----------------------------------------------------
            cursor.execute("SELECT to_regclass('public.orders_production')")
            production_exists = cursor.fetchone()[0] is not None

            if not production_exists:
                eprint(
                    "Data plane healthcheck failed: "
                    "orders_production table does not exist"
                )
                return 1

            # -----------------------------------------------------
            # 2. Kiểm tra row count tối thiểu
            # -----------------------------------------------------
            cursor.execute("SELECT COUNT(*) FROM orders_production")
            row_count = cursor.fetchone()[0]

            if row_count < min_expected_rows:
                eprint(
                    "Data plane healthcheck failed: "
                    f"row_count={row_count}, "
                    f"min_expected_rows={min_expected_rows}"
                )
                return 1

            # -----------------------------------------------------
            # 3. Kiểm tra NULL ở các cột bắt buộc
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM orders_production
                WHERE order_id IS NULL
                   OR customer_id IS NULL
                   OR amount IS NULL
                   OR status IS NULL
                   OR created_at IS NULL
                """
            )

            null_count = cursor.fetchone()[0]

            if null_count > 0:
                eprint(
                    "Data plane healthcheck failed: "
                    f"null_count={null_count} in required columns"
                )
                return 1

            # -----------------------------------------------------
            # 4. Kiểm tra amount âm
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM orders_production
                WHERE amount < 0
                """
            )

            negative_amount_count = cursor.fetchone()[0]

            if negative_amount_count > 0:
                eprint(
                    "Data plane healthcheck failed: "
                    f"negative_amount_count={negative_amount_count}"
                )
                return 1

            # -----------------------------------------------------
            # 5. Kiểm tra duplicate order_id
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT order_id
                    FROM orders_production
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) duplicated_orders
                """
            )

            duplicate_count = cursor.fetchone()[0]

            if duplicate_count > 0:
                eprint(
                    "Data plane healthcheck failed: "
                    f"duplicate_order_id_groups={duplicate_count}"
                )
                return 1

            # -----------------------------------------------------
            # 6. Kiểm tra freshness dựa trên created_at
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600.0
                FROM orders_production
                """
            )

            age_hours = cursor.fetchone()[0]

            if age_hours is None:
                eprint(
                    "Data plane healthcheck failed: "
                    "cannot determine max(created_at)"
                )
                return 1

            if age_hours > max_age_hours:
                eprint(
                    "Data plane healthcheck failed: "
                    f"data is too old. age_hours={age_hours:.2f}, "
                    f"max_age_hours={max_age_hours:.2f}"
                )
                return 1

            # -----------------------------------------------------
            # 7. Kiểm tra số bảng backup
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public'
                  AND tablename = 'orders_production_backup'
                """
            )

            backup_count = cursor.fetchone()[0]

            if backup_count > 1:
                eprint(
                    "Data plane healthcheck failed: "
                    f"backup_count={backup_count}, expected <= 1"
                )
                return 1

            # -----------------------------------------------------
            # 8. Cảnh báo staging orphan
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public'
                  AND tablename LIKE 'orders_stg_%'
                """
            )

            staging_count = cursor.fetchone()[0]

            if staging_count > 0:
                print(
                    "Data plane healthcheck warning: "
                    f"staging_count={staging_count}. "
                    "If no pipeline run is active, investigate orphan staging."
                )

        print(
            "Data plane healthcheck passed: "
            "production table exists, quality checks passed, freshness OK"
        )
        return 0

    except Exception as exc:
        eprint(f"Data plane healthcheck failed: unexpected error: {exc}")
        return 1

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    sys.exit(main())