# platform/dagster/healthchecks/daemon_healthcheck.py

import os
import sys
import time


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def get_column_type(cursor, table_name: str, column_name: str):
    cursor.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )

    row = cursor.fetchone()
    return row[0].lower() if row else None


def main() -> int:
    db_timeout = int(os.environ.get("HEALTHCHECK_DB_TIMEOUT_SECONDS", "5"))
    max_stale_seconds = int(
        os.environ.get("DAGSTER_DAEMON_HEARTBEAT_MAX_STALE_SECONDS", "120")
    )

    required_env_vars = [
        "DAGSTER_METADATA_HOST",
        "DAGSTER_METADATA_USER",
        "DAGSTER_METADATA_PASSWORD",
        "DAGSTER_METADATA_DB",
    ]

    missing_env_vars = [
        name for name in required_env_vars if not os.environ.get(name)
    ]

    if missing_env_vars:
        eprint(
            "Daemon healthcheck failed: missing env vars: "
            f"{missing_env_vars}"
        )
        return 1

    try:
        import psycopg2
    except Exception as exc:
        eprint(f"Daemon healthcheck failed: cannot import psycopg2: {exc}")
        return 1

    connection = None

    try:
        connection = psycopg2.connect(
            host=os.environ["DAGSTER_METADATA_HOST"],
            port=int(os.environ.get("DAGSTER_METADATA_PORT", "5432")),
            user=os.environ["DAGSTER_METADATA_USER"],
            password=os.environ["DAGSTER_METADATA_PASSWORD"],
            dbname=os.environ["DAGSTER_METADATA_DB"],
            connect_timeout=db_timeout,
        )

        with connection.cursor() as cursor:
            # -----------------------------------------------------
            # 1. Kiểm tra bảng heartbeat có tồn tại không
            # -----------------------------------------------------
            cursor.execute("SELECT to_regclass('public.daemon_heartbeats')")
            table_exists = cursor.fetchone()[0] is not None

            if not table_exists:
                eprint(
                    "Daemon healthcheck failed: "
                    "table public.daemon_heartbeats not found"
                )
                return 1

            # -----------------------------------------------------
            # 2. Đọc danh sách cột để xử lý linh hoạt schema
            # -----------------------------------------------------
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'daemon_heartbeats'
                """
            )

            columns = {row[0].lower() for row in cursor.fetchall()}

            if "timestamp" not in columns:
                eprint(
                    "Daemon healthcheck failed: "
                    "daemon_heartbeats table does not have timestamp column"
                )
                return 1

            timestamp_type = get_column_type(
                cursor,
                "daemon_heartbeats",
                "timestamp",
            )

            if timestamp_type is None:
                eprint(
                    "Daemon healthcheck failed: "
                    "cannot determine timestamp column type"
                )
                return 1

            timestamp_column = '"timestamp"'

            if timestamp_type in (
                "timestamp with time zone",
                "timestamp without time zone",
            ):
                timestamp_expression = f"EXTRACT(EPOCH FROM {timestamp_column})"
                fresh_predicate = f"{timestamp_column} > TO_TIMESTAMP(%s)"
            else:
                # Ví dụ: double precision, real, numeric, bigint
                timestamp_expression = timestamp_column
                fresh_predicate = f"{timestamp_column} > %s"

            daemon_type = "RUN_COORDINATOR"

            # -----------------------------------------------------
            # 3. Kiểm tra heartbeat mới nhất của RUN_COORDINATOR
            # -----------------------------------------------------
            cursor.execute(
                f"""
                SELECT MAX({timestamp_expression})
                FROM daemon_heartbeats
                WHERE UPPER(daemon_type) = %s
                """,
                (daemon_type,),
            )

            latest_heartbeat = cursor.fetchone()[0]

            if latest_heartbeat is None:
                eprint(
                    "Daemon healthcheck failed: "
                    f"no heartbeat found for daemon_type={daemon_type}"
                )
                return 1

            age_seconds = time.time() - float(latest_heartbeat)

            if age_seconds > max_stale_seconds:
                eprint(
                    "Daemon healthcheck failed: "
                    f"{daemon_type} heartbeat is stale. "
                    f"age_seconds={age_seconds:.1f}, "
                    f"max_stale_seconds={max_stale_seconds}"
                )
                return 1

            # -----------------------------------------------------
            # 4. Kiểm tra split-brain cơ bản
            # -----------------------------------------------------
            if "daemon_id" in columns:
                cutoff_epoch = time.time() - max_stale_seconds

                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT daemon_id)
                    FROM daemon_heartbeats
                    WHERE UPPER(daemon_type) = %s
                      AND {fresh_predicate}
                    """,
                    (daemon_type, cutoff_epoch),
                )

                active_daemon_count = cursor.fetchone()[0]

                if active_daemon_count > 1:
                    eprint(
                        "Daemon healthcheck failed: possible split-brain. "
                        f"active_daemon_count={active_daemon_count} "
                        f"for daemon_type={daemon_type}"
                    )
                    return 1

        print(
            "Daemon healthcheck passed: "
            "metadata DB OK, RUN_COORDINATOR heartbeat fresh"
        )
        return 0

    except Exception as exc:
        eprint(f"Daemon healthcheck failed: unexpected error: {exc}")
        return 1

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    sys.exit(main())