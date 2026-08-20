# platform/dagster/healthchecks/webserver_healthcheck.py

import os
import sys
import urllib.request


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def main() -> int:
    http_timeout = int(os.environ.get("HEALTHCHECK_HTTP_TIMEOUT_SECONDS", "5"))
    url = "http://localhost:3000/health"

    # ---------------------------------------------------------
    # 1. HTTP liveness/readiness check cho Dagster webserver
    # ---------------------------------------------------------
    try:
        with urllib.request.urlopen(url, timeout=http_timeout) as response:
            if response.status != 200:
                eprint(
                    "Webserver healthcheck failed: "
                    f"HTTP status {response.status} from {url}"
                )
                return 1
    except Exception as exc:
        eprint(f"Webserver healthcheck failed: HTTP check error: {exc}")
        return 1

    # ---------------------------------------------------------
    # 2. Dependency health check: Dagster metadata PostgreSQL
    # ---------------------------------------------------------
    try:
        import psycopg2
    except Exception as exc:
        eprint(f"Webserver healthcheck failed: cannot import psycopg2: {exc}")
        return 1

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
            "Webserver healthcheck failed: missing env vars: "
            f"{missing_env_vars}"
        )
        return 1

    db_timeout = int(os.environ.get("HEALTHCHECK_DB_TIMEOUT_SECONDS", "5"))

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
            cursor.execute("SELECT 1")
            cursor.fetchone()

        connection.close()

    except Exception as exc:
        eprint(
            "Webserver healthcheck failed: metadata DB check error: "
            f"{exc}"
        )
        return 1

    print("Webserver healthcheck passed: HTTP OK, metadata DB OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())