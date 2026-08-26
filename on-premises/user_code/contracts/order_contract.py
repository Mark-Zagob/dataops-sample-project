# user_code/contracts/order_contract.py

"""
=================================================================
DATA CONTRACT: sales_db.orders
=================================================================

Contract này không chỉ là một file cấu hình kỹ thuật.
Nó là một sản phẩm nội bộ giữa:

- Source Owner / Producer Team
- Platform Engineering
- Data Engineering
- Analytics / Business consumers

Mọi thay đổi đến contract này phải tuân theo:
docs/processes/data-contract-change-process.md

Nguyên tắc:
- Fail-fast nếu vi phạm breaking contract.
- Backward-compatible additive changes được phép nhưng cần được ghi nhận.
- Breaking changes phải có approval, deprecation timeline và migration plan.
"""

import re
from typing import Any, Dict, List, Tuple


# =================================================================
# REQUIRED METADATA FIELDS
# =================================================================

CONTRACT_REQUIRED_FIELDS = (
    "contract_id",
    "version",
    "status",
    "domain",
    "dataset",
    "source_system",
    "source_table",
    "production_table",
    "owner_team",
    "owner_contact",
    "producer_team",
    "producer_contact",
    "consumers",
    "compatibility_policy",
    "non_breaking_changes",
    "breaking_changes",
    "deprecation_timeline_days",
    "review_process",
    "violation_response_sla_minutes",
    "alert_channels",
    "min_expected_rows",
    "freshness_max_age_hours",
    "pipeline_lock_key",
    "staging_table_prefix",
    "legacy_staging_table_name",
    "deprecated_columns",
    "target_columns",
    "required_columns",
    "expected_schema",
)

CONSUMER_REQUIRED_FIELDS = (
    "team",
    "contact",
    "use_case",
    "criticality",
    "freshness_sla",
)


# =================================================================
# DATA CONTRACT PRODUCT DEFINITION
# =================================================================

ORDER_CONTRACT: Dict[str, Any] = {
    # -------------------------------------------------------------
    # Contract identity
    # -------------------------------------------------------------
    "contract_id": "sales_db.orders.v1",
    "version": "1.0.0",
    "status": "active",  # draft | active | deprecated | retired

    # -------------------------------------------------------------
    # Domain / dataset
    # -------------------------------------------------------------
    "domain": "sales",
    "dataset": "orders",

    # -------------------------------------------------------------
    # Source / target binding
    # -------------------------------------------------------------
    "source_system": "MySQL sales_db",
    "source_table": "orders",
    "production_table": "orders_production",

    # -------------------------------------------------------------
    # Ownership
    # -------------------------------------------------------------
    # Trong production thực tế, owner nên là domain owner của dữ liệu.
    # Platform team có thể là co-owner về mặt kỹ thuật.
    "owner_team": "Platform Engineering",
    "owner_contact": "#platform-support",

    # Producer là team sở hữu hệ thống source.
    "producer_team": "Sales Application Team",
    "producer_contact": "#sales-app-team",

    # -------------------------------------------------------------
    # Consumers
    # -------------------------------------------------------------
    "consumers": [
        {
            "team": "Data Engineering",
            "contact": "#data-engineering",
            "use_case": "orders_production materialization",
            "criticality": "high",
            "freshness_sla": "daily before 07:30",
        },
        {
            "team": "Analytics/Executive",
            "contact": "#analytics-support",
            "use_case": "daily orders dashboard",
            "criticality": "high",
            "freshness_sla": "daily before 07:30",
        },
    ],

    # -------------------------------------------------------------
    # Compatibility policy
    # -------------------------------------------------------------
    "compatibility_policy": "backward_compatible_additive_only",

    "non_breaking_changes": [
        "add_optional_source_column",
        "increase_varchar_length",
        "increase_decimal_precision_with_same_scale",
        "add_index",
        "add_comment",
        "widen_integer_type_safe",
    ],

    "breaking_changes": [
        "drop_required_column",
        "rename_required_column",
        "change_required_column_type_incompatible",
        "decrease_varchar_length",
        "change_decimal_scale",
        "change_primary_key_semantics",
        "change_nullability_of_required_column",
    ],

    # -------------------------------------------------------------
    # Deprecation / governance
    # -------------------------------------------------------------
    "deprecation_timeline_days": 30,
    "review_process": (
        "Pull request must be approved by Platform Engineering, "
        "Data Engineering Lead, and Source Owner."
    ),
    "violation_response_sla_minutes": 60,
    "alert_channels": [
        "#data-contract-alerts",
        "#sre-oncall",
    ],

    # -------------------------------------------------------------
    # Operational thresholds
    # -------------------------------------------------------------
    "min_expected_rows": 1,
    "freshness_max_age_hours": 26,

    # -------------------------------------------------------------
    # Pipeline internal binding
    # -------------------------------------------------------------
    "pipeline_lock_key": 906033,
    "staging_table_prefix": "orders_stg_",
    "legacy_staging_table_name": "orders_staging",

    # -------------------------------------------------------------
    # Column lifecycle
    # -------------------------------------------------------------
    "deprecated_columns": [],

    # -------------------------------------------------------------
    # Columns consumed by target pipeline
    # -------------------------------------------------------------
    "target_columns": [
        "order_id",
        "customer_id",
        "amount",
        "status",
        "created_at",
    ],

    # -------------------------------------------------------------
    # Required source columns
    # -------------------------------------------------------------
    "required_columns": [
        "order_id",
        "customer_id",
        "amount",
        "status",
        "created_at",
    ],

    # -------------------------------------------------------------
    # Expected source schema
    # -------------------------------------------------------------
    # Dùng kiểu đủ ràng buộc hơn so với prefix check đơn giản:
    # - decimal(10,2): scale phải giữ nguyên, precision có thể tăng
    # - varchar(50): length có thể tăng, không được giảm
    # - int: có thể chấp nhận widen lên bigint trong helper phía dưới
    # -------------------------------------------------------------
    "expected_schema": {
        "order_id": "int",
        "customer_id": "int",
        "amount": "decimal(10,2)",
        "status": "varchar(50)",
        "created_at": "datetime",
    },
}


# =================================================================
# BACKWARD COMPATIBLE CONSTANTS
# =================================================================
# Giữ lại các tên cũ để các asset hiện tại không bị phá vỡ.

SOURCE_TABLE_NAME = ORDER_CONTRACT["source_table"]
PRODUCTION_TABLE_NAME = ORDER_CONTRACT["production_table"]
STAGING_TABLE_PREFIX = ORDER_CONTRACT["staging_table_prefix"]
LEGACY_STAGING_TABLE_NAME = ORDER_CONTRACT["legacy_staging_table_name"]
TARGET_COLUMNS = list(ORDER_CONTRACT["target_columns"])
REQUIRED_COLUMNS = list(ORDER_CONTRACT["required_columns"])
EXPECTED_ORDERS_SCHEMA = dict(ORDER_CONTRACT["expected_schema"])
ORDERS_PIPELINE_LOCK_KEY = ORDER_CONTRACT["pipeline_lock_key"]
MIN_EXPECTED_ROWS = ORDER_CONTRACT["min_expected_rows"]


# =================================================================
# TYPE COMPATIBILITY HELPERS
# =================================================================

NUMERIC_WIDENING = {
    "tinyint": {"smallint", "mediumint", "int", "integer", "bigint"},
    "smallint": {"mediumint", "int", "integer", "bigint"},
    "mediumint": {"int", "integer", "bigint"},
    "int": {"integer", "bigint"},
    "integer": {"int", "bigint"},
}


def _normalize_sql_type(sql_type: str) -> str:
    """
    Chuẩn hóa kiểu SQL:
    - lowercase
    - bỏ khoảng trắng thừa
    - bỏ unsigned / zerofill
    """
    normalized = sql_type.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\s+unsigned", "", normalized)
    normalized = re.sub(r"\s+zerofill", "", normalized)
    return normalized.strip()


def _base_type(sql_type: str) -> str:
    """
    Lấy phần kiểu gốc.
    Ví dụ:
      varchar(50) -> varchar
      decimal(10,2) -> decimal
    """
    normalized = _normalize_sql_type(sql_type)
    return normalized.split("(", 1)[0].strip()


def _type_params(sql_type: str) -> str:
    """
    Lấy phần tham số trong ngoặc.
    Ví dụ:
      varchar(50) -> 50
      decimal(10,2) -> 10,2
    """
    normalized = _normalize_sql_type(sql_type)
    if "(" not in normalized or not normalized.endswith(")"):
        return ""
    return normalized.split("(", 1)[1].rsplit(")", 1)[0].strip()


def _parse_int(value: str) -> int:
    try:
        return int(value.strip())
    except Exception:
        return -1


def _parse_decimal_params(params: str) -> Tuple[int, int]:
    """
    Trả về (precision, scale).
    Nếu không parse được, trả về (-1, 0).
    """
    if not params:
        return -1, 0

    parts = [part.strip() for part in params.split(",")]
    precision = _parse_int(parts[0]) if parts else -1
    scale = _parse_int(parts[1]) if len(parts) > 1 else 0

    return precision, scale


def is_type_compatible(expected_type: str, actual_type: str) -> bool:
    """
    Kiểm tra kiểu thực tế có tương thích với kiểu kỳ vọng hay không.

    Nguyên tắc:
    - Cùng base type là nền tảng.
    - varchar/char: length thực tế phải >= length kỳ vọng.
    - decimal: precision thực tế có thể >= kỳ vọng, nhưng scale phải giữ nguyên.
    - integer: cho phép widening an toàn, ví dụ int -> bigint.
    """

    expected_base = _base_type(expected_type)
    actual_base = _base_type(actual_type)

    if expected_base != actual_base:
        return actual_base in NUMERIC_WIDENING.get(expected_base, set())

    expected_params = _type_params(expected_type)
    actual_params = _type_params(actual_type)

    # Nếu contract không khai báo tham số, dùng chế độ prefix-compatible đơn giản.
    if not expected_params:
        return True

    # -------------------------------------------------------------
    # varchar / char length compatibility
    # -------------------------------------------------------------
    if expected_base in {"varchar", "char", "varbinary", "binary"}:
        expected_length = _parse_int(expected_params)
        actual_length = _parse_int(actual_params)

        if expected_length < 0 or actual_length < 0:
            return True

        return actual_length >= expected_length

    # -------------------------------------------------------------
    # decimal precision/scale compatibility
    # -------------------------------------------------------------
    if expected_base == "decimal":
        expected_precision, expected_scale = _parse_decimal_params(expected_params)
        actual_precision, actual_scale = _parse_decimal_params(actual_params)

        if expected_precision < 0 or actual_precision < 0:
            return True

        return actual_precision >= expected_precision and actual_scale == expected_scale

    # -------------------------------------------------------------
    # Fallback: exact params
    # -------------------------------------------------------------
    return expected_params == actual_params


# =================================================================
# CONTRACT DEFINITION VALIDATION
# =================================================================

def validate_contract_definition(contract: Dict[str, Any]) -> List[str]:
    """
    Kiểm tra bản thân contract có đầy đủ metadata sản phẩm hay không.

    Hàm này không kiểm tra schema ngoài database.
    Nó kiểm tra tính hoàn chỉnh của contract như một sản phẩm nội bộ.
    """

    errors: List[str] = []

    # -------------------------------------------------------------
    # Required fields
    # -------------------------------------------------------------
    # Phân biệt rõ hai loại lỗi:
    # 1. Field hoàn toàn không tồn tại trong contract.
    # 2. Field tồn tại nhưng rỗng.
    #
    # Một số field bắt buộc phải khai báo, nhưng được phép rỗng.
    # Ví dụ:
    #   deprecated_columns = []
    # nghĩa là chưa có cột nào bị deprecated. Đây là trạng thái hợp lệ.
    # -------------------------------------------------------------
    for field in CONTRACT_REQUIRED_FIELDS:
        if field not in contract:
            errors.append(f"Contract missing required field: {field}")
            continue

        value = contract[field]

        # deprecated_columns là field lifecycle bắt buộc phải khai báo,
        # nhưng danh sách rỗng là hợp lệ.
        if field == "deprecated_columns":
            if value is None or not isinstance(value, list):
                errors.append("Contract deprecated_columns must be a list.")
            continue

        if value is None or value == "" or value == [] or value == {}:
            errors.append(f"Contract missing required field: {field}")

    # -------------------------------------------------------------
    # Status lifecycle
    # -------------------------------------------------------------
    status = contract.get("status")
    if status not in {"draft", "active", "deprecated", "retired"}:
        errors.append(
            f"Contract status '{status}' is invalid. "
            "Expected one of: draft, active, deprecated, retired."
        )

    # -------------------------------------------------------------
    # Schema consistency
    # -------------------------------------------------------------
    expected_schema = contract.get("expected_schema", {})
    required_columns = contract.get("required_columns", [])
    target_columns = contract.get("target_columns", [])

    if not isinstance(expected_schema, dict) or not expected_schema:
        errors.append("Contract expected_schema must be a non-empty dict.")
    if not isinstance(required_columns, list) or not required_columns:
        errors.append("Contract required_columns must be a non-empty list.")
    if not isinstance(target_columns, list) or not target_columns:
        errors.append("Contract target_columns must be a non-empty list.")

    if isinstance(expected_schema, dict) and isinstance(required_columns, list):
        missing_required_in_schema = [
            column for column in required_columns if column not in expected_schema
        ]
        if missing_required_in_schema:
            errors.append(
                "Required columns missing from expected_schema: "
                f"{sorted(missing_required_in_schema)}"
            )

    if isinstance(expected_schema, dict) and isinstance(target_columns, list):
        missing_target_in_schema = [
            column for column in target_columns if column not in expected_schema
        ]
        if missing_target_in_schema:
            errors.append(
                "Target columns missing from expected_schema: "
                f"{sorted(missing_target_in_schema)}"
            )

    # -------------------------------------------------------------
    # Consumers
    # -------------------------------------------------------------
    consumers = contract.get("consumers", [])
    if not isinstance(consumers, list) or not consumers:
        errors.append("Contract must define at least one consumer.")
    else:
        for index, consumer in enumerate(consumers):
            if not isinstance(consumer, dict):
                errors.append(f"Consumer at index {index} must be a dict.")
                continue

            for field in CONSUMER_REQUIRED_FIELDS:
                if not consumer.get(field):
                    errors.append(
                        f"Consumer at index {index} missing required field: {field}"
                    )

    # -------------------------------------------------------------
    # Numeric governance fields
    # -------------------------------------------------------------
    min_expected_rows = contract.get("min_expected_rows")
    if not isinstance(min_expected_rows, int) or min_expected_rows < 0:
        errors.append("Contract min_expected_rows must be an integer >= 0.")

    freshness_max_age_hours = contract.get("freshness_max_age_hours")
    if not isinstance(freshness_max_age_hours, (int, float)) or freshness_max_age_hours <= 0:
        errors.append("Contract freshness_max_age_hours must be a positive number.")

    deprecation_timeline_days = contract.get("deprecation_timeline_days")
    if not isinstance(deprecation_timeline_days, int) or deprecation_timeline_days < 0:
        errors.append("Contract deprecation_timeline_days must be an integer >= 0.")

    violation_response_sla_minutes = contract.get("violation_response_sla_minutes")
    if (
        not isinstance(violation_response_sla_minutes, int)
        or violation_response_sla_minutes <= 0
    ):
        errors.append("Contract violation_response_sla_minutes must be a positive integer.")

    pipeline_lock_key = contract.get("pipeline_lock_key")
    if not isinstance(pipeline_lock_key, int) or pipeline_lock_key <= 0:
        errors.append("Contract pipeline_lock_key must be a positive integer.")

    return errors


# =================================================================
# RUNTIME SCHEMA VALIDATION / COMPATIBILITY CLASSIFICATION
# =================================================================

def classify_schema_change(
    actual_schema: Dict[str, str],
    contract: Dict[str, Any],
) -> Tuple[List[str], List[str], str]:
    """
    So sánh schema thực tế của source với contract.

    Trả về:
      errors: danh sách lỗi breaking
      warnings: danh sách cảnh báo additive/deprecated/unknown
      compatibility: 'breaking' hoặc 'backward_compatible'
    """

    errors: List[str] = []
    warnings: List[str] = []

    actual = {
        str(column).strip(): str(sql_type).strip()
        for column, sql_type in actual_schema.items()
    }

    expected_schema = contract.get("expected_schema", {})
    required_columns = contract.get("required_columns", [])
    deprecated_columns = contract.get("deprecated_columns", [])

    # -------------------------------------------------------------
    # 1. Kiểm tra thiếu cột bắt buộc
    # -------------------------------------------------------------
    missing_required = [
        column for column in required_columns if column not in actual
    ]

    if missing_required:
        errors.append(
            f"Missing required contracted columns: {sorted(missing_required)}."
        )

    # -------------------------------------------------------------
    # 2. Kiểm tra kiểu dữ liệu
    # -------------------------------------------------------------
    for column, expected_type in expected_schema.items():
        if column not in actual:
            if column not in required_columns:
                warnings.append(
                    f"Optional contracted column '{column}' is missing from source."
                )
            continue

        if not is_type_compatible(expected_type, actual[column]):
            errors.append(
                f"Column '{column}' type mismatch. "
                f"Expected compatible with '{expected_type}', "
                f"actual '{actual[column]}'."
            )

    # -------------------------------------------------------------
    # 3. Cột mới chưa khai báo trong contract
    # -------------------------------------------------------------
    unknown_columns = sorted([
        column for column in actual if column not in expected_schema
    ])

    if unknown_columns:
        warnings.append(
            "Source has columns not declared in contract: "
            f"{unknown_columns}. If these columns will be consumed, "
            "add them via the contract change process."
        )

    # -------------------------------------------------------------
    # 4. Deprecated columns
    # -------------------------------------------------------------
    for column in deprecated_columns:
        if column in actual:
            warnings.append(
                f"Column '{column}' is deprecated in contract. "
                "Plan removal within the deprecation timeline."
            )

    compatibility = "breaking" if errors else "backward_compatible"

    return errors, warnings, compatibility