# user_code/definitions.py

"""
=================================================================
DAGSTER DEFINITIONS - Entry Point
=================================================================

File này tổng hợp tất cả Data Assets, Jobs, Schedules, Sensors
thành một đối tượng Definitions duy nhất để Dagster load.

Đây là nơi duy nhất mà workspace.yaml trỏ đến.
"""

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
)

from user_code.assets.orders import (
    validate_orders_schema,
    orders_staging,
    orders_production,
)

from user_code.assets.contract_registry import contract_registry


# =================================================================
# ORDERS PIPELINE JOB
# =================================================================

orders_pipeline_job = define_asset_job(
    name="orders_pipeline_job",
    selection=AssetSelection.groups("orders_pipeline"),
    tags={
        "pipeline": "orders",
    },
)


# =================================================================
# CONTRACT GOVERNANCE JOB
# =================================================================

contract_governance_job = define_asset_job(
    name="contract_governance_job",
    selection=AssetSelection.groups("data_contracts"),
    tags={
        "pipeline": "contracts",
    },
)


# =================================================================
# DEFINITIONS
# =================================================================

defs = Definitions(
    assets=[
        contract_registry,
        validate_orders_schema,
        orders_staging,
        orders_production,
    ],
    jobs=[
        orders_pipeline_job,
        contract_governance_job,
    ],
)