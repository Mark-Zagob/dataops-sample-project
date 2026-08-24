# user_code/assets/contract_registry.py

"""
=================================================================
DATA CONTRACT REGISTRY ASSET
=================================================================

Asset này hiện thực hóa quan điểm:

    Data Contract là một sản phẩm nội bộ.

Nó không chạy ETL. Nó kiểm tra:

- Contract có đủ owner, consumer, version hay không.
- Contract có policy rõ ràng hay không.
- Registry có bị trùng contract_id hay không.

Kết quả được hiển thị trong Dagster UI để Data Engineer,
Platform Engineer và SRE có thể quan sát trạng thái governance.
"""

from typing import Any, Dict

from dagster import AssetExecutionContext, MetadataValue, asset

from user_code.contracts.registry import (
    CONTRACT_REGISTRY,
    validate_registry,
)


@asset(
    name="contract_registry",
    description=(
        "Catalog các data contract đang được quản lý như sản phẩm nội bộ. "
        "Asset này kiểm tra ownership, versioning, consumer, compatibility policy "
        "và tính hợp lệ của registry."
    ),
    group_name="data_contracts",
    tags={
        "stage": "contract",
        "pipeline": "contracts",
    },
)
def contract_registry(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Validate contract registry và expose metadata catalog.
    """

    errors = validate_registry()

    if errors:
        error_message = (
            "❌ Contract registry validation failed: "
            + "; ".join(errors)
        )
        context.log.error(error_message)
        raise ValueError(error_message)

    catalog = []

    for contract in CONTRACT_REGISTRY.values():
        catalog.append(
            {
                "contract_id": contract["contract_id"],
                "version": contract["version"],
                "status": contract["status"],
                "domain": contract["domain"],
                "dataset": contract["dataset"],
                "source_system": contract["source_system"],
                "source_table": contract["source_table"],
                "production_table": contract["production_table"],
                "owner_team": contract["owner_team"],
                "owner_contact": contract["owner_contact"],
                "producer_team": contract["producer_team"],
                "producer_contact": contract["producer_contact"],
                "consumer_count": len(contract.get("consumers", [])),
                "compatibility_policy": contract["compatibility_policy"],
                "deprecation_timeline_days": contract["deprecation_timeline_days"],
                "violation_response_sla_minutes": contract["violation_response_sla_minutes"],
                "alert_channels": contract["alert_channels"],
            }
        )

    context.log.info(
        "✅ Contract registry is valid. Registered contracts: %s",
        len(catalog),
    )

    context.add_output_metadata(
        {
            "contract_count": MetadataValue.text(str(len(catalog))),
            "catalog": MetadataValue.json(catalog),
            "registry_policy": MetadataValue.text(
                "Data contracts must have owner, producer, consumers, version, "
                "compatibility policy, deprecation timeline, review process, "
                "and alert channels."
            ),
        }
    )

    return {
        "status": "valid",
        "contract_count": len(catalog),
    }