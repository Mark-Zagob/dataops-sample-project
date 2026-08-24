# user_code/contracts/registry.py

"""
=================================================================
DATA CONTRACT REGISTRY
=================================================================

Registry này là danh sách tập trung các data contract đang được
quản lý như sản phẩm nội bộ.

Ở phase hiện tại, registry nằm trong code.
Ở phase sau, nó có thể được đồng bộ ra:

- Data Catalog
- PostgreSQL metadata table
- Dagster metadata
- Slack/Confluence documentation
- API contract store
"""

from typing import Any, Dict, List

from user_code.contracts.order_contract import (
    ORDER_CONTRACT,
    validate_contract_definition,
)


CONTRACT_REGISTRY: Dict[str, Dict[str, Any]] = {
    ORDER_CONTRACT["contract_id"]: ORDER_CONTRACT,
}


def get_contract(contract_id: str) -> Dict[str, Any]:
    """
    Lấy một contract theo contract_id.
    """
    return CONTRACT_REGISTRY.get(contract_id)


def list_contracts() -> List[Dict[str, Any]]:
    """
    Trả về danh sách tất cả contracts đã đăng ký.
    """
    return list(CONTRACT_REGISTRY.values())


def validate_registry() -> List[str]:
    """
    Kiểm tra tính hợp lệ của toàn bộ registry.

    Kiểm tra:
    - Contract ID không trùng.
    - Contract ID trong registry trùng với contract_id bên trong contract.
    - Mỗi contract có đủ metadata sản phẩm.
    """

    errors: List[str] = []
    seen_contract_ids = set()

    for registry_contract_id, contract in CONTRACT_REGISTRY.items():
        contract_id = contract.get("contract_id")

        if registry_contract_id != contract_id:
            errors.append(
                "Registry key does not match contract_id: "
                f"registry_key='{registry_contract_id}', contract_id='{contract_id}'."
            )

        if contract_id in seen_contract_ids:
            errors.append(f"Duplicate contract_id detected: '{contract_id}'.")

        seen_contract_ids.add(contract_id)

        errors.extend(validate_contract_definition(contract))

    return errors