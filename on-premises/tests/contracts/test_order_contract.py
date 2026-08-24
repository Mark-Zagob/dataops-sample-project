# tests/contracts/test_order_contract.py

import unittest

from user_code.contracts.order_contract import (
    ORDER_CONTRACT,
    classify_schema_change,
    validate_contract_definition,
)

from user_code.contracts.registry import validate_registry


class TestOrderContract(unittest.TestCase):
    def setUp(self):
        self.valid_schema = {
            "order_id": "int",
            "customer_id": "int",
            "amount": "decimal(10,2)",
            "status": "varchar(50)",
            "created_at": "datetime",
        }

    # -------------------------------------------------------------
    # Contract metadata tests
    # -------------------------------------------------------------

    def test_contract_definition_is_valid(self):
        errors = validate_contract_definition(ORDER_CONTRACT)
        self.assertEqual(errors, [])

    def test_registry_is_valid(self):
        errors = validate_registry()
        self.assertEqual(errors, [])

    # -------------------------------------------------------------
    # Happy path schema tests
    # -------------------------------------------------------------

    def test_exact_schema_passes(self):
        errors, warnings, compatibility = classify_schema_change(
            self.valid_schema,
            ORDER_CONTRACT,
        )

        self.assertEqual(errors, [])
        self.assertEqual(compatibility, "backward_compatible")

    # -------------------------------------------------------------
    # Breaking changes
    # -------------------------------------------------------------

    def test_missing_required_column_fails(self):
        actual = self.valid_schema.copy()
        del actual["amount"]

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertTrue(errors)
        self.assertEqual(compatibility, "breaking")

    def test_type_change_fails(self):
        actual = self.valid_schema.copy()
        actual["amount"] = "varchar(50)"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertTrue(errors)
        self.assertEqual(compatibility, "breaking")

    def test_varchar_length_decrease_fails(self):
        actual = self.valid_schema.copy()
        actual["status"] = "varchar(20)"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertTrue(errors)
        self.assertEqual(compatibility, "breaking")

    def test_decimal_scale_change_fails(self):
        actual = self.valid_schema.copy()
        actual["amount"] = "decimal(10,3)"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertTrue(errors)
        self.assertEqual(compatibility, "breaking")

    # -------------------------------------------------------------
    # Backward-compatible changes
    # -------------------------------------------------------------

    def test_new_unknown_column_warns_but_does_not_fail(self):
        actual = self.valid_schema.copy()
        actual["updated_at"] = "datetime"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertEqual(errors, [])
        self.assertTrue(any("updated_at" in warning for warning in warnings))
        self.assertEqual(compatibility, "backward_compatible")

    def test_varchar_length_increase_passes(self):
        actual = self.valid_schema.copy()
        actual["status"] = "varchar(100)"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertEqual(errors, [])
        self.assertEqual(compatibility, "backward_compatible")

    def test_decimal_precision_increase_same_scale_passes(self):
        actual = self.valid_schema.copy()
        actual["amount"] = "decimal(12,2)"

        errors, warnings, compatibility = classify_schema_change(
            actual,
            ORDER_CONTRACT,
        )

        self.assertEqual(errors, [])
        self.assertEqual(compatibility, "backward_compatible")


if __name__ == "__main__":
    unittest.main()