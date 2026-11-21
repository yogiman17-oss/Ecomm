"""Schema-contract tests for the synthetic ecommerce dataset."""
import pytest

from ecommerce_lib import synthetic
from ecommerce_lib.schemas import RAW_SCHEMAS


@pytest.mark.parametrize("entity", ["customers", "products", "orders"])
def test_schema_matches_contract(raw, entity):
    assert raw[entity].schema == RAW_SCHEMAS[entity]


@pytest.mark.parametrize(
    "entity,expected_columns",
    [
        ("customers", ["customer_id", "first_name", "last_name", "email", "city", "state", "loyalty_tier", "signup_date"]),
        ("products", ["product_id", "product_name", "category", "brand", "unit_price"]),
        ("orders", ["order_id", "customer_id", "product_id", "order_ts", "quantity", "unit_price", "status", "payment_method"]),
    ],
)
def test_column_names_and_order(raw, entity, expected_columns):
    assert raw[entity].columns == expected_columns


def test_column_types(raw):
    dtypes = {e: dict(raw[e].dtypes) for e in raw}
    assert dtypes["customers"]["signup_date"] == "date"
    assert dtypes["products"]["unit_price"] == "double"
    assert dtypes["orders"]["order_ts"] == "timestamp"
    assert dtypes["orders"]["quantity"] == "int"


def test_raw_row_counts_include_injected_bad_rows(raw):
    assert raw["customers"].count() == synthetic.EXPECTED_COUNTS["bronze_customers"]
    assert raw["products"].count() == synthetic.EXPECTED_COUNTS["bronze_products"]
    assert raw["orders"].count() == synthetic.EXPECTED_COUNTS["bronze_orders"]


def test_key_formats(raw):
    assert raw["customers"].filter("customer_id NOT RLIKE '^C[0-9]{3}$'").count() == 0
    assert raw["products"].filter("product_id NOT RLIKE '^P[0-9]{3}$'").count() == 0
    assert raw["orders"].filter("order_id NOT RLIKE '^O[0-9]{4}$'").count() == 0


def test_generator_is_deterministic():
    assert synthetic.order_rows() == synthetic.order_rows()
    assert synthetic.customer_rows(seed=1) != synthetic.customer_rows(seed=2)
