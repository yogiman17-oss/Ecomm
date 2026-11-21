"""Unit tests for the silver / gold transformations and data-quality rules."""
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from ecommerce_lib import transforms as t
from ecommerce_lib.schemas import ORDERS_SCHEMA


def _orders(spark, rows):
    return t.enrich_orders(spark.createDataFrame(rows, ORDERS_SCHEMA))


# ---------- silver ----------
def test_customers_dedup_keeps_latest_and_standardizes(raw):
    out = t.standardize_customers(raw["customers"])
    assert out.count() == 51  # 53 raw rows, 2 duplicate ids collapsed
    c001 = out.filter("customer_id = 'C001'").collect()
    assert len(c001) == 1
    assert c001[0].loyalty_tier == "Platinum"  # newer duplicate wins
    assert c001[0].email == c001[0].email.lower().strip()
    assert c001[0].first_name == c001[0].first_name.strip()


def test_customer_rules_drop_null_email(raw):
    out = t.apply_rules(t.standardize_customers(raw["customers"]), t.CUSTOMER_RULES)
    assert out.count() == 50
    assert out.filter("customer_id = 'C051'").count() == 0


def test_product_rules_drop_negative_price_and_cast_decimal(raw):
    out = t.apply_rules(t.standardize_products(raw["products"]), t.PRODUCT_RULES)
    assert out.count() == 20
    assert dict(out.dtypes)["unit_price"] == "decimal(10,2)"
    assert out.filter("product_id = 'P021'").count() == 0


def test_enrich_orders_computes_line_amount_and_date(spark):
    out = _orders(spark, [("O1", "C001", "P001", datetime(2026, 2, 3, 23, 59), 3, 10.5, " shipped ", "UPI")]).first()
    assert out.line_amount == Decimal("31.50")
    assert out.order_date == date(2026, 2, 3)
    assert out.status == "SHIPPED"


def test_order_rules_drop_bad_rows(raw):
    out = t.apply_rules(t.enrich_orders(raw["orders"]), t.ORDER_RULES)
    assert out.count() == 300
    assert out.filter(F.col("order_id").isin("O0301", "O0302", "O0303")).count() == 0


def test_apply_rules_treats_null_as_violation(spark):
    df = _orders(spark, [("O1", "C001", "P001", datetime(2026, 1, 1), None, 1.0, "PLACED", "UPI")])
    assert t.apply_rules(df, t.ORDER_RULES).count() == 0


# ---------- gold ----------
def test_daily_revenue_excludes_cancelled_and_returned(spark):
    df = _orders(spark, [
        ("O1", "C001", "P001", datetime(2026, 1, 1, 9), 2, 10.0, "DELIVERED", "UPI"),
        ("O2", "C002", "P001", datetime(2026, 1, 1, 10), 1, 10.0, "CANCELLED", "UPI"),
        ("O3", "C002", "P001", datetime(2026, 1, 1, 11), 1, 10.0, "RETURNED", "UPI"),
        ("O4", "C003", "P002", datetime(2026, 1, 2, 11), 1, 5.0, "PLACED", "CARD"),
    ])
    rows = {r.order_date: r for r in t.daily_revenue(df).collect()}
    assert rows[date(2026, 1, 1)].revenue == Decimal("20.00")
    assert rows[date(2026, 1, 1)].order_count == 1
    assert rows[date(2026, 1, 2)].units_sold == 1


def test_clv_keeps_customers_without_orders(spark, raw):
    customers = t.apply_rules(t.standardize_customers(raw["customers"]), t.CUSTOMER_RULES)
    orders = _orders(spark, [("O1", "C001", "P001", datetime(2026, 1, 1), 2, 10.0, "DELIVERED", "UPI")])
    out = t.customer_lifetime_value(customers, orders)
    assert out.count() == 50
    assert out.filter("customer_id = 'C001'").first().lifetime_revenue == Decimal("20.00")
    assert out.filter("customer_id = 'C002'").first().total_orders == 0


def test_gold_views_reconcile_on_synthetic_data(raw):
    customers = t.apply_rules(t.standardize_customers(raw["customers"]), t.CUSTOMER_RULES)
    products = t.apply_rules(t.standardize_products(raw["products"]), t.PRODUCT_RULES)
    orders = t.apply_rules(t.enrich_orders(raw["orders"]), t.ORDER_RULES)
    total = lambda df, col: df.agg(F.sum(col)).first()[0]
    daily = total(t.daily_revenue(orders), "revenue")
    assert daily == total(t.category_sales(orders, products), "revenue")
    assert daily == total(t.customer_lifetime_value(customers, orders), "lifetime_revenue")
