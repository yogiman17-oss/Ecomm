"""Pure DataFrame transformations and data-quality rules shared by the pipeline and the unit tests."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA

VALID_STATUSES = ("PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
REVENUE_STATUSES = ("PLACED", "SHIPPED", "DELIVERED")

# Silver expectations (expect_all_or_drop). Keys are expectation names.
CUSTOMER_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email": "email IS NOT NULL AND email LIKE '%@%.%'",
}
PRODUCT_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "positive_price": "unit_price > 0",
}
ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "valid_status": "status IN ('" + "','".join(VALID_STATUSES) + "')",
}


def apply_rules(df: DataFrame, rules: dict) -> DataFrame:
    """Keep only rows that pass every rule (same semantics as expect_all_or_drop)."""
    return df.filter(" AND ".join(f"coalesce(({r}), false)" for r in rules.values()))


# ---------- silver ----------
def standardize_customers(df: DataFrame) -> DataFrame:
    """Trim / case names, lower-case email, keep the latest record per customer_id."""
    latest = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc())
    return (
        df.withColumn("first_name", F.initcap(F.trim("first_name")))
        .withColumn("last_name", F.initcap(F.trim("last_name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .select(*CUSTOMERS_SCHEMA.fieldNames())
    )


def standardize_products(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("category", F.initcap(F.trim("category")))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .select(*PRODUCTS_SCHEMA.fieldNames())
    )


def enrich_orders(df: DataFrame) -> DataFrame:
    price = F.col("unit_price").cast("decimal(10,2)")
    return df.select(
        "order_id", "customer_id", "product_id", "order_ts",
        F.to_date("order_ts").alias("order_date"),
        "quantity",
        price.alias("unit_price"),
        (F.col("quantity") * price).cast("decimal(12,2)").alias("line_amount"),
        F.upper(F.trim("status")).alias("status"),
        "payment_method",
    )


# ---------- gold ----------
def _revenue_orders(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("status").isin(*REVENUE_STATUSES))


def _sales_aggs():
    return [
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").cast("bigint").alias("units_sold"),
        F.sum("line_amount").cast("decimal(18,2)").alias("revenue"),
    ]


def daily_revenue(orders: DataFrame) -> DataFrame:
    return _revenue_orders(orders).groupBy("order_date").agg(*_sales_aggs())


def category_sales(orders: DataFrame, products: DataFrame) -> DataFrame:
    return (
        _revenue_orders(orders)
        .join(products.select("product_id", "category"), "product_id", "inner")
        .groupBy("category")
        .agg(*_sales_aggs())
    )


def customer_lifetime_value(customers: DataFrame, orders: DataFrame) -> DataFrame:
    per_customer = _revenue_orders(orders).groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("line_amount").cast("decimal(18,2)").alias("lifetime_revenue"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    return (
        customers.select("customer_id", "first_name", "last_name", "loyalty_tier")
        .join(per_customer, "customer_id", "left")
        .select(
            "customer_id", "first_name", "last_name", "loyalty_tier",
            F.coalesce("total_orders", F.lit(0).cast("bigint")).alias("total_orders"),
            F.coalesce("lifetime_revenue", F.lit(0).cast("decimal(18,2)")).alias("lifetime_revenue"),
            "first_order_date", "last_order_date",
        )
    )
