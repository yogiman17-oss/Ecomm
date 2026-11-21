"""Schema contracts for the raw synthetic files and the expected columns of every pipeline dataset."""
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("signup_date", DateType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_ts", TimestampType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
])

RAW_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
}

_INGEST_COLS = ["_source_file", "_ingested_at"]

# Expected column order of every dataset the pipeline publishes (used by integration tests).
LAYER_COLUMNS = {
    "bronze_customers": CUSTOMERS_SCHEMA.fieldNames() + _INGEST_COLS,
    "bronze_products": PRODUCTS_SCHEMA.fieldNames() + _INGEST_COLS,
    "bronze_orders": ORDERS_SCHEMA.fieldNames() + _INGEST_COLS,
    "silver_customers": CUSTOMERS_SCHEMA.fieldNames(),
    "silver_products": PRODUCTS_SCHEMA.fieldNames(),
    "silver_orders": [
        "order_id", "customer_id", "product_id", "order_ts", "order_date",
        "quantity", "unit_price", "line_amount", "status", "payment_method",
    ],
    "gold_daily_revenue": ["order_date", "order_count", "units_sold", "revenue"],
    "gold_customer_lifetime_value": [
        "customer_id", "first_name", "last_name", "loyalty_tier",
        "total_orders", "lifetime_revenue", "first_order_date", "last_order_date",
    ],
    "gold_category_sales": ["category", "order_count", "units_sold", "revenue"],
}
