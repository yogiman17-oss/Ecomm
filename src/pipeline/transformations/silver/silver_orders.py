from pyspark import pipelines as dp

from ecommerce_lib.transforms import ORDER_RULES, enrich_orders


@dp.materialized_view(
    name="silver_orders",
    comment="Validated orders with order_date and line_amount; invalid rows dropped.",
)
@dp.expect_all_or_drop(ORDER_RULES)
def silver_orders():
    return enrich_orders(spark.read.table("bronze_orders"))
