from pyspark import pipelines as dp

from ecommerce_lib.transforms import daily_revenue


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Daily orders, units and revenue (PLACED / SHIPPED / DELIVERED orders only).",
)
def gold_daily_revenue():
    return daily_revenue(spark.read.table("silver_orders"))
