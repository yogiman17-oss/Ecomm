from pyspark import pipelines as dp

from ecommerce_lib.transforms import customer_lifetime_value


@dp.materialized_view(
    name="gold_customer_lifetime_value",
    comment="Lifetime orders and revenue per customer; customers without orders have 0.",
)
def gold_customer_lifetime_value():
    return customer_lifetime_value(
        spark.read.table("silver_customers"), spark.read.table("silver_orders")
    )
