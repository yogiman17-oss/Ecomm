from pyspark import pipelines as dp

from ecommerce_lib.transforms import category_sales


@dp.materialized_view(
    name="gold_category_sales",
    comment="Orders, units and revenue per product category.",
)
def gold_category_sales():
    return category_sales(
        spark.read.table("silver_orders"), spark.read.table("silver_products")
    )
