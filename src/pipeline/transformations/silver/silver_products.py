from pyspark import pipelines as dp

from ecommerce_lib.transforms import PRODUCT_RULES, standardize_products


@dp.materialized_view(
    name="silver_products",
    comment="Validated product catalog with decimal pricing; non-positive prices dropped.",
)
@dp.expect_all_or_drop(PRODUCT_RULES)
def silver_products():
    return standardize_products(spark.read.table("bronze_products"))
