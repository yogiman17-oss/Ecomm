from pyspark import pipelines as dp

from ecommerce_lib.transforms import CUSTOMER_RULES, standardize_customers


@dp.materialized_view(
    name="silver_customers",
    comment="Cleaned, de-duplicated customers (latest record per customer_id); invalid rows dropped.",
)
@dp.expect_all_or_drop(CUSTOMER_RULES)
def silver_customers():
    return standardize_customers(spark.read.table("bronze_customers"))
