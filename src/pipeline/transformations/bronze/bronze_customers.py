from pyspark import pipelines as dp
from pyspark.sql import functions as F

from ecommerce_lib.schemas import CUSTOMERS_SCHEMA


@dp.materialized_view(
    name="bronze_customers",
    comment="Raw customers loaded as-is from CSV files in the raw_data volume.",
)
def bronze_customers():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(CUSTOMERS_SCHEMA)
        .load(f"{raw_path}/customers")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
