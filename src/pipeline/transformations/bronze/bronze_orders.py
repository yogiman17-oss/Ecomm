from pyspark import pipelines as dp
from pyspark.sql import functions as F

from ecommerce_lib.schemas import ORDERS_SCHEMA


@dp.materialized_view(
    name="bronze_orders",
    comment="Raw orders loaded as-is from Parquet files in the raw_data volume.",
)
def bronze_orders():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.read.format("parquet")
        .schema(ORDERS_SCHEMA)
        .load(f"{raw_path}/orders")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
