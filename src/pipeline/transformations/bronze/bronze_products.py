from pyspark import pipelines as dp
from pyspark.sql import functions as F

from ecommerce_lib.schemas import PRODUCTS_SCHEMA


@dp.materialized_view(
    name="bronze_products",
    comment="Raw product catalog loaded as-is from JSON files in the raw_data volume.",
)
def bronze_products():
    raw_path = spark.conf.get("raw_data_path")
    return (
        spark.read.format("json")
        .schema(PRODUCTS_SCHEMA)
        .load(f"{raw_path}/products")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
