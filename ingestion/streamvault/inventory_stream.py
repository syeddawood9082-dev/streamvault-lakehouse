"""Spark Structured Streaming job that lands inventory changes in Hudi.

Mirrors the orders job with the choices that fit a small hot table.
The inventory table is copy on write, so Trino reads stay cheap and
the stock queries in the sql folder run against plain parquet files.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streamvault.transforms import dedupe_latest, to_inventory_row

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
INVENTORY_TOPIC = "streamvault.public.inventory"
TABLE_PATH = os.environ.get("INVENTORY_TABLE_PATH", "s3a://lakehouse/inventory")
CHECKPOINT_PATH = os.environ.get(
    "INVENTORY_CHECKPOINT", "s3a://lakehouse/checkpoints/inventory"
)

HUDI_OPTIONS = {
    "hoodie.table.name": "inventory",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "sku,warehouse",
    "hoodie.datasource.write.precombine.field": "source_ts_ms",
    "hoodie.datasource.write.partitionpath.field": "warehouse",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.hive_sync.enable": "false",
}


def build_spark():
    return (
        SparkSession.builder.appName("streamvault-inventory-ingest")
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .getOrCreate()
    )


def parse_batch(batch_df, batch_id):
    raw_values = [row.value for row in batch_df.select("value").collect()]
    events = [json.loads(value) for value in raw_values]
    rows = dedupe_latest(
        [to_inventory_row(event) for event in events], ("sku", "warehouse")
    )
    if not rows:
        return
    spark = batch_df.sparkSession
    frame = spark.createDataFrame(rows)
    frame.write.format("hudi").options(**HUDI_OPTIONS).mode("append").save(TABLE_PATH)


def main():
    spark = build_spark()
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INVENTORY_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast("string").alias("value"))
    )
    query = (
        stream.writeStream.foreachBatch(parse_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
