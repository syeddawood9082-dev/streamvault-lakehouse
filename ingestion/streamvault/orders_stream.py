"""Spark Structured Streaming job that lands order changes in Hudi.

Reads the Debezium topic for the orders table, parses each change
event with the shared transforms, and upserts into the orders Hudi
table on object storage. Run it with spark-submit as shown in the
ingestion README.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from streamvault.transforms import dedupe_latest, to_order_row

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
ORDERS_TOPIC = "streamvault.public.orders"
TABLE_PATH = os.environ.get("ORDERS_TABLE_PATH", "s3a://lakehouse/orders")
CHECKPOINT_PATH = os.environ.get("ORDERS_CHECKPOINT", "s3a://lakehouse/checkpoints/orders")

HUDI_OPTIONS = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "source_ts_ms",
    "hoodie.datasource.write.partitionpath.field": "event_date",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.hive_sync.enable": "false",
}


def build_spark():
    return (
        SparkSession.builder.appName("streamvault-orders-ingest")
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
    """Apply one micro batch of raw Kafka values to the Hudi table."""
    raw_values = [row.value for row in batch_df.select("value").collect()]
    events = [json.loads(value) for value in raw_values]
    rows = dedupe_latest([to_order_row(event) for event in events], ("order_id",))
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
        .option("subscribe", ORDERS_TOPIC)
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
