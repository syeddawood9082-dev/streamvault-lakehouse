# Ingestion

Two Spark Structured Streaming jobs live here. One lands order changes. One lands inventory changes. Both read Debezium topics from Kafka, run each event through the shared transforms in `streamvault/transforms.py`, dedupe the micro batch down to the newest version of each row, and upsert into Hudi.

Submit the orders job from the repo root once the stack is up and the connectors are registered.

```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.14.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  ingestion/streamvault/orders_stream.py
```

Swap the script name to run the inventory job. The jobs read their Kafka and storage settings from environment variables with local defaults, so the same code runs against the compose stack with no edits.

The transform module stays free of Spark imports on purpose. The pytest suite exercises the parsing and dedupe rules directly, which keeps the feedback loop fast when the event shape changes.
