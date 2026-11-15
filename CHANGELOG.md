# Changelog

All notable changes to this project are recorded here. The format follows the spirit of Keep a Changelog and the versions follow semantic versioning.

## Unreleased

- Inventory snapshot query added to the sql folder
- Compaction schedule for the orders table added to the lakehouse folder
- Connector offset notes added to the cdc folder
- Notes on the merge on read and copy on write split in the lakehouse folder

## 0.1.0, 2026-11-08

- First working pipeline. Debezium streams orders and inventory changes from Postgres into Kafka.
- Spark Structured Streaming job applies change events to Apache Hudi tables on MinIO.
- Trino queries for daily revenue, stock levels, and stale orders.
- Pytest suite covering envelope parsing, row transforms, and dedupe.
- Docker compose stack for the full local run.
