# Lakehouse notes

Two tables live here. Both are fed by the streaming jobs in the ingestion folder and read through the Trino hudi catalog.

Orders is merge on read. Order rows churn. A placed order moves through payment, picking, shipping, and delivery, and each step rewrites the row. Merge on read keeps those writes fast and compacts in the background. The table partitions by event date because nearly every question about orders starts with a day range.

Inventory is copy on write. The table is small and the stock questions are constant, so paying the rewrite cost on write buys simple fast reads.

Both tables take the source update timestamp as the precombine field. When two versions of a row race, the one with the higher timestamp survives. That rule matches how the source system orders its own writes, and it makes replays safe. Replaying a topic from the start rebuilds the same table state.

Deletes land as tombstone rows with the deleted flag set. Trino queries filter on that flag. Dropping deletes at ingest would be simpler, and it would also erase the only record that a removal happened.

Compaction for the orders table runs on a set schedule so its delta logs never grow unchecked. The schedule and the writer settings live in compaction.md and compaction.properties in this folder.
