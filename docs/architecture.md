# Architecture

Data moves in one direction through this project. Postgres is the source of truth. Everything downstream of it can be rebuilt by replaying the Kafka topics.

## The flow

A write lands in Postgres. The logical decoding slot picks it up because both source tables publish with replica identity full, which means update and delete events carry the full before image. Debezium turns each change into an event on its topic. The event keeps the before and after images plus the source timestamp, and that timestamp becomes the ordering rule for the whole pipeline.

The Spark jobs read the topics in micro batches. Each batch goes through the shared transforms, which shape events into table rows and collapse repeated versions of a row down to the newest one. The jobs then upsert into Hudi. Hudi applies the precombine rule again at write time, so a replayed batch cannot resurrect an older row over a newer one.

Trino reads through the Hudi connector against the same object storage. Readers see committed table state only. A streaming batch that is mid write is invisible until its commit lands.

## Failure handling

If a Spark job dies, it restarts from its checkpoint and replays the unprocessed offsets. Upserts make the replay safe. If Debezium falls behind, the slot retains WAL on the Postgres side until Connect catches up, and the retention is the thing to watch on a busy source.

Poison events are the awkward case. An event that fails parsing stops the batch rather than slipping past, because a silently skipped order is worse than a loud pipeline. The fix is to correct the transform, add the failing shape to the test suite, and restart.
