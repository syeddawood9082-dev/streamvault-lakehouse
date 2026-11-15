# Connector offset notes

These notes cover what to do when a Debezium connector needs its position reset. They apply to the two connectors in this folder, one for orders and one for inventory. Each connector owns its own replication slot and its own publication on the Postgres side, and each one tracks its own position in the write ahead log.

## Where the position lives

A connector position has two halves. Kafka Connect stores the offset the connector last confirmed, and Postgres holds the replication slot whose confirmed flush position says how far the database can stop keeping log segments. Resetting only one half leaves the two sides disagreeing, and the connector will either reread a stretch of the log or fail to start.

The orders connector uses the slot named streamvault_orders_slot and the publication named streamvault_orders_pub. The inventory connector carries the matching inventory names in its own definition file. Nothing is shared between the two, so one connector can be reset while the other keeps streaming.

## When a reset is the right move

- The slot fell so far behind that the retained log is larger than the database can hold
- The connector task failed repeatedly and the stored offset no longer matches the slot
- A local rebuild is starting from an empty lakehouse and a fresh snapshot is the honest answer

A reset is not the fix for a connector that is merely paused or a Connect worker that restarted. Those cases resume from the stored offset on their own.

## The reset steps, in order

1. Stop the connector through the Connect REST API and wait until the task state reads paused or stopped.
2. Delete the connector. Deleting the connector leaves the slot behind on purpose, so the next steps can see it.
3. In Postgres, check the slot state with a query on pg_replication_slots. Drop the slot only after the connector is gone, and only for the table pair being reset.
4. Delete the stored offset for that connector from the Connect offsets topic if the cluster keeps one, so no stale position survives.
5. Re-register the connector with the register script in this folder. The snapshot mode in the definition reads the table once and then follows the log from the new slot position.
6. Watch the first snapshot finish, then change one row in the source table and confirm the change lands in the lakehouse table before calling the reset done.

## What not to do

- Never drop a replication slot while its connector is still running. The connector fails at once and the position is lost with no record of where it was.
- Never edit a connector definition to point at the other table slot. The two streams look alike and the mix up is silent until rows land in the wrong table.
- Never reset both connectors to fix a problem in one. A fresh snapshot of a healthy stream costs time and buys nothing.

## How to tell it worked

The connector status reads running with no task errors. The slot confirmed flush position advances as new changes land. A test row changed in Postgres shows up in the matching lakehouse table within the usual streaming delay, and the row counts from the source and the lakehouse agree for the snapshot window.
