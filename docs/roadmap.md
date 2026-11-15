# Roadmap

Work is grouped by the part of the pipeline it touches. Items move to the changelog when they ship.

## Now

- Publish this scaffold to the live repo so every item below can ship in the open instead of waiting in a local copy
- Inventory snapshot query for end of day stock positions
- A compaction schedule for the orders table so merge on read file sizes stay sane
- Connector offset notes in the cdc folder for anyone resetting a slot

## Next

- A second source table pair for shipments, following the orders pattern
- Schema evolution notes covering what happens when the source adds a column
- A small data quality check job that compares source counts against lakehouse counts per day

## Later

- Time travel examples that show how Hudi snapshots answer what the table looked like last week
- A note on moving the same layout from MinIO to S3 with no code changes
- Retention settings for old file versions once the table has real history

## Not planned

- A web UI. The project stays a set of files and jobs on purpose.
- Exactly once claims. The pipeline is at least once with idempotent writes, and the docs say so plainly.
