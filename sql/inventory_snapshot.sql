-- End of day stock positions frozen into a snapshot table.
-- The inventory table only holds the newest row for each sku and
-- warehouse pair, so the position from a finished day is gone once
-- the next day of changes lands. Run the insert once at the end of
-- each day and the snapshot table keeps that day ready to read back.
create table if not exists inventory_snapshot_daily (
    snapshot_date varchar,
    sku varchar,
    warehouse varchar,
    on_hand integer,
    reserved integer,
    available integer,
    updated_at varchar
)
with (
    type = 'cow',
    partitioning = 'snapshot_date'
);

insert into inventory_snapshot_daily
select
    cast(current_date as varchar) as snapshot_date,
    sku,
    warehouse,
    on_hand,
    reserved,
    greatest(on_hand - reserved, 0) as available,
    updated_at
from hudi.default.inventory
where not deleted;

-- Read back the positions for one finished day.
select
    warehouse,
    sku,
    on_hand,
    reserved,
    available
from inventory_snapshot_daily
where snapshot_date = '2026-11-08'
order by warehouse, sku;
