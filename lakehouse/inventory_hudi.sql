create table if not exists inventory (
    sku varchar,
    warehouse varchar,
    on_hand integer,
    reserved integer,
    updated_at varchar,
    deleted boolean,
    source_ts_ms bigint
)
with (
    type = 'cow',
    primary_key = 'sku,warehouse',
    precombine_field = 'source_ts_ms',
    partitioning = 'warehouse'
);
