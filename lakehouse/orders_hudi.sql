create table if not exists orders (
    order_id bigint,
    customer_id bigint,
    status varchar,
    total_cents bigint,
    placed_at varchar,
    updated_at varchar,
    deleted boolean,
    source_ts_ms bigint,
    event_date varchar
)
with (
    type = 'mor',
    primary_key = 'order_id',
    precombine_field = 'source_ts_ms',
    partitioning = 'event_date'
);
