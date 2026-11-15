-- Source schema for the local Postgres container.
-- The streamvault role needs replication rights for Debezium, which
-- the compose setup grants through the logical WAL settings.

create table if not exists orders (
    order_id bigint primary key,
    customer_id bigint not null,
    status text not null,
    total_cents bigint not null,
    placed_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists inventory (
    sku text not null,
    warehouse text not null,
    on_hand integer not null default 0,
    reserved integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (sku, warehouse)
);

alter table orders replica identity full;
alter table inventory replica identity full;

create publication streamvault_orders_pub for table orders;
create publication streamvault_inventory_pub for table inventory;
