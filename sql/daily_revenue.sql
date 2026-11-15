-- Revenue by day over the orders table.
-- Tombstoned orders are excluded and only delivered or shipped
-- orders count toward revenue.
select
    event_date as day,
    count(*) as order_count,
    sum(total_cents) / 100.0 as revenue_dollars
from hudi.default.orders
where not deleted
    and status in ('shipped', 'delivered')
group by event_date
order by event_date;
