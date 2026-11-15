-- Orders that have sat in one status for more than two days.
-- Useful as a morning check for a fulfilment team.
select
    order_id,
    customer_id,
    status,
    placed_at,
    updated_at
from hudi.default.orders
where not deleted
    and status in ('placed', 'paid')
    and date_diff('day', from_iso8601_timestamp(updated_at), current_timestamp) > 2
order by updated_at;
