-- Current sellable stock by warehouse and sku.
-- Available units are on hand minus reserved, floored at zero.
select
    warehouse,
    sku,
    on_hand,
    reserved,
    greatest(on_hand - reserved, 0) as available
from hudi.default.inventory
where not deleted
order by warehouse, sku;
