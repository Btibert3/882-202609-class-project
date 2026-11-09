with deal_items as (
    select * from {{ ref('stg_deal_items') }}
)

select
    deal_id,
    count(*)                   as item_count,
    sum(total_price)           as total_deal_value,
    avg(total_price)           as avg_item_value,
    count(distinct product_id) as unique_products
from deal_items
group by deal_id
