-- Join orders and order_items so each line item carries order-level context.
-- This is the grain that downstream marts are built on.

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

joined as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.product_id,
        order_items.quantity,
        order_items.unit_price,
        order_items.quantity * order_items.unit_price as line_total,

        orders.customer_id,
        orders.rep_id,
        orders.order_status,
        orders.order_date

    from order_items
    left join orders using (order_id)
)

select * from joined
