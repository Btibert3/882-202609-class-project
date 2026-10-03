-- Fact table: one row per order line item.
-- Joins the enriched line items with dimension keys for reporting.

with order_items as (
    select * from {{ ref('int_order_items') }}
),

reps as (
    select rep_id, first_name, last_name from {{ ref('stg_reps') }}
),

products as (
    select product_id, product_name from {{ ref('stg_products') }}
),

customers as (
    select customer_id, first_name, last_name, state from {{ ref('stg_customers') }}
),

final as (
    select
        order_items.order_item_id,
        order_items.order_id,
        order_items.order_date,
        order_items.order_status,

        order_items.customer_id,
        customers.first_name  as customer_first_name,
        customers.last_name   as customer_last_name,
        customers.state       as customer_state,

        order_items.rep_id,
        reps.first_name       as rep_first_name,
        reps.last_name        as rep_last_name,

        order_items.product_id,
        products.product_name,

        order_items.quantity,
        order_items.unit_price,
        order_items.line_total

    from order_items
    left join reps      using (rep_id)
    left join products  using (product_id)
    left join customers using (customer_id)
)

select * from final
