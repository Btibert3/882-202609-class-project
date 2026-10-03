-- Dimension table: one row per product.

select
    product_id,
    product_name,
    product_description,
    is_active

from {{ ref('stg_products') }}
