-- Dimension table: one row per customer.

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    state

from {{ ref('stg_customers') }}
