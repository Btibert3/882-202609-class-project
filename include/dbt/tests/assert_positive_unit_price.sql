-- Custom test: no order line item should have a negative unit price.
-- Zero is valid (e.g. financing add-ons).
-- dbt convention: this query returns rows that FAIL the test.
-- If zero rows are returned, the test passes.

select *
from {{ ref('stg_order_items') }}
where unit_price < 0
