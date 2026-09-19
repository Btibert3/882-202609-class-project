with source as (
    select * from {{ source('autoelite_raw', 'order_items') }}
),

renamed as (
    select
        id                 as order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        pricebook_entry_id,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
