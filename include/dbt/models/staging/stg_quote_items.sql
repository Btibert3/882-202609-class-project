with source as (
    select * from {{ source('autoelite_raw', 'quote_items') }}
),

renamed as (
    select
        id                      as quote_item_id,
        quote_id,
        product2_id             as product_id,
        pricebook_entry_id,
        quantity,
        unit_price,
        discount,
        total_price,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
