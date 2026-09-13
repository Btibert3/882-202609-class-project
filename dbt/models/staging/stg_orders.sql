with source as (
    select * from {{ source('autoelite_raw', 'orders') }}
),

renamed as (
    select
        id             as order_id,
        account_id     as customer_id,
        owner_id       as rep_id,
        opportunity_id,
        pricebook_id,
        status,
        effective_date as order_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
