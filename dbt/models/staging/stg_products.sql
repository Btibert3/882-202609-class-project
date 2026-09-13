with source as (
    select * from {{ source('autoelite_raw', 'products') }}
),

renamed as (
    select
        id          as product_id,
        name        as product_name,
        description as product_description,
        is_active,
        external_id,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
