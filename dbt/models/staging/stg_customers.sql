with source as (
    select * from {{ source('autoelite_raw', 'customers') }}
),

renamed as (
    select
        id          as customer_id,
        first_name,
        last_name,
        phone,
        description,
        shipping_state as state,
        person_email   as email,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
