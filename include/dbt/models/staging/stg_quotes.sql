with source as (
    select * from {{ source('autoelite_raw', 'quotes') }}
),

renamed as (
    select
        id                  as quote_id,
        opportunity_id      as deal_id,
        account_id          as customer_id,
        contact_id,
        name                as quote_name,
        description,
        status              as quote_status,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', created_date) as created_date,
        expiration_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
