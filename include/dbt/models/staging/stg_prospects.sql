with source as (
    select * from {{ source('autoelite_raw', 'prospects') }}
),

renamed as (
    select
        id                      as prospect_id,
        first_name,
        last_name,
        email,
        phone,
        status                  as prospect_status,
        owner_id                as rep_id,
        converted_contact_id,
        converted_account_id    as converted_customer_id,
        is_converted,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', created_date) as created_date,
        converted_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
