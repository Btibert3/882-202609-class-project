with source as (
    select * from {{ source('autoelite_raw', 'deals') }}
),

renamed as (
    select
        id           as deal_id,
        contract_id,
        account_id   as customer_id,
        contact_id,
        owner_id     as rep_id,
        stage_name,
        amount,
        probability,
        created_date,
        close_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
