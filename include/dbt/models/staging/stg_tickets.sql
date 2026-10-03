with source as (
    select * from {{ source('autoelite_raw', 'tickets') }}
),

renamed as (
    select
        id                  as ticket_id,
        priority,
        subject,
        description,
        status              as ticket_status,
        contact_id,
        account_id          as customer_id,
        owner_id            as rep_id,
        order_item_id__c    as order_item_id,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', created_date) as created_date,
        closed_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
