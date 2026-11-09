{{
    config(
        materialized='incremental',
        unique_key='deal_item_id'
    )
}}

with source as (
    select * from {{ source('autoelite_raw', 'deal_items') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        Id                as deal_item_id,
        OpportunityId     as deal_id,
        Product2Id        as product_id,
        PricebookEntryId  as pricebook_entry_id,
        cast(Quantity as numeric)    as quantity,
        cast(TotalPrice as numeric)  as total_price,
        primary_date,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by Id order by _loaded_at desc) = 1
)

select * from renamed
