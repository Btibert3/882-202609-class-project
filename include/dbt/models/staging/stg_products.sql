-- products is static reference data — loaded via dbt seed, not the API pipeline
with source as (
    select * from {{ ref('products') }}
),

renamed as (
    select
        Id              as product_id,
        Name            as product_name,
        Description     as product_description,
        IsActive        as is_active,
        External_ID__c  as external_id
    from source
)

select * from renamed
