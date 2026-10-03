-- reps is static reference data — loaded via dbt seed, not the API pipeline
with source as (
    select * from {{ ref('reps') }}
),

renamed as (
    select
        Id       as rep_id,
        FirstName as first_name,
        LastName  as last_name,
        Email     as email,
        Phone     as phone,
        Username  as username,
        Alias     as alias
    from source
)

select * from renamed
