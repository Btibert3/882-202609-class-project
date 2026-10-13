with source as (
    select * from {{ source('autoelite_raw', 'activities') }}
),

renamed as (
    select
        -- TODO: inspect the raw table and rename columns
        -- hint: what_id is the deal FK -- what should it be called?
        -- follow the same patterns as stg_deals.sql
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
