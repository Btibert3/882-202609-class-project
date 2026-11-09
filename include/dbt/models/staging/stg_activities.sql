with source as (
    select * from {{ source('autoelite_raw', 'activities') }}
),

renamed as (
    select
        id            as activity_id,
        what_id       as deal_id,
        owner_id      as rep_id,
        priority,
        status,
        activity_date,
        subject,
        description,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
