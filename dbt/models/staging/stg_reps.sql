with source as (
    select * from {{ source('autoelite_raw', 'reps') }}
),

renamed as (
    select
        id         as rep_id,
        first_name,
        last_name,
        email,
        phone,
        username,
        alias,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
