with source as (
    select * from {{ source('autoelite_raw', 'call_transcripts') }}
),

renamed as (
    select
        id                                                                    as transcript_id,
        opportunity_id                                                        as deal_id,
        lead_id,
        body                                                                  as transcript_text,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', created_date)             as created_date,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', end_time)                 as end_time,
        _loaded_at,
        _source
    from source
    qualify row_number() over (partition by id order by _loaded_at desc) = 1
)

select * from renamed
