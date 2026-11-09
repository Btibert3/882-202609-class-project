with activities as (
    select * from {{ ref('stg_activities') }}
    where deal_id is not null
)

select
    deal_id,
    count(*)                                        as activity_count,
    countif(lower(status) = 'completed')            as completed_activities,
    countif(lower(priority) = 'high')               as high_priority_activities,
    countif(lower(subject) like '%call%')           as call_count,
    countif(lower(subject) like '%email%')          as email_count,
    countif(lower(subject) like '%meeting%')        as meeting_count,
    max(parse_date('%Y-%m-%d', activity_date))      as last_activity_date,
    min(parse_date('%Y-%m-%d', activity_date))      as first_activity_date,
    date_diff(
        max(parse_date('%Y-%m-%d', activity_date)),
        min(parse_date('%Y-%m-%d', activity_date)),
        day
    )                                               as activity_span_days
from activities
group by deal_id
