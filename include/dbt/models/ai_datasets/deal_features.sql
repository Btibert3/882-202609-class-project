{{
    config(
        materialized='table',
        schema='ai_datasets'
    )
}}

-- Feature table for deal won/lost classification.
-- Label: is_won (contract_id is not null = deal resulted in a signed contract)
-- Aggregations live in intermediate models -- this layer only joins and derives the label.

with deals as (
    select
        deal_id,
        customer_id,
        rep_id,
        stage_name,
        amount,
        probability,
        created_date,
        close_date,
        case when contract_id is not null then 1 else 0 end as is_won
    from {{ ref('stg_deals') }}
),

activities as (
    select * from {{ ref('int_deal_activities') }}
),

deal_items as (
    select * from {{ ref('int_deal_items') }}
)

select
    -- identifiers (kept for lineage/debugging, not used as model features)
    d.deal_id,
    d.customer_id,
    d.rep_id,

    -- deal-level features
    d.stage_name,
    d.amount,
    d.probability,
    date_diff(coalesce(d.close_date, current_date()), date(d.created_date), day) as deal_age_days,

    -- deal item features (null-safe — not all deals have line items)
    coalesce(di.item_count, 0)       as item_count,
    coalesce(di.total_deal_value, 0) as total_deal_value,
    coalesce(di.avg_item_value, 0)   as avg_item_value,
    coalesce(di.unique_products, 0)  as unique_products,

    -- activity features (null-safe — not all deals have logged activities)
    coalesce(a.activity_count, 0)           as activity_count,
    coalesce(a.completed_activities, 0)     as completed_activities,
    coalesce(a.high_priority_activities, 0) as high_priority_activities,
    coalesce(a.call_count, 0)               as call_count,
    coalesce(a.email_count, 0)              as email_count,
    coalesce(a.meeting_count, 0)            as meeting_count,
    coalesce(a.activity_span_days, 0)       as activity_span_days,
    date_diff(
        current_date(),
        coalesce(a.last_activity_date, date(d.created_date)),
        day
    )                                       as days_since_last_activity,

    -- label
    d.is_won

from deals d
left join deal_items di on d.deal_id = di.deal_id
left join activities a  on d.deal_id = a.deal_id
