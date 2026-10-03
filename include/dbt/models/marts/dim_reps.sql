-- Dimension table: one row per sales rep.

select
    rep_id,
    first_name,
    last_name,
    email,
    phone,
    username,
    alias

from {{ ref('stg_reps') }}
