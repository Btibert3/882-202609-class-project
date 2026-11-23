{{
    config(
        materialized='incremental',
        schema='ai_datasets',
        unique_key='transcript_id'
    )
}}

select
    id                             as transcript_id,
    body                           as transcript_text,
    ml_generate_embedding_result   as embedding
from ML.GENERATE_EMBEDDING(
    MODEL `{{ env_var('GCP_PROJECT', 'PROJECT_NOT_SET') }}.stage.embed_model`,
    (
        select id, body as content
        from `{{ env_var('GCP_PROJECT', 'PROJECT_NOT_SET') }}.autoelite_raw.call_transcripts`
        {% if is_incremental() %}
        where id not in (select transcript_id from {{ this }})
        {% endif %}
    )
)
where ml_generate_embedding_status = ''
