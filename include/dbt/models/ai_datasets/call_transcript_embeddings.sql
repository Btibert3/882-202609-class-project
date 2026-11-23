{{
    config(
        materialized='incremental',
        schema='ai_datasets',
        unique_key='transcript_id'
    )
}}

select
    transcript_id,
    transcript_text,
    ml_generate_embedding_result   as embedding
from ML.GENERATE_EMBEDDING(
    MODEL `{{ env_var('GCP_PROJECT', 'PROJECT_NOT_SET') }}.stage.embed_model`,
    (
        select transcript_id as id, transcript_text as content
        from {{ ref('stg_call_transcripts') }}
        {% if is_incremental() %}
        where transcript_id not in (select transcript_id from {{ this }})
        {% endif %}
    )
)
where ml_generate_embedding_status = ''
