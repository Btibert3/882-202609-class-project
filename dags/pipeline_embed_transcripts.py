"""
## AutoElite: Embed Transcripts

Triggered by pipeline_call_transcripts after each ingest run.
Uses BigQuery ML.GENERATE_EMBEDDING to embed any call transcripts not yet
in stage.call_transcript_embeddings. BQ handles batching internally — no
Cloud Function needed.

Prerequisite: run setup/create_embedding_model.sql once to create the remote model.

schedule=None     — only runs when triggered, never on its own schedule
max_active_runs=1 — prevents concurrent runs stepping on each other during backfill
"""

import os
from airflow.sdk import dag, task
from google.cloud import bigquery

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")

EMBED_TABLE  = f"`{GCP_PROJECT}.stage.call_transcript_embeddings`"
SOURCE_TABLE = f"`{GCP_PROJECT}.autoelite_raw.call_transcripts`"
EMBED_MODEL  = f"`{GCP_PROJECT}.stage.embed_model`"


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["embeddings", "call_transcripts"],
)
def pipeline_embed_transcripts():

    @task
    def create_embeddings_table() -> None:
        """Ensure the destination table exists."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS {EMBED_TABLE} (
                transcript_id   STRING,
                transcript_text STRING,
                embedding       ARRAY<FLOAT64>
            )
        """).result()
        print("embeddings table ready")

    @task
    def embed() -> int:
        """
        Insert embeddings for any transcripts not yet embedded.
        ML.GENERATE_EMBEDDING calls Vertex AI via the remote model and
        handles batching internally — no row limits to manage here.
        """
        bq = bigquery.Client(project=GCP_PROJECT)

        job = bq.query(f"""
            INSERT INTO {EMBED_TABLE} (transcript_id, transcript_text, embedding)
            SELECT
                id                             AS transcript_id,
                content                        AS transcript_text,
                ml_generate_embedding_result   AS embedding
            FROM ML.GENERATE_EMBEDDING(
                MODEL {EMBED_MODEL},
                (
                    SELECT id, body AS content
                    FROM `{GCP_PROJECT}.autoelite_raw.call_transcripts`
                    WHERE id NOT IN (
                        SELECT transcript_id FROM {EMBED_TABLE}
                    )
                )
            )
            WHERE ml_generate_embedding_status = ''
        """)

        job.result()
        rows_inserted = job.num_dml_affected_rows or 0
        print(f"embedded {rows_inserted} new transcripts")
        return rows_inserted

    create_embeddings_table() >> embed()


pipeline_embed_transcripts()
