"""
## AutoElite: Embed Transcripts

Triggered by pipeline_call_transcripts after each ingest run.
Calls the embed-transcripts Cloud Function, which finds any transcripts
not yet in stage.call_transcript_embeddings and generates embeddings for them.

schedule=None     — only runs when triggered, never on its own schedule
max_active_runs=1 — prevents concurrent runs stepping on each other during backfill
"""

import os
import requests
from airflow.sdk import dag, task

EMBED_URL   = os.environ.get("EMBED_URL", "")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["embeddings", "call_transcripts"],
)
def pipeline_embed_transcripts():

    @task(retries=2)
    def embed() -> dict:
        """Call the Cloud Function to embed any new transcripts."""
        resp = requests.post(EMBED_URL, json={}, timeout=300)
        resp.raise_for_status()
        result = resp.json()
        print(f"embedded: {result.get('embedded', 0)}")
        return result

    embed()


pipeline_embed_transcripts()
