"""
## AutoElite: Embed Transcripts

Triggered by pipeline_call_transcripts after each ingest run.
Runs the dbt incremental model `call_transcript_embeddings`, which calls
ML.GENERATE_EMBEDDING to embed any transcripts not yet in ai_datasets.

Prerequisite: run setup/create_embedding_model.sql once to create the remote model.

schedule=None     — only runs when triggered, never on its own schedule
max_active_runs=1 — prevents concurrent runs stepping on each other during backfill
"""

import os
import subprocess
from pathlib import Path

from airflow.sdk import dag, task

DBT_PROJECT_PATH = (Path(__file__).parent / "../include/dbt").absolute()
DBT_EXECUTABLE   = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["embeddings", "call_transcripts", "dbt"],
)
def pipeline_embed_transcripts():

    @task
    def embed() -> None:
        """Run the dbt incremental model to embed any new transcripts."""
        result = subprocess.run(
            [
                DBT_EXECUTABLE, "run",
                "--select", "call_transcript_embeddings",
                "--profiles-dir", str(DBT_PROJECT_PATH),
                "--project-dir", str(DBT_PROJECT_PATH),
            ],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError("dbt run failed — see logs above")

    embed()


pipeline_embed_transcripts()
