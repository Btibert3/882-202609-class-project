"""
## AutoElite: Call Transcripts Pipeline

Pulls call transcript records from the AutoElite API and loads them into BigQuery.
At the end of each run, triggers the embedding pipeline to enrich any new transcripts.

Pattern: API → GCS (raw artifact) → BigQuery → TriggerDagRunOperator (embed)
"""

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from google.cloud import storage
from include.transforms import normalize_rows

API_BASE    = os.environ.get("AUTOELITE_API_BASE", "")
API_KEY     = os.environ.get("AUTOELITE_API_KEY", "")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")

TABLE = "call_transcripts"


@dag(
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["raw", "call_transcripts"],
)
def pipeline_call_transcripts():

    @task(retries=3)
    def extract(data_interval_end=None) -> str:
        """Call the API and write raw rows to GCS. Returns the blob path."""

        run_date = (data_interval_end - timedelta(days=1)).strftime("%Y-%m-%d")

        resp = requests.get(
            f"{API_BASE}/data/{TABLE}",
            params={"date": run_date, "api_key": API_KEY},
            timeout=30,
        )
        if resp.status_code in (400, 403):
            raise AirflowSkipException(f"API returned {resp.status_code} for {run_date}: {resp.text}")
        resp.raise_for_status()

        rows = resp.json().get("rows", [])
        print(f"{TABLE} | {run_date} | {len(rows)} rows")

        if not rows:
            print("no rows — skipping GCS write")
            return None

        rows = normalize_rows(rows)

        ndjson = "\n".join(json.dumps(row, default=str) for row in rows)

        blob_path = f"autoelite/raw/{TABLE}/date={run_date}/data.json"
        storage.Client().bucket(GCS_BUCKET).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        print(f"gs://{GCS_BUCKET}/{blob_path}")
        return blob_path

    @task
    def load(blob_path: str | None) -> None:
        """Load the GCS file into BigQuery."""
        if blob_path is None:
            print("no file to load — skipping")
            return

        from google.cloud import bigquery, storage as gcs

        blob = gcs.Client().bucket(GCS_BUCKET).blob(blob_path)
        blob.reload()
        if blob.size == 0:
            print("empty file — skipping load")
            return

        bq  = bigquery.Client(project=GCP_PROJECT)
        job = bq.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{blob_path}",
            f"{GCP_PROJECT}.autoelite_raw.{TABLE}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,
                ignore_unknown_values=True,
            ),
        )
        job.result()
        print(f"loaded → {GCP_PROJECT}.autoelite_raw.{TABLE}")

    trigger_embed = TriggerDagRunOperator(
        task_id="trigger_embed",
        trigger_dag_id="pipeline_embed_transcripts",
        wait_for_completion=False,
    )

    load(extract()) >> trigger_embed


pipeline_call_transcripts()
