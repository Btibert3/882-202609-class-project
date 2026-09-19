"""
## AutoElite: Customers Pipeline

Pulls customer records from the AutoElite API and loads them into BigQuery.

Pattern: API → GCS (raw artifact) → BigQuery
"""

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import dag, task
from google.cloud import storage


API_BASE    = os.environ.get("AUTOELITE_API_BASE", "")
API_KEY     = os.environ.get("AUTOELITE_API_KEY", "")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")

TABLE = "customers"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["raw", "customers"],
)
def pipeline_customers():

    @task
    def extract(data_interval_end=None) -> str:
        """Call the API and write the raw rows to GCS. Returns the blob path."""

        run_date = data_interval_end.strftime("%Y-%m-%d")

        resp = requests.get(
            f"{API_BASE}/data/{TABLE}",
            params={"date": run_date, "api_key": API_KEY},
            timeout=30,
        )
        resp.raise_for_status()

        rows = resp.json().get("rows", [])
        print(f"{TABLE} | {run_date} | {len(rows)} rows")

        if not rows:
            print("no rows — skipping GCS write")
            return None

        # one row per line so BigQuery can load the file directly
        ndjson = "\n".join(json.dumps(row, default=str) for row in rows)

        blob_path = f"autoelite/raw/{TABLE}/date={run_date}/data.json"
        storage.Client().bucket(GCS_BUCKET).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        print(f"gs://{GCS_BUCKET}/{blob_path}")
        return blob_path

    @task
    def load(blob_path: str | None) -> None:
        """Load the GCS file directly into BigQuery. BigQuery reads the file — Python doesn't."""
        if blob_path is None:
            print("no file to load — skipping")
            return

        from google.cloud import bigquery

        bq  = bigquery.Client(project=GCP_PROJECT)
        job = bq.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{blob_path}",
            f"{GCP_PROJECT}.autoelite_raw.{TABLE}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True,
            ),
        )
        job.result()
        print(f"loaded → {GCP_PROJECT}.autoelite_raw.{TABLE}")

    load(extract())


pipeline_customers()
