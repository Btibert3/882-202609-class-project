"""
## AutoElite Customers Pipeline

Pulls daily customer records from the AutoElite CRM API, lands the raw JSON
in GCS, then reads that file and loads it into BigQuery.

### Pattern
    API (/data/customers?date=...) → GCS (raw artifact) → BigQuery (autoelite_raw.customers)

### GCS path structure
    autoelite/raw/customers/date={YYYY-MM-DD}/data.json

Landing in GCS first is intentional — the file is a durable artifact you can
inspect, reprocess, or replay without calling the API again.

### Backfill
The API feed starts 2026-09-01. With catchup=True, Airflow will schedule one
run per day from start_date forward when the DAG is first turned on. Each run
processes the date that is one day before its scheduled interval end (the "prior
business day" pattern the API expects).

### Retries
The API intentionally fails ~12% of requests with a 503. Retries handle this —
the pipeline should succeed without manual intervention.

### dbt
dbt is run manually for now: `dbt run --select stg_customers` from inside
include/dbt/. Wiring Airflow to trigger dbt automatically comes in a later session.
"""

import json
import os
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
from google.cloud import storage


API_BASE = os.environ.get("AUTOELITE_API_BASE", "")
API_KEY  = os.environ.get("AUTOELITE_API_KEY", "")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")

TABLE = "customers"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
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
    def extract(data_interval_end=None) -> dict:
        """
        Call the AutoElite API for the prior day's customer records.
        Returns the parsed JSON response payload.
        """
        # API is date-gated: request the day before the interval end
        run_date = (data_interval_end - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Requesting {TABLE} for date={run_date}")

        resp = requests.get(
            f"{API_BASE}/data/{TABLE}",
            params={"date": run_date, "api_key": API_KEY},
            timeout=30,
        )
        resp.raise_for_status()  # non-200 triggers retry via retries setting
        payload = resp.json()
        print(f"Received {len(payload.get('rows', []))} rows for {run_date}")
        return payload

    @task
    def land_to_gcs(payload: dict) -> str:
        """
        Write the raw API response to GCS as a JSON artifact.
        Returns the GCS blob path so the next task knows where to read from.

        Path: autoelite/raw/customers/date={YYYY-MM-DD}/data.json
        """
        run_date = payload["date"]
        blob_path = f"autoelite/raw/{TABLE}/date={run_date}/data.json"

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(payload, default=str),
            content_type="application/json",
        )
        print(f"Landed raw artifact → gs://{GCS_BUCKET}/{blob_path}")
        return blob_path

    @task
    def load_to_bigquery(blob_path: str) -> None:
        """
        Read the raw JSON artifact from GCS and load rows into BigQuery.

        Rows are appended with _loaded_at and _source metadata columns so
        the staging model can deduplicate on id + _loaded_at.
        """
        from google.cloud import bigquery
        from datetime import timezone

        client_gcs = storage.Client()
        bucket = client_gcs.bucket(GCS_BUCKET)
        blob = bucket.blob(blob_path)
        payload = json.loads(blob.download_as_text())

        rows = payload.get("rows", [])
        run_date = payload["date"]

        if not rows:
            print(f"No rows for {run_date} — skipping BigQuery load")
            return

        loaded_at = datetime.now(timezone.utc).isoformat()
        gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"

        for row in rows:
            row["_loaded_at"] = loaded_at
            row["_source"] = gcs_uri

        project = os.environ.get("GCP_PROJECT", "")
        bq = bigquery.Client(project=project)
        table_ref = f"{project}.autoelite_raw.{TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        load_job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        print(f"Loaded {len(rows)} rows → {table_ref}")

    payload   = extract()
    blob_path = land_to_gcs(payload)
    load_to_bigquery(blob_path)


pipeline_customers()
