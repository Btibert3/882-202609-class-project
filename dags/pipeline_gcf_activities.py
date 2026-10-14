"""
Pipeline: activities via Cloud Function
Airflow orchestrates; the Cloud Function does the work.

catchup=True means Airflow will backfill from start_date automatically.
Each run passes its logical date (ds) to the function.
"""

import json
import os

import pendulum
import requests
from airflow.sdk import dag, task
from google.cloud import storage

GCF_URL    = "https://us-central1-btibert-ba882-fall26.cloudfunctions.net/activities-extract"
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3},
    tags=["activities", "gcf"],
)
def pipeline_gcf_activities():

    @task()
    def extract(ds=None) -> dict:
        resp = requests.post(GCF_URL, json={"date": ds}, timeout=90)
        resp.raise_for_status()
        return resp.json()

    @task()
    def load(payload: dict) -> None:
        date    = payload["date"]
        records = payload["records"]
        blob    = f"activities/date={date}/data.json"
        storage.Client().bucket(GCS_BUCKET).blob(blob).upload_from_string(
            json.dumps(records), content_type="application/json"
        )
        print(f"loaded {len(records)} records → gs://{GCS_BUCKET}/{blob}")

    load(extract())


pipeline_gcf_activities()
