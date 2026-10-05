# Another pattern
# use cloud function as the compute, and Airflow as a pure orchestrator
# we will use this in the second phase of the course

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import dag, task
from google.cloud import storage

GCF_URL    = "https://us-central1-btibert-ba882-fall26.cloudfunctions.net/weather-extract"
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")


@dag(
    schedule="32 9 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["weather", "gcf"],
)
def pipeline_gcf_weather():

    @task()
    def extract() -> dict:
        resp = requests.get(GCF_URL, timeout=90)
        resp.raise_for_status()
        return resp.json()

    @task()
    def load(payload: dict) -> None:
        date = payload["date"]
        blob_path = f"weather/date={date}/data.json"
        storage.Client().bucket(GCS_BUCKET).blob(blob_path).upload_from_string(
            json.dumps(payload), content_type="application/json"
        )
        print(f"gs://{GCS_BUCKET}/{blob_path}")

    load(extract())


pipeline_gcf_weather()
