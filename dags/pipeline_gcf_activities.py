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

# TODO: paste your deployed function URL here
GCF_URL    = ""
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
        # TODO: write records to GCS at the path above
        #       look at pipeline_gcf_weather.py for the pattern

    load(extract())


pipeline_gcf_activities()
