# a simple dag to show the taskflow pattern and xcom

import os
import requests
from airflow.sdk import dag, task
from google.cloud import storage

# this is defined in our env and is mapped into airflow
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")


@dag(
    schedule=None,
    tags=["smoke-test"],
)
def verify():

    @task
    def runtime_info(data_interval_start=None, data_interval_end=None, ds=None, run_id=None, dag_run=None) -> None:
        # Airflow injects schedule and run context into tasks automatically.
        # These values are available in every task — no need to pass them explicitly.
        # See: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        print(f"run_id:               {run_id}")
        print(f"data_interval_start:  {data_interval_start}")
        print(f"data_interval_end:    {data_interval_end}")
        print(f"ds:                   {ds}")  # shorthand for data_interval_start as YYYY-MM-DD string
        print(f"dag_run.conf:         {dag_run.conf}")  # payload passed when triggering manually
        print(f"GCS_BUCKET:           {GCS_BUCKET}")

    @task
    def extract() -> str:
        # a simple API request — fetch current weather for Boston
        # NOTE this is a simple response (very small data) so we can pass it. but this isnt really ideal
        resp = requests.get("https://wttr.in/Boston?format=3", timeout=10)
        resp.raise_for_status()
        return resp.text

    @task
    def load(content: str) -> None:
        storage.Client().bucket(GCS_BUCKET).blob("smoke-test.txt").upload_from_string(
            content, content_type="text/plain"
        )
        print(f"wrote to gs://{GCS_BUCKET}/smoke-test.txt")

    # define the pipeline: extract passes its return value to load via XCom
    # this is equivalent to calling load(extract()) — both define the same dependency
    r = runtime_info()
    e = extract()
    l = load(e)
    r >> e >> l


verify()
