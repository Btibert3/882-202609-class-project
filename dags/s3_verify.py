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
    e = extract()
    l = load(e)
    e >> l


verify()
