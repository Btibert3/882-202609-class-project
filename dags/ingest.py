"""
## AutoElite: Raw Ingestion Pipeline

Keeps the raw layer current for the tables that feed our exercises.
Runs daily and backfills from the API open date (2026-09-01).

For each table, three steps run in order:
    1. create  — ensure the raw table exists in BigQuery (CREATE IF NOT EXISTS)
    2. extract — call the API, write raw rows to GCS as a dated artifact
    3. load    — tell BigQuery to load from that GCS file (BigQuery reads it, Python doesn't)

The three tables run as independent branches so a retry on one table
does not hold up the others.

The flat files loaded in Session 2 are the pre-9/1 history.
This DAG picks up from there and keeps appending new records as they arrive.

Note: reps and products are static reference data — they will be handled
as dbt seeds in a later session.
"""

import json
import os
from datetime import timedelta

import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task
from utils.transforms import normalize_rows
from google.cloud import storage


API_BASE    = os.environ.get("AUTOELITE_API_BASE", "")
API_KEY     = os.environ.get("AUTOELITE_API_KEY", "")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")

DATASET = "autoelite_raw"

# ── helpers ───────────────────────────────────────────────────────────────────

def _extract(table: str, data_interval_end) -> str:
    """Call the API for one table, write NDJSON rows to GCS, return blob path."""
    run_date = (data_interval_end - timedelta(days=1)).strftime("%Y-%m-%d")

    resp = requests.get(
        f"{API_BASE}/data/{table}",
        params={"date": run_date, "api_key": API_KEY},
        timeout=30,
    )
    if resp.status_code in (400, 403):
        raise AirflowSkipException(f"API returned {resp.status_code} for {run_date}: {resp.text}")
    resp.raise_for_status()

    rows = resp.json().get("rows", [])
    print(f"{table} | {run_date} | {len(rows)} rows")

    if not rows:
        print("no rows — skipping GCS write")
        return None

    rows = normalize_rows(rows)

    ndjson = "\n".join(json.dumps(row, default=str) for row in rows)
    blob_path = f"autoelite/raw/{table}/date={run_date}/data.json"
    storage.Client().bucket(GCS_BUCKET).blob(blob_path).upload_from_string(
        ndjson, content_type="application/json"
    )
    print(f"gs://{GCS_BUCKET}/{blob_path}")
    return blob_path


def _load(blob_path: str | None, table: str) -> None:
    """Load a GCS file directly into BigQuery. BigQuery reads the file — Python doesn't."""
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
        f"{GCP_PROJECT}.{DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        ),
    )
    job.result()
    print(f"loaded → {GCP_PROJECT}.{DATASET}.{table}")


def _create(ddl: str) -> None:
    """Run a CREATE TABLE IF NOT EXISTS statement in BigQuery."""
    from google.cloud import bigquery
    bigquery.Client(project=GCP_PROJECT).query(ddl).result()
    print("table ready")


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 31, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["raw", "ingest"],
)
def pipeline_raw():

    # ── customers ─────────────────────────────────────────────────────────────

    @task
    def create_customers():
        _create(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{DATASET}.customers` (
                id             STRING,
                first_name     STRING,
                last_name      STRING,
                phone          STRING,
                description    STRING,
                shipping_state STRING,
                person_email   STRING,
                _loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                _source        STRING    DEFAULT 'api'
            )
        """)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_customers(data_interval_end=None) -> str:
        return _extract("customers", data_interval_end)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def load_customers(blob_path: str) -> None:
        _load(blob_path, "customers")

    # ── orders ────────────────────────────────────────────────────────────────

    @task
    def create_orders():
        _create(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{DATASET}.orders` (
                id             STRING,
                account_id     STRING,
                status         STRING,
                effective_date DATE,
                pricebook_id   STRING,
                owner_id       STRING,
                opportunity_id STRING,
                _loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                _source        STRING    DEFAULT 'api'
            )
        """)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_orders(data_interval_end=None) -> str:
        return _extract("orders", data_interval_end)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def load_orders(blob_path: str) -> None:
        _load(blob_path, "orders")

    # ── order_items ───────────────────────────────────────────────────────────

    @task
    def create_order_items():
        _create(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{DATASET}.order_items` (
                id                 STRING,
                order_id           STRING,
                product_id         STRING,
                quantity           FLOAT64,
                unit_price         FLOAT64,
                pricebook_entry_id STRING,
                _loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                _source            STRING    DEFAULT 'api'
            )
        """)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_order_items(data_interval_end=None) -> str:
        return _extract("order_items", data_interval_end)

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def load_order_items(blob_path: str) -> None:
        _load(blob_path, "order_items")

    # customers
    sc  = create_customers()
    ec  = extract_customers()
    lc  = load_customers(ec)
    sc >> ec >> lc

    # orders
    so  = create_orders()
    eo  = extract_orders()
    lo  = load_orders(eo)
    so >> eo >> lo

    # order_items
    soi = create_order_items()
    eoi = extract_order_items()
    loi = load_order_items(eoi)
    soi >> eoi >> loi


pipeline_raw()
