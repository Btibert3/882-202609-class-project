"""
## AutoElite: MLOps Schema Setup

Creates the mlops dataset and tracking tables in BigQuery.
Run once manually to initialize the environment before any model training.

Re-run safe — all DDL uses CREATE TABLE IF NOT EXISTS.

All tables live in the mlops dataset in BigQuery:
  mlops.model          — business problem definition
  mlops.dataset        — versioned training data snapshots
  mlops.training_run   — one experiment attempt (params, metrics, artifact)
  mlops.model_version  — promoted model artifacts (used in S10)
  mlops.deployment     — inference endpoints (used in S10)
"""

import os
from datetime import datetime

from airflow.sdk import dag, task
from google.cloud import bigquery

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "setup"],
)
def mlops_setup():

    @task
    def create_datasets():
        """Create the mlops BigQuery dataset if it doesn't exist."""
        bq = bigquery.Client(project=GCP_PROJECT)
        dataset = bigquery.Dataset(f"{GCP_PROJECT}.mlops")
        dataset.location = "US"
        bq.create_dataset(dataset, exists_ok=True)
        print(f"dataset ready: {GCP_PROJECT}.mlops")

    @task
    def create_model_table():
        """mlops.model — defines the business problem being solved."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.mlops.model` (
                model_id         STRING,
                name             STRING,
                business_problem STRING,
                ticket_number    STRING,
                owner            STRING,
                created_at       TIMESTAMP
            )
        """).result()
        print(f"table ready: {GCP_PROJECT}.mlops.model")

    @task
    def create_dataset_table():
        """mlops.dataset — versioned snapshot of training data used in a run."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.mlops.dataset` (
                dataset_id    STRING,
                model_id      STRING,
                data_version  STRING,
                gcs_path      STRING,
                row_count     INT64,
                feature_count INT64,
                created_at    TIMESTAMP
            )
        """).result()
        print(f"table ready: {GCP_PROJECT}.mlops.dataset")

    @task
    def create_training_run_table():
        """mlops.training_run — one experiment: algorithm, params, metrics, artifact."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.mlops.training_run` (
                run_id     STRING,
                model_id   STRING,
                dataset_id STRING,
                params     JSON,
                metrics    JSON,
                artifact   STRING,
                status     STRING,
                created_at TIMESTAMP
            )
        """).result()
        print(f"table ready: {GCP_PROJECT}.mlops.training_run")

    @task
    def create_model_version_table():
        """mlops.model_version — promoted artifact from a training run (used in S10)."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.mlops.model_version` (
                model_version_id STRING,
                model_id         STRING,
                training_run_id  STRING,
                artifact_path    STRING,
                metrics          JSON,
                status           STRING,
                created_at       TIMESTAMP
            )
        """).result()
        print(f"table ready: {GCP_PROJECT}.mlops.model_version")

    @task
    def create_deployment_table():
        """mlops.deployment — inference endpoint registry (used in S10)."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.mlops.deployment` (
                deployment_id    STRING,
                model_version_id STRING,
                endpoint_url     STRING,
                traffic_split    FLOAT64,
                deployed_at      TIMESTAMP
            )
        """).result()
        print(f"table ready: {GCP_PROJECT}.mlops.deployment")

    # datasets must exist before any table creation
    datasets = create_datasets()
    datasets >> create_model_table()
    datasets >> create_dataset_table()
    datasets >> create_training_run_table()
    datasets >> create_model_version_table()
    datasets >> create_deployment_table()


mlops_setup()
