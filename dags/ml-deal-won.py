"""
## AutoElite: Deal Won/Lost Model Training

Orchestrates the full training pipeline for the deal won/lost classifier.

Flow:
  1. register_model    — idempotent: define the business problem in mlops.model
  2. create_dataset    — snapshot deal_features metadata into mlops.dataset
  3. train_model       — invoke Cloud Function to train, save artifact to GCS
  4. register_run      — log params, metrics, artifact path to mlops.training_run

The Cloud Function is a consumer of ai_datasets.deal_features — all features
and the label (is_won) are owned by dbt. The function trains and returns results.

S10 extends this DAG with parallel training (.expand()) and deployment branching.
"""

import json
import os
import uuid
from datetime import datetime

import requests
from airflow.sdk import dag, task
from google.cloud import bigquery

GCP_PROJECT  = os.environ.get("GCP_PROJECT", "")
ML_TRAIN_URL = os.environ.get("ML_TRAIN_URL", "")

# defines the business problem — static for the life of this model
MODEL = {
    "model_id":         "model-deal-won",
    "name":             "Deal Won/Lost Classifier",
    "business_problem": "Predict whether an open deal will result in a signed contract",
    "ticket_number":    "BA882-26",
    "owner":            "analytics_team",
}

# single algorithm for S9 — extended to parallel configs in S10
ALGORITHM    = "logistic_regression"
HYPERPARAMS  = {"max_iter": 1000, "C": 1.0}


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "model"],
)
def ml_deal_won():

    @task
    def register_model() -> None:
        """Insert the model definition once. Safe to re-run — skips if already exists."""
        bq = bigquery.Client(project=GCP_PROJECT)
        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.model`
                (model_id, name, business_problem, ticket_number, owner, created_at)
            SELECT
                '{MODEL["model_id"]}',
                '{MODEL["name"]}',
                '{MODEL["business_problem"]}',
                '{MODEL["ticket_number"]}',
                '{MODEL["owner"]}',
                CURRENT_TIMESTAMP()
            FROM (SELECT 1)
            WHERE NOT EXISTS (
                SELECT 1 FROM `{GCP_PROJECT}.mlops.model`
                WHERE model_id = '{MODEL["model_id"]}'
            )
        """).result()
        print(f"model registered: {MODEL['model_id']}")

    @task
    def create_dataset() -> dict:
        """Snapshot deal_features metadata and log to mlops.dataset."""
        bq = bigquery.Client(project=GCP_PROJECT)

        # get row count and feature count from the feature table
        result = bq.query(f"""
            SELECT
                COUNT(*)                                             AS row_count,
                (SELECT COUNT(*) FROM `{GCP_PROJECT}`.ai_datasets.INFORMATION_SCHEMA.COLUMNS
                 WHERE table_name = 'deal_features'
                   AND column_name NOT IN ('deal_id', 'customer_id', 'rep_id', 'is_won')
                ) AS feature_count
            FROM `{GCP_PROJECT}.ai_datasets.deal_features`
        """).result()

        row = list(result)[0]
        data_version = datetime.utcnow().strftime("%Y-%m-%d")
        dataset_id   = f"ds-deal-won-{data_version}"

        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.dataset`
                (dataset_id, model_id, data_version, gcs_path, row_count, feature_count, created_at)
            VALUES (
                '{dataset_id}',
                '{MODEL["model_id"]}',
                '{data_version}',
                'bigquery://{GCP_PROJECT}/ai_datasets/deal_features',
                {row.row_count},
                {row.feature_count},
                CURRENT_TIMESTAMP()
            )
        """).result()

        print(f"dataset logged: {dataset_id} | rows={row.row_count} | features={row.feature_count}")
        return {"dataset_id": dataset_id, "data_version": data_version}

    @task
    def train_model(dataset_meta: dict, **context) -> dict:
        """Invoke the Cloud Function to train the model and save the artifact to GCS."""
        run_id         = f"run-{MODEL['model_id']}-{uuid.uuid4().hex[:8]}"
        airflow_run_id = context["dag_run"].run_id

        resp = requests.post(
            ML_TRAIN_URL,
            json={
                "run_id":          run_id,
                "model_id":        MODEL["model_id"],
                "dataset_id":      dataset_meta["dataset_id"],
                "algorithm":       ALGORITHM,
                "hyperparameters": HYPERPARAMS,
                "gcp_project":     GCP_PROJECT,
            },
            timeout=300,
        )
        resp.raise_for_status()
        result = resp.json()
        result["run_id"]         = run_id
        result["airflow_run_id"] = airflow_run_id
        print(f"training complete: {run_id} | airflow_run={airflow_run_id} | metrics={result.get('metrics')}")
        return result

    @task
    def register_run(train_result: dict, dataset_meta: dict) -> None:
        """Log the completed training run into mlops.training_run."""
        bq = bigquery.Client(project=GCP_PROJECT)

        params  = json.dumps({"algorithm": ALGORITHM, "hyperparameters": HYPERPARAMS})
        metrics = json.dumps(train_result.get("metrics", {}))

        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.training_run`
                (run_id, model_id, dataset_id, airflow_run_id, params, metrics, artifact, status, created_at)
            VALUES (
                '{train_result["run_id"]}',
                '{MODEL["model_id"]}',
                '{dataset_meta["dataset_id"]}',
                '{train_result["airflow_run_id"]}',
                PARSE_JSON('{params}'),
                PARSE_JSON('{metrics}'),
                '{train_result.get("artifact", "")}',
                'completed',
                CURRENT_TIMESTAMP()
            )
        """).result()
        print(f"run registered: {train_result['run_id']}")

    model    = register_model()
    dataset  = create_dataset()
    training = train_model(dataset)
    model >> dataset
    register_run(training, dataset)


ml_deal_won()
