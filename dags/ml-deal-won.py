"""
## AutoElite: Deal Won/Lost Model Training — S10 Upgrade

Trains multiple algorithms in parallel, picks the best, promotes and deploys it.

Flow:
  1. register_model    — idempotent: define the business problem in mlops.model
  2. create_dataset    — snapshot deal_features metadata into mlops.dataset
  3. train_model       — parallel training via .expand() across MODEL_CONFIGS
                         (each config invokes the Cloud Function independently)
  4. register_run      — log each result to mlops.training_run (.expand())
  5. find_best_run     — pick the run with the highest AUC
  6. should_promote    — @task.branch: beat current production? promote : skip
  7. promote           — write winning run to mlops.model_version
  8. trigger_deploy    — TriggerDagRunOperator fires ml_deploy with model_version_id
"""

import json
import os
import uuid
from datetime import datetime

import requests
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from google.cloud import bigquery

GCP_PROJECT  = os.environ.get("GCP_PROJECT", "")
ML_TRAIN_URL = os.environ.get("ML_TRAIN_URL", "")

MODEL = {
    "model_id":         "model-deal-won",
    "name":             "Deal Won/Lost Classifier",
    "business_problem": "Predict whether an open deal will result in a signed contract",
    "ticket_number":    "BA882-26",
    "owner":            "analytics_team",
}

# S10: three algorithms trained in parallel via .expand()
MODEL_CONFIGS = [
    {"algorithm": "logistic_regression",  "hyperparameters": {"max_iter": 1000, "C": 1.0}},
    {"algorithm": "random_forest",        "hyperparameters": {"n_estimators": 100}},
    {"algorithm": "gradient_boosting",    "hyperparameters": {"n_estimators": 100, "learning_rate": 0.1}},
]


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

        result = bq.query(f"""
            SELECT
                COUNT(*) AS row_count,
                (SELECT COUNT(*) FROM `{GCP_PROJECT}`.dev_ai_datasets.INFORMATION_SCHEMA.COLUMNS
                 WHERE table_name = 'deal_features'
                   AND column_name NOT IN ('deal_id', 'customer_id', 'rep_id', 'is_won')
                ) AS feature_count
            FROM `{GCP_PROJECT}.dev_ai_datasets.deal_features`
        """).result()

        row          = list(result)[0]
        data_version = datetime.utcnow().strftime("%Y-%m-%d")
        dataset_id   = f"ds-deal-won-{data_version}"

        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.dataset`
                (dataset_id, model_id, data_version, gcs_path, row_count, feature_count, created_at)
            VALUES (
                '{dataset_id}',
                '{MODEL["model_id"]}',
                '{data_version}',
                'bigquery://{GCP_PROJECT}/dev_ai_datasets/deal_features',
                {row.row_count},
                {row.feature_count},
                CURRENT_TIMESTAMP()
            )
        """).result()

        print(f"dataset logged: {dataset_id} | rows={row.row_count} | features={row.feature_count}")
        return {"dataset_id": dataset_id, "data_version": data_version}

    @task
    def train_model(config: dict, dataset_meta: dict, **context) -> dict:
        """Invoke the Cloud Function for one algorithm config. Called once per config via .expand()."""
        algorithm      = config["algorithm"]
        hyperparams    = config["hyperparameters"]
        run_id         = f"run-{MODEL['model_id']}-{uuid.uuid4().hex[:8]}"
        airflow_run_id = context["dag_run"].run_id

        resp = requests.post(
            ML_TRAIN_URL,
            json={
                "run_id":          run_id,
                "model_id":        MODEL["model_id"],
                "dataset_id":      dataset_meta["dataset_id"],
                "algorithm":       algorithm,
                "hyperparameters": hyperparams,
                "gcp_project":     GCP_PROJECT,
            },
            timeout=300,
        )
        resp.raise_for_status()
        result                    = resp.json()
        result["run_id"]          = run_id
        result["airflow_run_id"]  = airflow_run_id
        result["algorithm"]       = algorithm
        result["hyperparameters"] = hyperparams
        print(f"trained: {run_id} | algo={algorithm} | metrics={result.get('metrics')}")
        return result

    @task
    def register_run(train_result: dict, dataset_meta: dict) -> None:
        """Log one completed training run to mlops.training_run. Called once per result via .expand()."""
        bq      = bigquery.Client(project=GCP_PROJECT)
        params  = json.dumps({"algorithm": train_result["algorithm"], "hyperparameters": train_result["hyperparameters"]})
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
        print(f"run registered: {train_result['run_id']} | algo={train_result['algorithm']}")

    @task
    def find_best_run(train_results: list) -> dict:
        """Pick the run with the highest AUC from all parallel training results."""
        best = max(train_results, key=lambda r: r.get("metrics", {}).get("auc", 0))
        print(f"best run: {best['run_id']} | algo={best['algorithm']} | auc={best['metrics']['auc']}")
        return best

    @task.branch
    def should_promote(best: dict) -> str:
        """
        Compare best new run against the current production model in mlops.model_version.
        Promote if no production model exists or if the new AUC exceeds the current AUC.
        """
        bq      = bigquery.Client(project=GCP_PROJECT)
        new_auc = best.get("metrics", {}).get("auc", 0)

        result = bq.query(f"""
            SELECT JSON_VALUE(metrics, '$.auc') AS auc
            FROM `{GCP_PROJECT}.mlops.model_version`
            WHERE model_id = '{MODEL["model_id"]}'
              AND status = 'production'
            ORDER BY created_at DESC
            LIMIT 1
        """).result()

        rows = list(result)
        if not rows or rows[0].auc is None:
            print(f"no production model — promoting (auc={new_auc})")
            return "promote"

        current_auc = float(rows[0].auc)
        print(f"current production auc={current_auc} | new auc={new_auc}")
        return "promote" if new_auc > current_auc else "skip_promote"

    @task
    def promote(best: dict) -> str:
        """Write the winning run to mlops.model_version. Archive the previous production version."""
        bq               = bigquery.Client(project=GCP_PROJECT)
        model_version_id = f"mv-{MODEL['model_id']}-{uuid.uuid4().hex[:8]}"
        metrics          = json.dumps(best.get("metrics", {}))

        # archive the current production version before inserting the new one
        bq.query(f"""
            UPDATE `{GCP_PROJECT}.mlops.model_version`
            SET status = 'archived'
            WHERE model_id = '{MODEL["model_id"]}'
              AND status = 'production'
        """).result()

        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.model_version`
                (model_version_id, model_id, training_run_id, artifact_path, metrics, status, created_at)
            VALUES (
                '{model_version_id}',
                '{MODEL["model_id"]}',
                '{best["run_id"]}',
                '{best.get("artifact", "")}',
                PARSE_JSON('{metrics}'),
                'production',
                CURRENT_TIMESTAMP()
            )
        """).result()

        print(f"promoted: {model_version_id} | run={best['run_id']} | algo={best['algorithm']}")
        return model_version_id

    @task
    def skip_promote() -> None:
        print("current production model retained — no promotion")

    # -------------------------------------------------------------------------
    # wire the DAG
    # -------------------------------------------------------------------------
    model   = register_model()
    dataset = create_dataset()
    model >> dataset

    # parallel training: one Cloud Function call per config
    train_results = train_model.partial(dataset_meta=dataset).expand(config=MODEL_CONFIGS)

    # register every run to mlops.training_run (also parallel)
    register_run.partial(dataset_meta=dataset).expand(train_result=train_results)

    # pick winner and branch on whether to promote
    best   = find_best_run(train_results)
    branch = should_promote(best)

    promoted = promote(best)
    skip     = skip_promote()
    branch >> [promoted, skip]

    # trigger the deploy DAG only on the promote path
    trigger_deploy = TriggerDagRunOperator(
        task_id="trigger_deploy",
        trigger_dag_id="ml_deploy",
        conf={"model_version_id": "{{ task_instance.xcom_pull(task_ids='promote') }}"},
        wait_for_completion=False,
    )
    promoted >> trigger_deploy


ml_deal_won()
