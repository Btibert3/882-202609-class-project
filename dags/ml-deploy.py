"""
## AutoElite: Deal Won/Lost Model Deployment

Triggered by ml_deal_won via TriggerDagRunOperator when a new model version is promoted.
Writes the active deployment record to mlops.deployment and archives the previous one.

endpoint_url = the ml-predict-deals Cloud Function URL.
The serving function queries mlops.deployment WHERE traffic_split > 0 to find which
model version is active, then loads the artifact from mlops.model_version.artifact_path.

Flow:
  1. register_deployment — look up the promoted model_version, archive prior deployment,
                           write new row to mlops.deployment with traffic_split=1.0
"""

import os
import uuid
from datetime import datetime

from airflow.sdk import dag, task
from google.cloud import bigquery

GCP_PROJECT  = os.environ.get("GCP_PROJECT", "")
ML_SERVE_URL = os.environ.get("ML_SERVE_URL", "")


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "deploy"],
)
def ml_deploy():

    @task
    def register_deployment(**context) -> None:
        """
        Read model_version_id from the trigger conf, look up the artifact path in
        mlops.model_version, archive any prior active deployment, and insert the new one.
        """
        bq               = bigquery.Client(project=GCP_PROJECT)
        model_version_id = context["dag_run"].conf.get("model_version_id")

        if not model_version_id:
            raise ValueError("model_version_id missing from dag_run conf")

        # look up the artifact path for this model version
        rows = list(bq.query(f"""
            SELECT model_id, artifact_path
            FROM `{GCP_PROJECT}.mlops.model_version`
            WHERE model_version_id = '{model_version_id}'
              AND status = 'production'
        """).result())

        if not rows:
            raise ValueError(f"model_version_id={model_version_id} not found or not in production status")

        model_id     = rows[0].model_id
        artifact_path = rows[0].artifact_path

        # archive all prior active deployments
        bq.query(f"""
            UPDATE `{GCP_PROJECT}.mlops.deployment`
            SET traffic_split = 0.0
            WHERE traffic_split > 0
        """).result()

        deployment_id = f"dep-{model_version_id}-{uuid.uuid4().hex[:6]}"

        bq.query(f"""
            INSERT INTO `{GCP_PROJECT}.mlops.deployment`
                (deployment_id, model_version_id, endpoint_url, traffic_split, deployed_at)
            VALUES (
                '{deployment_id}',
                '{model_version_id}',
                '{ML_SERVE_URL}',
                1.0,
                CURRENT_TIMESTAMP()
            )
        """).result()

        print(f"deployed: {deployment_id} | version={model_version_id} | endpoint={ML_SERVE_URL}")

    register_deployment()


ml_deploy()
