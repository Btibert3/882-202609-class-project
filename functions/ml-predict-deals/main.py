# Serve deal won/lost predictions from the currently deployed model.
# Looks up the active deployment from mlops.deployment, loads the artifact
# from GCS, pulls features for the requested deal from BigQuery, and returns
# a win probability score.

import io
import os

import functions_framework
import joblib
import pandas as pd
from google.cloud import bigquery, storage

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")

MODEL_ID = "model-deal-won"

# columns that identify a row but are not model features — must match training
ID_COLS = ["deal_id", "customer_id", "rep_id", "is_won", "stage_name"]


@functions_framework.http
def task(request):
    """
    Score one or more deals against the currently deployed model.

    Query params:
      deal_id  (optional) — score a single deal by ID
               omit to score all deals in ai_datasets.deal_features

    Returns:
      list of {deal_id, win_probability, prediction, model_version_id}
    """
    deal_id = request.args.get("deal_id")

    bq = bigquery.Client(project=GCP_PROJECT)

    # -------------------------------------------------------------------------
    # look up the active deployment
    # -------------------------------------------------------------------------
    rows = list(bq.query(f"""
        SELECT d.model_version_id, d.endpoint_url AS artifact_path
        FROM `{GCP_PROJECT}.mlops.deployment` d
        JOIN `{GCP_PROJECT}.mlops.model_version` mv
          ON d.model_version_id = mv.model_version_id
        WHERE mv.model_id = '{MODEL_ID}'
          AND d.traffic_split > 0
        ORDER BY d.deployed_at DESC
        LIMIT 1
    """).result())

    if not rows:
        return {"error": "no active deployment found — run the training pipeline first"}, 404

    model_version_id = rows[0].model_version_id
    artifact_path    = rows[0].artifact_path

    print(f"serving model_version_id={model_version_id} | artifact={artifact_path}")

    # -------------------------------------------------------------------------
    # load the model artifact from GCS
    # -------------------------------------------------------------------------
    gcs_path   = artifact_path.replace("gs://", "")
    bucket_name, blob_path = gcs_path.split("/", 1)

    model_bytes = io.BytesIO()
    storage.Client().bucket(bucket_name).blob(blob_path).download_to_file(model_bytes)
    model_bytes.seek(0)
    model = joblib.load(model_bytes)

    print("model loaded")

    # -------------------------------------------------------------------------
    # pull features from BigQuery
    # -------------------------------------------------------------------------
    where = f"WHERE deal_id = '{deal_id}'" if deal_id else ""

    df = bq.query(f"""
        SELECT * FROM `{GCP_PROJECT}.dev_ai_datasets.deal_features`
        {where}
    """).to_dataframe()

    if df.empty:
        return {"error": "no deals found", "deal_id": deal_id}, 404

    feature_cols = [c for c in df.columns if c not in ID_COLS]
    X = df[feature_cols].fillna(0)

    # -------------------------------------------------------------------------
    # score
    # -------------------------------------------------------------------------
    probabilities = model.predict_proba(X)[:, 1]
    predictions   = model.predict(X)

    results = []
    for i, row in df.iterrows():
        results.append({
            "deal_id":          row["deal_id"],
            "win_probability":  round(float(probabilities[i]), 4),
            "prediction":       int(predictions[i]),
            "model_version_id": model_version_id,
        })

    print(f"scored {len(results)} deals")
    return {"predictions": results, "count": len(results)}, 200
