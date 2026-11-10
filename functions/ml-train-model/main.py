# Train a deal won/lost classifier from the ai_datasets.deal_features table.
# Reads from BigQuery, trains a sklearn model, saves the artifact to GCS.
# All features and the label (is_won) are owned by dbt -- no feature engineering here.

import io
import json
import os

import functions_framework
import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

GCS_BUCKET = os.environ.get("GCS_BUCKET", "")

# columns that identify a row but are not model features
ID_COLS = ["deal_id", "customer_id", "rep_id", "is_won", "stage_name"]


@functions_framework.http
def task(request):
    """
    Train a classifier on ai_datasets.deal_features.

    Expected JSON body:
      run_id, model_id, dataset_id, algorithm, hyperparameters, gcp_project
    """
    body = request.get_json(silent=True) or {}

    run_id      = body.get("run_id")
    model_id    = body.get("model_id")
    dataset_id  = body.get("dataset_id")
    algorithm   = body.get("algorithm", "logistic_regression")
    hyperparams = body.get("hyperparameters", {})
    gcp_project = body.get("gcp_project")

    if not all([run_id, model_id, dataset_id, gcp_project]):
        return {"error": "missing required fields: run_id, model_id, dataset_id, gcp_project"}, 400

    print(f"starting training | run_id={run_id} | algorithm={algorithm}")

    # -------------------------------------------------------------------------
    # load features from BigQuery
    # -------------------------------------------------------------------------
    bq = bigquery.Client(project=gcp_project)
    df = bq.query(f"""
        SELECT * FROM `{gcp_project}.dev_ai_datasets.deal_features`
    """).to_dataframe()

    print(f"loaded {len(df)} rows from deal_features")

    if len(df) == 0:
        return {"error": "deal_features table is empty — run dbt first"}, 400

    # -------------------------------------------------------------------------
    # prepare features and label
    # -------------------------------------------------------------------------
    feature_cols = [c for c in df.columns if c not in ID_COLS]
    X = df[feature_cols].fillna(0)
    y = df["is_won"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"train={len(X_train)} | test={len(X_test)} | features={len(feature_cols)} | positive_rate={y.mean():.3f}")

    # -------------------------------------------------------------------------
    # train
    # -------------------------------------------------------------------------
    if algorithm == "logistic_regression":
        model = LogisticRegression(**hyperparams, random_state=42)
    elif algorithm == "random_forest":
        model = RandomForestClassifier(**hyperparams, random_state=42)
    elif algorithm == "gradient_boosting":
        model = GradientBoostingClassifier(**hyperparams, random_state=42)
    else:
        return {"error": f"unknown algorithm: {algorithm}"}, 400

    model.fit(X_train, y_train)

    # -------------------------------------------------------------------------
    # evaluate
    # -------------------------------------------------------------------------
    y_pred      = model.predict(X_test)
    y_prob      = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy":   round(float(accuracy_score(y_test, y_pred)), 4),
        "auc":        round(float(roc_auc_score(y_test, y_prob)), 4),
        "f1":         round(float(f1_score(y_test, y_pred)), 4),
        "test_count": int(len(y_test)),
        "positive_rate": round(float(y.mean()), 4),
    }

    print(f"metrics: {metrics}")

    # -------------------------------------------------------------------------
    # save artifact to GCS
    # -------------------------------------------------------------------------
    model_bytes = io.BytesIO()
    joblib.dump(model, model_bytes)
    model_bytes.seek(0)

    gcs_path = f"models/model_id={model_id}/dataset_id={dataset_id}/run_id={run_id}/model.pkl"
    storage.Client().bucket(GCS_BUCKET).blob(gcs_path).upload_from_file(
        model_bytes, content_type="application/octet-stream"
    )
    artifact = f"gs://{GCS_BUCKET}/{gcs_path}"
    print(f"artifact saved: {artifact}")

    return {
        "run_id":    run_id,
        "algorithm": algorithm,
        "artifact":  artifact,
        "metrics":   metrics,
    }, 200
