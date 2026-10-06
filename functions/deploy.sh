#!/bin/bash
# Deploy Cloud Functions for the AutoElite pipeline.
# Run from the project root: bash functions/deploy.sh

PROJECT="btibert-ba882-fall26"
REGION="us-central1"
# your service account here
# example:
# SA="{your-alias-here}-ba882-fall26@${PROJECT}.iam.gserviceaccount.com"
# you can get your service account from IAM on your project
SA="in-class-project@btibert-ba882-fall26.iam.gserviceaccount.com"
STAGE_BUCKET="${PROJECT}-functions"
GCS_BUCKET="qst-btibert-882-202609-inclass-proj"    # we created this earlier in the semester

gcloud config set project $PROJECT

# create the stage bucket if it doesn't exist
if ! gsutil ls gs://$STAGE_BUCKET &>/dev/null; then
    echo "creating bucket gs://$STAGE_BUCKET"
    gsutil mb -l $REGION gs://$STAGE_BUCKET
fi

echo "======================================================"
echo "deploying weather-extract"
echo "======================================================"

gcloud functions deploy weather-extract \
    --gen2 \
    --runtime python312 \
    --trigger-http \
    --entry-point task \
    --source ./functions/weather-extract \
    --stage-bucket $STAGE_BUCKET \
    --service-account $SA \
    --region $REGION \
    --allow-unauthenticated \
    --memory 256MB \
    --timeout 60s \
    --set-env-vars GCS_BUCKET=$GCS_BUCKET

echo "done"
