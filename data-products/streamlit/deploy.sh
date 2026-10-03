PROJECT_ID=btibert-ba882-fall26
SERVICE_NAME=streamlit-dashboard-demo
REGION=us-central1
REPO=cloud-run-source-deploy
IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE_NAME}
SA=ba882-fall26@${PROJECT_ID}.iam.gserviceaccount.com

gcloud config set project ${PROJECT_ID}

echo "======================================================"
echo "ensure Artifact Registry repo exists"
echo "======================================================"

gcloud artifacts repositories create ${REPO} \
    --repository-format=docker \
    --location=${REGION} \
    --project=${PROJECT_ID} 2>/dev/null || true

echo "======================================================"
echo "build in cloud (Cloud Build)"
echo "======================================================"

gcloud builds submit --tag ${IMAGE} .

echo "======================================================"
echo "deploy to Cloud Run"
echo "======================================================"

gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE} \
    --platform managed \
    --region ${REGION} \
    --allow-unauthenticated \
    --service-account ${SA} \
    --memory 1Gi
