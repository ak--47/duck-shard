#!/bin/bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-duck-shard-api}"
MEMORY="${MEMORY:-4Gi}"
CPU="${CPU:-2}"
TIMEOUT="${TIMEOUT:-900}"
MAX_INSTANCES="${MAX_INSTANCES:-10}"

echo -e "${BLUE}🦆 Duck Shard API Deployment Script${NC}"
echo "======================================"

# Load environment variables from .env file (if present)
if [ -f .env ]; then
  echo -e "${YELLOW}Loading environment variables from .env file...${NC}"
  set -a
  source .env
  set +a
fi

# Ensure GCP_PROJECT_ID is set
if [ -z "${GCP_PROJECT_ID:-}" ]; then
  echo -e "${RED}ERROR: GCP_PROJECT_ID is not set.${NC}"
  echo "Please set it in your .env file or environment."
  exit 1
fi

echo -e "${GREEN}Using GCP Project: ${GCP_PROJECT_ID}${NC}"
echo -e "${GREEN}Service Name: ${SERVICE_NAME}${NC}"
echo -e "${GREEN}Region: ${REGION}${NC}"

# Build Docker image
echo -e "\n${BLUE}Building Docker image...${NC}"
gcloud builds submit --tag gcr.io/"$GCP_PROJECT_ID"/"$SERVICE_NAME" \
  --project="$GCP_PROJECT_ID"

# Compose --set-env-vars list for Cloud Run
echo -e "\n${BLUE}Preparing environment variables...${NC}"
ENV_VARS=""
for var in GCS_KEY_ID GCS_SECRET S3_KEY_ID S3_SECRET; do
  value="${!var:-}"
  if [ -n "$value" ]; then
    ENV_VARS="${ENV_VARS}${var}=${value},"
    echo -e "${GREEN}✓ ${var} is set${NC}"
  else
    echo -e "${YELLOW}⚠ ${var} is not set (cloud storage may not work)${NC}"
  fi
done
# Remove trailing comma
ENV_VARS="${ENV_VARS%,}"

# Deploy to Cloud Run
echo -e "\n${BLUE}Deploying to Cloud Run...${NC}"
DEPLOY_CMD="gcloud run deploy $SERVICE_NAME \
  --image=gcr.io/$GCP_PROJECT_ID/$SERVICE_NAME \
  --platform=managed \
  --region=$REGION \
  --allow-unauthenticated \
  --memory=$MEMORY \
  --cpu=$CPU \
  --timeout=$TIMEOUT \
  --max-instances=$MAX_INSTANCES \
  --port=8080 \
  --project=$GCP_PROJECT_ID"

# Add environment variables if any are set
if [ -n "$ENV_VARS" ]; then
  DEPLOY_CMD="$DEPLOY_CMD --set-env-vars=\"$ENV_VARS\""
fi

# Execute deployment
eval $DEPLOY_CMD

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region=$REGION \
  --project=$GCP_PROJECT_ID \
  --format="value(status.url)")

echo -e "\n${GREEN}✅ Deployment completed successfully!${NC}"
echo "======================================"
echo -e "${GREEN}Service URL: ${SERVICE_URL}${NC}"
echo -e "${GREEN}Health Check: ${SERVICE_URL}/health${NC}"
echo -e "${GREEN}API Documentation: ${SERVICE_URL}/${NC}"
echo ""
echo "Example usage:"
echo -e "${BLUE}curl -X POST ${SERVICE_URL}/run \\${NC}"
echo -e "${BLUE}  -H \"Content-Type: application/json\" \\${NC}"
echo -e "${BLUE}  -d '{${NC}"
echo -e "${BLUE}    \"input_path\": \"gs://your-bucket/data.parquet\",${NC}"
echo -e "${BLUE}    \"format\": \"ndjson\",${NC}"
echo -e "${BLUE}    \"output\": \"gs://your-bucket/output/\",${NC}"
echo -e "${BLUE}    \"jq\": \".user_id = (.user_id | tonumber)\",${NC}"
echo -e "${BLUE}    \"preview\": 10${NC}"
echo -e "${BLUE}  }'${NC}"
echo ""
