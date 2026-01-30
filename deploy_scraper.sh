#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
STACK="VersoStat-ScraperServiceStack-prod"
IMAGE_TAG="${IMAGE_TAG:-latest}"
SPORTMONKS_SECRET_ID="${SPORTMONKS_SECRET_ID:-versostat/sportmonks-api-key}"

echo "Region:        $REGION"
echo "Image tag:     $IMAGE_TAG"
echo "Service stack: $STACK"

# 1) Get ECR repo URI from CFN export
REPO_URI=$(aws cloudformation list-exports --region "$REGION" \
  --query "Exports[?Name=='VersoStat-PyscraperEcrRepositoryUri'].Value" --output text)
if [[ -z "$REPO_URI" || "$REPO_URI" == "None" ]]; then
  echo "ERROR: Could not resolve VersoStat-PyscraperEcrRepositoryUri export."
  echo "       Deploy ScraperPlatformStack first: cd ../versostat-infra && npx cdk deploy VersoStat-ScraperPlatformStack-prod"
  exit 2
fi
echo "ECR repo URI:  $REPO_URI"

# 2) Build for linux/amd64 (so Fargate can run it) and LOAD the image locally
if ! docker buildx ls >/dev/null 2>&1; then
  docker buildx create --use >/dev/null
fi
docker buildx build --platform linux/amd64 -t "$REPO_URI:$IMAGE_TAG" --load .

# 3) Login + push
aws ecr get-login-password --region "$REGION" \
| docker login --username AWS --password-stdin "$(echo "$REPO_URI" | awk -F/ '{print $1}')"
docker push "$REPO_URI:$IMAGE_TAG"

# 4) (Optional) Deploy the ScraperServiceStack if CDK infra changed
# If you didn't change CDK infra, you can comment this out to save time.
SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SPORTMONKS_SECRET_ID" \
  --region "$REGION" --query ARN --output text 2>/dev/null || true)
if [[ -z "$SECRET_ARN" ]]; then
  echo "WARN: Could not resolve secret $SPORTMONKS_SECRET_ID; skipping CDK deploy."
  echo "      Create the secret first, then re-run to deploy the stack."
else
  cdk deploy "$STACK" \
    -c sportmonksSecretArn="$SECRET_ARN" \
    -c imageTag="$IMAGE_TAG" \
    --require-approval never
fi

echo "Deployed image: $REPO_URI:$IMAGE_TAG"
echo "Next Step Function run (scheduled or manual) will use this image."
