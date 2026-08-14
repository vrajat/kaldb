#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-local-s3mock}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/kaldb-nrt-snapshot/${TIMESTAMP}}"
LOCAL_CONTAINER_NAME="${LOCAL_CONTAINER_NAME:-kaldb-nrt-s3mock}"
LOCAL_BUCKET="${LOCAL_BUCKET:-test-nrt-s3-bucket}"
LOCAL_ENDPOINT="${LOCAL_ENDPOINT:-http://127.0.0.1:9090}"
LOCAL_REGION="${LOCAL_REGION:-us-east-1}"
LOCAL_ACCESS_KEY="${LOCAL_ACCESS_KEY:-access}"
LOCAL_SECRET_KEY="${LOCAL_SECRET_KEY:-secret}"

mkdir -p "${ARTIFACT_DIR}"

echo "artifact_dir=${ARTIFACT_DIR}"

cleanup_local() {
  docker logs "${LOCAL_CONTAINER_NAME}" >"${ARTIFACT_DIR}/s3mock.log" 2>&1 || true
  docker rm -f "${LOCAL_CONTAINER_NAME}" >/dev/null 2>&1 || true
}

run_local() {
  trap cleanup_local EXIT

  docker rm -f "${LOCAL_CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "${LOCAL_CONTAINER_NAME}" \
    -p 9090:9090 \
    -e "initialBuckets=${LOCAL_BUCKET}" \
    adobe/s3mock:2.1.29 >"${ARTIFACT_DIR}/docker-run.txt"

  export AWS_REGION="${LOCAL_REGION}"
  export AWS_ACCESS_KEY_ID="${LOCAL_ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${LOCAL_SECRET_KEY}"
  export NRT_S3_SMOKE_BUCKET="${LOCAL_BUCKET}"
  export NRT_S3_SMOKE_REGION="${LOCAL_REGION}"
  export NRT_S3_SMOKE_ENDPOINT="${LOCAL_ENDPOINT}"
  export NRT_S3_SMOKE_ACCESS_KEY="${LOCAL_ACCESS_KEY}"
  export NRT_S3_SMOKE_SECRET_KEY="${LOCAL_SECRET_KEY}"
  export NRT_S3_SMOKE_PREFIX="local/${TIMESTAMP}"

  aws --endpoint-url "${LOCAL_ENDPOINT}" s3api list-buckets >"${ARTIFACT_DIR}/local-buckets.json"

  mvn -pl astra \
    -Dtest=AstraIndexerTest#testIndexerPublishesLiveNrtSnapshotToS3Mock,NrtSnapshotPublisherTest#testConfiguredS3SmokePublishRoundTrip \
    test | tee "${ARTIFACT_DIR}/mvn-local.log"

  aws --endpoint-url "${LOCAL_ENDPOINT}" s3api list-objects-v2 \
    --bucket "${LOCAL_BUCKET}" \
    --prefix "${NRT_S3_SMOKE_PREFIX}/nrt/" >"${ARTIFACT_DIR}/local-s3-objects.json"
}

run_aws() {
  : "${NRT_S3_SMOKE_BUCKET:?set NRT_S3_SMOKE_BUCKET}"
  export NRT_S3_SMOKE_REGION="${NRT_S3_SMOKE_REGION:-us-east-1}"
  export NRT_S3_SMOKE_PREFIX="${NRT_S3_SMOKE_PREFIX:-aws/${TIMESTAMP}}"
  export AWS_REGION="${NRT_S3_SMOKE_REGION}"

  aws sts get-caller-identity >"${ARTIFACT_DIR}/aws-sts.json"
  aws s3api head-bucket \
    --bucket "${NRT_S3_SMOKE_BUCKET}" \
    --region "${NRT_S3_SMOKE_REGION}" >"${ARTIFACT_DIR}/aws-head-bucket.json"

  mvn -pl astra \
    -Dtest=NrtSnapshotPublisherTest#testConfiguredS3SmokePublishRoundTrip \
    test | tee "${ARTIFACT_DIR}/mvn-aws.log"

  aws s3api list-objects-v2 \
    --bucket "${NRT_S3_SMOKE_BUCKET}" \
    --prefix "${NRT_S3_SMOKE_PREFIX}/nrt/" >"${ARTIFACT_DIR}/aws-s3-objects.json"
}

case "${MODE}" in
  local-s3mock)
    run_local
    ;;
  aws-s3)
    run_aws
    ;;
  *)
    echo "usage: $0 [local-s3mock|aws-s3]" >&2
    exit 2
    ;;
esac

if [ -d "astra/target/surefire-reports" ]; then
  rm -rf "${ARTIFACT_DIR}/surefire-reports"
  cp -R "astra/target/surefire-reports" "${ARTIFACT_DIR}/surefire-reports"
fi
