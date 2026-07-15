#!/bin/bash
set -euo pipefail

BUCKETS=("telemetry-bronze")

for bucket in "${BUCKETS[@]}"; do
  if awslocal s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    echo "[init] bucket $bucket already exists"
  else
    awslocal s3 mb "s3://$bucket"
    echo "[init] created bucket $bucket"
  fi
done
