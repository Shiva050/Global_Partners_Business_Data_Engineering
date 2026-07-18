#!/usr/bin/env bash
# Package the jobs/ PySpark package and upload to S3 for Glue.
#   - jobs.zip           -> shared modules, passed to every job via --extra-py-files
#                           (so `import jobs.common.config` resolves at runtime)
#   - scripts/jobs/...    -> each entry script, used as a Glue job ScriptLocation
#
# Usage:  cd glue && source config.env && bash package_jobs.sh
set -euo pipefail
: "${GLUE_S3:?source config.env first}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Building jobs.zip (jobs/ package)"
rm -f jobs.zip
zip -rq jobs.zip jobs -x '*__pycache__*' -x '*.pyc'

echo "==> Uploading jobs.zip -> ${GLUE_S3}/jobs.zip"
aws s3 cp jobs.zip "${GLUE_S3}/jobs.zip"

echo "==> Syncing entry scripts -> ${GLUE_S3}/scripts/jobs/"
aws s3 sync jobs/ "${GLUE_S3}/scripts/jobs/" \
  --exclude '*__pycache__*' --exclude '*.pyc' --delete

rm -f jobs.zip
echo "Done. Re-run this whenever you change any job code."
