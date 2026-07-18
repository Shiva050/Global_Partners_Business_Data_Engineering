#!/usr/bin/env bash
# Create (or update) one Glue job per pipeline script.
# Usage:  cd glue && source config.env && bash create_jobs.sh
set -euo pipefail
: "${GLUE_ROLE_ARN:?source config.env first}"

# name | script path (under jobs/) | extra default-args (JSON fragment, or "")
JOBS=(
  "gpb-bronze-snapshot-diff|bronze/snapshot_diff.py|"
  "gpb-silver-current-state|silver/current_state.py|"
  "gpb-silver-dim-date|silver/dim_date.py|"
  "gpb-silver-order-facts|silver/order_facts.py|"
  "gpb-gold-clv-daily|gold/customer_clv_daily.py|,\"--full\":\"true\""
  "gpb-gold-rfm|gold/customer_rfm.py|"
  "gpb-gold-clv-tiers|gold/customer_clv_tiers.py|"
  "gpb-gold-churn|gold/churn_indicators.py|"
  "gpb-gold-sales-trends|gold/sales_trends.py|"
  "gpb-gold-loyalty|gold/loyalty_impact.py|"
  "gpb-gold-location|gold/location_performance.py|"
  "gpb-gold-discount|gold/discount_effectiveness.py|"
  "gpb-gold-clv-trends|gold/clv_trends.py|"
)

for entry in "${JOBS[@]}"; do
  IFS='|' read -r NAME SCRIPT EXTRA <<< "$entry"
  SCRIPT_LOC="${GLUE_S3}/scripts/jobs/${SCRIPT}"
  DEFAULT_ARGS="{\"--bronze\":\"${BRONZE_S3}\",\"--extra-py-files\":\"${GLUE_S3}/jobs.zip\",\"--enable-metrics\":\"true\",\"--job-language\":\"python\"${EXTRA}}"
  COMMAND="{\"Name\":\"glueetl\",\"ScriptLocation\":\"${SCRIPT_LOC}\",\"PythonVersion\":\"3\"}"

  echo "==> ${NAME}  (${SCRIPT})"
  if aws glue get-job --job-name "$NAME" >/dev/null 2>&1; then
    aws glue update-job --job-name "$NAME" --job-update \
      "Role=${GLUE_ROLE_ARN},GlueVersion=${GLUE_VERSION},WorkerType=${GLUE_WORKER_TYPE},NumberOfWorkers=${GLUE_NUM_WORKERS},Command=${COMMAND},DefaultArguments=${DEFAULT_ARGS},MaxRetries=0" >/dev/null
  else
    aws glue create-job --name "$NAME" --role "${GLUE_ROLE_ARN}" \
      --glue-version "${GLUE_VERSION}" --worker-type "${GLUE_WORKER_TYPE}" \
      --number-of-workers "${GLUE_NUM_WORKERS}" --max-retries 0 \
      --command "${COMMAND}" --default-arguments "${DEFAULT_ARGS}" >/dev/null
  fi
done
echo "All jobs created/updated."
