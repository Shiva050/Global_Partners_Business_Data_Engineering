#!/usr/bin/env bash
# Run the full pipeline in dependency order, waiting for each job to finish.
# Usage:  cd glue && source config.env && bash run_pipeline.sh
set -euo pipefail
: "${AWS_REGION:?source config.env first}"

run_and_wait() {
  local name="$1"
  echo "==> starting $name"
  local rid
  rid=$(aws glue start-job-run --job-name "$name" --query JobRunId --output text)
  while true; do
    local st
    st=$(aws glue get-job-run --job-name "$name" --run-id "$rid" \
         --query 'JobRun.JobRunState' --output text)
    case "$st" in
      SUCCEEDED) echo "    $name: SUCCEEDED"; break ;;
      FAILED|ERROR|TIMEOUT|STOPPED)
        echo "    $name: $st"
        aws glue get-job-run --job-name "$name" --run-id "$rid" \
          --query 'JobRun.ErrorMessage' --output text
        exit 1 ;;
      *) sleep 20 ;;
    esac
  done
}

# 1) bronze: snapshots -> CDC change log
run_and_wait gpb-bronze-snapshot-diff

# 2) silver: current-state (CDC collapse) + static date dim
run_and_wait gpb-silver-current-state
run_and_wait gpb-silver-dim-date

# 3) silver: revenue facts (needs current-state tables)
run_and_wait gpb-silver-order-facts

# 4) gold: all metrics (each reads silver facts)
for g in gpb-gold-clv-daily gpb-gold-rfm gpb-gold-clv-tiers gpb-gold-churn \
         gpb-gold-sales-trends gpb-gold-loyalty gpb-gold-location gpb-gold-discount; do
  run_and_wait "$g"
done

echo "Pipeline complete."
