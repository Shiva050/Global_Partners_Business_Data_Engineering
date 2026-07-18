# Running the Pipeline on AWS Glue

Executes bronze → silver → gold on **AWS Glue** (serverless Spark). Our jobs are
plain PySpark (no Glue-specific API), so Glue just runs them as Spark scripts —
which keeps them portable and unit-testable.

```
S3 snapshots ─► bronze/snapshot_diff ─► cdc/ ─► silver/current_state ─┐
                                                 silver/dim_date ──────┤
                                                                        ├─► silver/order_facts ─► gold/* (8 metrics)
```

## Why Glue (vs EMR)
- **Serverless** — no cluster to size or keep running; pay per job-second.
- **AWS-native**, no extra licenses (satisfies the constraint), no Snowflake/DBT.
- **Glue 5.0 = Spark 3.5 / Python 3.11**, matching our `pyspark==3.5.1` pin, so
  local unit tests and Glue run the same engine.

---

## Prerequisites
- Bronze bucket `dms-global-partne-brusiness-bronze` has the DMS snapshots under
  `snapshots/dt=2026-07-16/gpb/<table>/` (already done).
- AWS CLI authenticated; `zip` installed.
- Edit [config.env](config.env) if any names differ, then `source config.env`.

---

## Step 1 — Glue IAM service role

Glue assumes a role to read/write S3 and write logs. Create it once:

```bash
source config.env
aws iam create-role --role-name "$GLUE_ROLE_NAME" \
  --assume-role-policy-document file://iam/glue-trust-policy.json

# AWS-managed baseline for Glue (CloudWatch logs, Glue APIs)
aws iam attach-role-policy --role-name "$GLUE_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Our bucket access (read snapshots, write cdc/silver/gold, read scripts)
aws iam put-role-policy --role-name "$GLUE_ROLE_NAME" \
  --policy-name gpb-glue-s3 \
  --policy-document file://iam/glue-permissions-policy.json
```

**Console equivalent:** IAM → Roles → Create role → trusted entity *Glue* →
attach `AWSGlueServiceRole` → add an inline policy from
`iam/glue-permissions-policy.json`.

## Step 2 — Package & upload the code

```bash
bash package_jobs.sh
```
This zips the `jobs/` package to `s3://.../glue/jobs.zip` (passed to every job as
`--extra-py-files`, so `import jobs.common.config` resolves) and syncs each entry
script to `s3://.../glue/scripts/jobs/...`. **Re-run after any code change.**

## Step 3 — Create the Glue jobs

```bash
bash create_jobs.sh          # creates/updates all 12 jobs
```
Each job is set to Glue 5.0, `G.1X` × 2 workers, with default args
`--bronze` and `--extra-py-files`. The CLV job also gets `--full` for the
initial backfill.

**Console equivalent (per job):** Glue → ETL jobs → Script editor →
*Spark* → point at the S3 script → Job details: role = `gpb-glue-service-role`,
Glue 5.0, G.1X, 2 workers → Advanced → Job parameters: add `--bronze` =
`s3://dms-global-partne-brusiness-bronze` and `--extra-py-files` =
`s3://.../glue/jobs.zip`.

## Step 4 — Run in dependency order

```bash
bash run_pipeline.sh         # runs bronze -> silver -> gold, waiting on each
```
Or trigger jobs individually in the console in this order:
1. `gpb-bronze-snapshot-diff`
2. `gpb-silver-current-state`, `gpb-silver-dim-date`
3. `gpb-silver-order-facts`
4. the eight `gpb-gold-*` jobs

## Step 5 — Validate outputs

```bash
aws s3 ls s3://$BRONZE_BUCKET/cdc/ --recursive | head
aws s3 ls s3://$BRONZE_BUCKET/silver/ --recursive | head
aws s3 ls s3://$BRONZE_BUCKET/gold/ --recursive | head
```
Reconcile: `silver/current/order_items` ≈ 203,519 rows;
`silver/facts/orders` = one row per order; `gold/customer_clv_daily` dense per
customer. (Quick row counts: run an Athena query or a tiny Glue/`spark` read.)

---

## Incremental CLV (later runs)
The first CLV run uses `--full`. For a subsequent batch, run the job with
`--full` removed and `--batch-min-order-date <YYYY-MM-DD>` (the min order date of
the new batch) so only affected month partitions are recomputed — see
[../jobs/gold/customer_clv_daily.py](../jobs/gold/customer_clv_daily.py).

## Cost & cleanup
- Glue bills per **DPU-second** (G.1X = 1 DPU/worker) while a job runs. These
  jobs are seconds-to-minutes on this data — a few cents per full pipeline run.
- Nothing runs idle (serverless) — no teardown needed between runs.
- To remove: `aws glue delete-job --job-name <name>` for each, and detach/delete
  the IAM role when done with the project.
