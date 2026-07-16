# Ingestion — SQL Server (RDS) → S3 Bronze (Snapshot + PySpark-derived CDC)

Layer 1 of the pipeline. DMS **full-load** lands periodic full snapshots of the 3
source tables into S3; a **PySpark snapshot-diff job** then derives an append-only
I/U/D change log by comparing consecutive snapshots.

```
RDS SQL Server (Express) ──► DMS full-load ──► S3  snapshots/dt=<date>/gpb/<table>/   (Op=I + ingested_at)
   order_items                                       │
   order_item_options              PySpark snapshot-diff (full-outer join on PK + row hash)
   date_dim                                          ▼
                                              S3  cdc/<table>/dt=<date>/             (Op = I/U/D + ingested_at)
```

## Why not DMS CDC (the architecture pivot)
The original design used DMS `full-load-and-cdc` (MS-CDC). **The source RDS runs
SQL Server Express, which does not support CDC** (edition-gated; Express also can't
be a replication publisher, ruling out MS-Replication too). Rather than pay for a
Standard-edition instance (~$0.50/hr), we keep Express (free) and **derive** the
I/U/D change log in PySpark by diffing snapshots — which also satisfies the
"all logic in PySpark" constraint. Trade-off: batch-granular, not row-level
real-time CDC. See [Deployment notes](#deployment-notes--decisions-as-built).

## Provisioned (no action)
- ✅ RDS SQL Server **Express** with the 3 tables (`gpb` schema), DB `GlobalPartnerBusiness`
- ✅ S3 bronze bucket (SSE-S3 / AES256)
- ✅ S3 gateway VPC endpoint (DMS Serverless → S3 network path)
- ✅ DMS S3-target IAM role — policy reference: [iam/](iam/)
- ✅ DMS source (SQL Server) + target (S3) endpoints
- ✅ DMS Serverless replication config `gpb-fullload-bronze` (type `full-load`)

## Pipeline
1. **Snapshot** — run `gpb-fullload-bronze` → writes to `snapshots/dt=<date>/gpb/<table>/`
   (bump `BucketFolder` date on the S3 endpoint per run)
2. **Diff** — PySpark job compares latest vs. prior snapshot → `cdc/<table>/dt=<date>/`
3. Validate + reconcile counts (203,519 / 193,017)

---

---
> ⚠️ **Legacy (superseded):** the console steps below documented the original
> DMS `full-load-and-cdc` attempt. They remain as reference, but the live config
> is now `full-load` only (see the pivot section above). Settings that don't
> apply to full-load — `DatePartitionEnabled`, `CdcPath` — were removed from the
> deployed S3 endpoint. This section will be replaced when the diff job lands.
---

## Step 1 — Create S3 target endpoint

DMS Console → **Endpoints** → *Create endpoint* → **Target**, engine **Amazon S3**.
- Service access role ARN: your existing DMS S3-target role
- Bucket name: your bronze bucket
- Bucket folder: `cdc`

In **Endpoint settings** switch to the **JSON editor** and paste (fill the 3 values):

```json
{
  "ServiceAccessRoleArn": "arn:aws:iam::<ACCOUNT_ID>:role/<DMS_S3_ROLE_NAME>",
  "BucketName": "<BRONZE_BUCKET>",
  "BucketFolder": "cdc",
  "DataFormat": "parquet",
  "ParquetVersion": "parquet-2-0",
  "ParquetTimestampInMillisecond": true,
  "EncryptionMode": "SSE_KMS",
  "ServerSideEncryptionKmsKeyId": "<KMS_KEY_ARN>",
  "IncludeOpForFullLoad": true,
  "AddColumnName": true,
  "TimestampColumnName": "ingested_at",
  "DatePartitionEnabled": true,
  "DatePartitionSequence": "YYYYMMDD",
  "DatePartitionDelimiter": "SLASH",
  "CdcMaxBatchInterval": 60,
  "CdcMinFileSize": 32000,
  "CdcPath": "cdc-changes"
}
```

Why these matter (the requirements → settings map):

| Requirement | Setting |
|---|---|
| Unified I/U/D flag across full load + CDC | `IncludeOpForFullLoad: true` → `Op` column; full-load rows = `I` |
| `ingested_at` for batch partitioning | `TimestampColumnName: "ingested_at"` |
| Date-partitioned change log | `DatePartitionEnabled` + `YYYYMMDD` + `SLASH` |
| Immutable / append-only | `CdcPath: cdc-changes` keeps ongoing changes separate from the initial snapshot; bucket object-lock enforces immutability |
| Columnar, downstream-friendly for PySpark | `DataFormat: parquet` |

Annotated source-of-truth: [dms/s3-target-settings.json](dms/s3-target-settings.json)

---

## Step 2 — Create replication task

DMS Console → **Database migration tasks** → *Create task*.
- Replication instance: your existing instance
- Source endpoint: your existing SQL Server source
- Target endpoint: the S3 target from Step 1
- Migration type: **Migrate existing data and replicate ongoing changes**
  (`full-load-and-cdc`)

**Table mappings** → JSON editor → paste [dms/table-mappings.json](dms/table-mappings.json)
(selects `dbo.order_items`, `dbo.order_item_options`, `dbo.date_dim`).

**Task settings** → JSON editor → paste [dms/task-settings.json](dms/task-settings.json).
Key choices: `TargetTablePrepMode: DO_NOTHING` (never truncate S3 — append only),
`RecoverableErrorCount: -1` (auto-resume from checkpoint on transient failure =
the failure/reload resilience requirement).

Start task at creation, or start it manually after testing connections.

---

## Step 3 — Validate output

Expected S3 layout after the task runs:

```
s3://<bronze>/cdc/dbo/order_items/LOAD00000001.parquet          # initial snapshot
s3://<bronze>/cdc/cdc-changes/dbo/order_items/2026/07/15/*.parquet  # ongoing CDC
```

Every Parquet row carries: original columns + `Op` (`I`/`U`/`D`) + `ingested_at`.

Sanity checks:
```bash
aws s3 ls s3://<bronze>/cdc/ --recursive | head
# row counts should reconcile with source after full load:
#   SELECT COUNT(*) FROM dbo.order_items;   -- expect 203,519
#   SELECT COUNT(*) FROM dbo.order_item_options; -- expect 193,017
```
Then INSERT/UPDATE/DELETE a test row in SQL Server and confirm a new CDC Parquet
file appears under the date partition with the matching `Op`.

---

## Deployment notes / decisions (as built)

Running on **DMS Serverless** (replication-config, not a classic instance).

- **S3 Gateway VPC endpoint** (`com.amazonaws.us-east-1.s3`) was required.
  DMS Serverless ENIs have no public IP, so in the default VPC they had no route
  to S3 and the target test failed with a misleading *"Failed to connect to
  database"*. Attaching a gateway endpoint to the main route table
  (`rtb-...`) fixed it. **No NAT / internet gateway cost** — gateway endpoints
  are free and keep S3 traffic on the AWS backbone.
- **Encryption: SSE-S3 (AES256)**, matching the bucket's default encryption.
  KMS was intentionally avoided — it added no security benefit here and only
  introduced key-policy failure modes for the DMS role.
- **Immutability** is enforced logically: DMS `TargetTablePrepMode = DO_NOTHING`
  never rewrites bronze, so every run only appends. Object Lock (WORM) would
  require recreating the bucket (can only be enabled at creation); deferred as
  optional hardening. Recommended add-on: enable versioning + a bucket policy
  denying `DeleteObject` to non-DMS principals.
- **Source must be SQL Server Standard/Enterprise, NOT Express.** CDC (MS-CDC)
  is edition-gated — Express cannot enable it (`sp_cdc_enable_db` fails), and
  Express can't be a replication publisher either, so the MS-Replication path is
  also unavailable. RDS does not allow changing edition in place, so the source
  runs on a `sqlserver-se` instance (`db.m5.large`, the smallest class Standard
  supports). DB name `GlobalPartnerBusiness`, schema `gpb`.
