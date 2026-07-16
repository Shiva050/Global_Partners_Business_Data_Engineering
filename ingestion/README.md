# Ingestion — SQL Server (RDS) → S3 Bronze (Full Load + CDC)

Layer 1 of the pipeline. AWS DMS replicates the 3 source tables from RDS SQL
Server into an **immutable, append-only** S3 bronze layer as an I/U/D change log,
partitioned by ingest date.

```
RDS SQL Server ──(MS-CDC)──► DMS replication instance ──► S3 bronze (Parquet)
   order_items                full-load-and-cdc task        Op(I/U/D) + ingested_at
   order_item_options                                       date-partitioned folders
   date_dim
```

## Already provisioned (no action)
- ✅ RDS SQL Server with the 3 tables loaded
- ✅ MS-CDC enabled on DB + tables — see [sql/01_enable_cdc.sql](sql/01_enable_cdc.sql) (reference)
- ✅ S3 bronze bucket (versioning + SSE-KMS + object-lock)
- ✅ DMS S3-target IAM role — policy reference: [iam/](iam/)
- ✅ DMS replication instance + SQL Server source endpoint

## To build (this doc) — via DMS Console
1. Create the **S3 target endpoint** (paste settings JSON below)
2. Create the **replication task** (paste table-mappings + task-settings)
3. Test connections → start task → validate S3 output

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
