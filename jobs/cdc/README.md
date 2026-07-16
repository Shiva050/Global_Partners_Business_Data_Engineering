# CDC — Snapshot-Diff Change Log

Derives an append-only **I/U/D change log** from consecutive DMS full-load
snapshots. This is the CDC mechanism for the pipeline, since the source runs
SQL Server **Express** (no MS-CDC) — see [../../ingestion/README.md](../../ingestion/README.md).

## How it works

```
snapshots/dt=<prev>/gpb/<table>/  ┐
                                  ├─►  compute_cdc()  ─►  cdc/<table>/dt=<cur>/
snapshots/dt=<cur>/gpb/<table>/   ┘        (full outer join on natural key)
```

Per table, compare current vs. previous snapshot on the natural key, using a
single `sha2` **row hash** over the source columns to detect change:

| Condition | `Op` | Payload emitted |
|---|---|---|
| key only in current | `I` | current row |
| key only in previous | `D` | last-known (previous) row |
| key in both, hash differs | `U` | current row |
| key in both, hash equal | — | dropped (unchanged) |

The **first** snapshot (no prior) is emitted entirely as `I`. Output carries the
source columns + `Op` + `cdc_snapshot_dt` + `cdc_processed_at` — the same shape
DMS CDC would have produced, so downstream (silver, metrics) is agnostic to how
the change log was generated.

DMS metadata columns (`Op`, `ingested_at`) on the snapshots are **excluded** from
the hash and re-derived here.

## Keys (see [config.py](config.py))
| Table | Natural key |
|---|---|
| `order_items` | `order_id`, `lineitem_id` |
| `order_item_options` | `order_id`, `lineitem_id`, `option_group_name`, `option_name` |
| `date_dim` | `date_key` |

> Assumption: these keys are unique within a snapshot. If a snapshot contains
> true duplicate keys the diff over-counts — the job should be extended with a
> pre-dedup / DQ check when that risk is real.

## Run

```bash
# EMR / spark-submit (auto-detects the latest two snapshot dates)
spark-submit jobs/cdc/snapshot_diff.py --bronze s3://dms-global-partne-brusiness-bronze

# Pin specific snapshots / a subset of tables
spark-submit jobs/cdc/snapshot_diff.py \
  --bronze s3://dms-global-partne-brusiness-bronze \
  --snapshot-date 2026-07-16 --prev-date 2026-07-15 \
  --tables order_items,order_item_options
```

On **AWS Glue**, use the same `snapshot_diff.py` as the job script and pass
`--bronze` (and optional dates) as job parameters; the module falls back to a
flat `import config` when the package root isn't on the path.

## Test

```bash
pip install -r requirements-dev.txt
pytest tests/ -v          # runs a local SparkSession, no AWS needed
```

Covered: baseline-all-inserts, insert/update/delete/unchanged, delete-emits-
last-known-row, and NULL-change detection. CI runs these on every push
(`.github/workflows/ci.yml`).
