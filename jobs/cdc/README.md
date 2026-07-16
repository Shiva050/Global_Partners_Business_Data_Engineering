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
source columns plus **DMS-CDC-style header columns** — `Op`, `cdc_seq` (an
LSN mimic), `cdc_commit_ts`, `cdc_snapshot_dt`, `cdc_processed_at` — so Silver
collapses on `cdc_seq` exactly as it would against a real DMS/MS-CDC feed.
See **[../../docs/cdc_design.md](../../docs/cdc_design.md)** for the log-based
mechanism, the LSN mimic, and the expected-vs-actual fidelity contract.

DMS metadata columns (`Op`, `ingested_at`) on the snapshots are **excluded** from
the hash and re-derived here.

## Keys (verified against the 2026-07-16 snapshot — see [config.py](config.py))
| Table | Strategy | Key |
|---|---|---|
| `order_items` | keyed diff (I/U/D) | `order_id`, `lineitem_id` — unique (203,519 rows) |
| `date_dim` | keyed diff (I/U/D) | `date_key` — unique (365 rows) |
| `order_item_options` | **multiset diff (I/D)** | none — see below |

### Why `order_item_options` is a multiset
It has **no unique key**: 2,299 rows are fully-identical duplicates (same
order/lineitem/group/name/price/quantity). A keyed join would explode on those.
Instead, `keys=None` triggers a **count-based diff** — group by the full row,
compare counts between snapshots, and emit the net `I`/`D` with exact
multiplicity preserved (multiplicity matters: revenue sums `option_price *
option_quantity` across rows). There is no `U` for a keyless row — an in-place
change is a delete of the old row + insert of the new.

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
