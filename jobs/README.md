# PySpark Jobs

Flat, layer-prefixed job modules. The folder is a package (`jobs/`) so tests
import `jobs.<module>`; each file also runs standalone via `spark-submit` (it
falls back to a flat `import <module>` when the package root isn't on the path).

| Module | Layer | Purpose |
|---|---|---|
| [`cdc_config.py`](cdc_config.py) | — | table specs (natural keys), DMS/CDC column contracts |
| [`cdc_snapshot_diff.py`](cdc_snapshot_diff.py) | bronze→cdc | derive the I/U/D change log by diffing snapshots |
| [`silver_current_state.py`](silver_current_state.py) | cdc→silver | collapse the change log to current-state tables |

---

## `cdc_snapshot_diff.py` — snapshot-diff CDC

Derives an append-only **I/U/D change log** from consecutive DMS full-load
snapshots (the CDC mechanism, since the source runs SQL Server **Express** with
no MS-CDC — see [../ingestion/README.md](../ingestion/README.md)).

```
snapshots/dt=<prev>/gpb/<table>/  ┐
                                  ├─►  compute_cdc()  ─►  cdc/<table>/dt=<cur>/
snapshots/dt=<cur>/gpb/<table>/   ┘
```

Output carries the source columns plus **DMS-CDC-style headers** — `Op`,
`cdc_seq` (LSN mimic), `cdc_commit_ts`, `cdc_snapshot_dt`, `cdc_processed_at` —
so Silver collapses on `cdc_seq` exactly as against a real DMS/MS-CDC feed. See
**[../docs/cdc_design.md](../docs/cdc_design.md)** for the mechanism, the LSN
mimic, and the expected-vs-actual fidelity contract.

### Keys (verified against the 2026-07-16 snapshot — see [cdc_config.py](cdc_config.py))
| Table | Strategy | Key |
|---|---|---|
| `order_items` | keyed diff (I/U/D) | `order_id`, `lineitem_id` — unique (203,519 rows) |
| `date_dim` | keyed diff (I/U/D) | `date_key` — unique (365 rows) |
| `order_item_options` | **multiset diff (I/D)** | none — 2,299 fully-identical dup rows, so it's a multiset; count-based diff preserves exact multiplicity |

---

## `silver_current_state.py` — collapse CDC to current state

Applies the change log per key with the canonical log-based merge — **latest
`cdc_seq` wins, drop `Op=D` tombstones** — producing one row per current key.

```
cdc/<table>/  ──►  collapse  ──►  silver/current/<table>/
```

- Keyed tables collapse by natural key (`row_number` over `cdc_seq desc`).
- Keyless `order_item_options` collapses by **net count** over the full history
  (Σ +1 for I, -1 for D per distinct row).

Swapping in a real DMS/MS-CDC feed later requires **no change here** — Silver
only depends on the change-log contract (`Op` + `cdc_seq` + source columns).

---

## Run

```bash
# CDC (auto-detects the latest two snapshot dates)
spark-submit jobs/cdc_snapshot_diff.py --bronze s3://dms-global-partne-brusiness-bronze

# CDC pinned to specific snapshots / a subset of tables
spark-submit jobs/cdc_snapshot_diff.py \
  --bronze s3://dms-global-partne-brusiness-bronze \
  --snapshot-date 2026-07-16 --prev-date 2026-07-15 \
  --tables order_items,order_item_options

# Silver current state
spark-submit jobs/silver_current_state.py --bronze s3://dms-global-partne-brusiness-bronze
```

On **AWS Glue**, use the module as the job script and pass `--bronze` (and other
flags) as job parameters.

## Test

```bash
pip install -r requirements-dev.txt
pytest tests/ -v          # local SparkSession, no AWS needed
```

CI runs the full suite on every push (`.github/workflows/ci.yml`).
