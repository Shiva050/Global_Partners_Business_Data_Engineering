# PySpark Jobs

Organized by medallion layer. `jobs/` and each layer are packages, so tests
import `jobs.<layer>.<module>`; each file also runs standalone via
`spark-submit` (with the package shipped via `--py-files`).

```
jobs/
  common/config.py          table specs (keys, static flag), DMS/CDC contracts
  bronze/snapshot_diff.py    bronze snapshots -> I/U/D change log
  silver/current_state.py    change log -> current-state tables (CDC collapse)
  silver/dim_date.py         static date dimension -> typed conformed dim
  silver/order_facts.py      current-state -> revenue-enriched line/order facts
  gold/customer_clv_daily.py order facts -> daily-evolving cumulative CLV
```

| Module | Layer | Purpose |
|---|---|---|
| [`common/config.py`](common/config.py) | shared | table specs (keys, static flag), DMS/CDC column contracts |
| [`bronze/snapshot_diff.py`](bronze/snapshot_diff.py) | bronze→cdc | derive the I/U/D change log by diffing snapshots |
| [`silver/current_state.py`](silver/current_state.py) | cdc→silver | collapse the change log to current-state tables |
| [`silver/dim_date.py`](silver/dim_date.py) | bronze→silver | typed load of the static date dimension (no CDC) |
| [`silver/order_facts.py`](silver/order_facts.py) | silver→silver | revenue-enriched line-item + order facts |
| [`gold/customer_clv_daily.py`](gold/customer_clv_daily.py) | silver→gold | daily-evolving cumulative CLV per customer |
| [`gold/customer_rfm.py`](gold/customer_rfm.py) | silver→gold | RFM scores + segments (VIP/New/Churn Risk) |

**CDC vs static:** `order_items` and `order_item_options` flow through the full
CDC pipeline (`CDC_TABLES`). `date_dim` is a static calendar dimension
(`static=True`) — it skips CDC and is loaded straight to silver with an enforced
typed schema. Running CDC on a dimension that never changes is pure overhead.

---

## `bronze/snapshot_diff.py` — snapshot-diff CDC

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

### Keys (verified against the 2026-07-16 snapshot — see [common/config.py](common/config.py))
| Table | Strategy | Key |
|---|---|---|
| `order_items` | keyed diff (I/U/D) | `order_id`, `lineitem_id` — unique (203,519 rows) |
| `order_item_options` | **multiset diff (I/D)** | none — 2,299 fully-identical dup rows; count-based diff preserves multiplicity |
| `date_dim` | **static** (no CDC) | `date_key` — typed load only (`silver/dim_date.py`) |

## `silver/current_state.py` — collapse CDC to current state

Applies the change log per key with the canonical log-based merge — **latest
`cdc_seq` wins, drop `Op=D` tombstones** — one row per current key. Keyed tables
collapse by natural key; keyless `order_item_options` by **net count** over the
full history. Swapping in a real DMS/MS-CDC feed later needs **no change here**.

## `gold/customer_clv_daily.py` — daily customer CLV

Daily-evolving cumulative CLV per customer (the assessment's primary goal).
Dense per customer from their first order to `as_of`; **month-partitioned**;
**windowed incremental** — a late order recomputes only months `>= floor`
(`trunc(batch_min_order_date,'month')`), seeded by the existing cumulative at
`floor-1` so history isn't recomputed. Emits `cumulative_clv` and
`cumulative_order_count` (the RFM frequency-to-date).

---

## Run

```bash
# Bronze: CDC change log (auto-detects the latest two snapshot dates)
spark-submit jobs/bronze/snapshot_diff.py --bronze s3://dms-global-partne-brusiness-bronze

# Silver: current state (CDC tables) + static date dimension + revenue facts
spark-submit jobs/silver/current_state.py --bronze s3://dms-global-partne-brusiness-bronze
spark-submit jobs/silver/dim_date.py      --bronze s3://dms-global-partne-brusiness-bronze
spark-submit jobs/silver/order_facts.py   --bronze s3://dms-global-partne-brusiness-bronze

# Gold: daily CLV — full backfill, then incremental per batch
spark-submit jobs/gold/customer_clv_daily.py --bronze s3://dms-global-partne-brusiness-bronze --full
spark-submit jobs/gold/customer_clv_daily.py --bronze s3://dms-global-partne-brusiness-bronze \
  --batch-min-order-date 2023-06-01

# Gold: RFM segmentation (full recompute; optional lookback / as-of)
spark-submit jobs/gold/customer_rfm.py --bronze s3://dms-global-partne-brusiness-bronze \
  --lookback-months 12
```

On **AWS Glue**, use the module as the job script and pass `--bronze` (and other
flags) as job parameters.

## Test

```bash
pip install -r requirements-dev.txt
pytest tests/ -v          # local SparkSession, no AWS needed
```

CI runs the full suite on every push (`.github/workflows/ci.yml`).
