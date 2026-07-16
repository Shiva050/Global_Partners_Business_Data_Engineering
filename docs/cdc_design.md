# CDC Design — Log-Based Mechanism on a Snapshot-Diff Substrate

> **TL;DR** — This project is **built to the log-based CDC pattern** (DMS + SQL
> Server MS-CDC): a change log with an LSN-style ordering key, collapsed to
> current state by "latest sequence wins, drop tombstones." Because the source
> runs SQL Server **Express** (no CDC), the change log is *produced* by diffing
> snapshots instead of reading a transaction log. **The collapse code is
> identical to production; only the fidelity of the input differs.** This doc
> states exactly what is expected vs. what actually happens.

## Why this shape

The intended production design is **DMS full-load + CDC** with MS-CDC on a
Standard/Enterprise SQL Server: DMS reads the transaction log and emits an
ordered stream of I/U/D records (with `AR_H_CHANGE_SEQ` LSNs) to S3; Silver
collapses that log per key to current state.

We deliberately run **Express + snapshot-diff** for cost/learning reasons (see
[../ingestion/README.md](../ingestion/README.md)). To keep the *implementation
experience* identical, the derived change log carries DMS-CDC-style header
columns and Silver collapses on them exactly as it would on a real feed.

## The change log schema (produced by `jobs/cdc/snapshot_diff.py`)

| Column | Mimics DMS header | Notes |
|---|---|---|
| `Op` | `AR_H_OPERATION` | `I` / `U` / `D` |
| `cdc_seq` | `AR_H_CHANGE_SEQ` | **LSN mimic** — 35-char sortable string: `lpad(batch_epoch,15) ++ lpad(monotonic_id,20)` |
| `cdc_commit_ts` | `AR_H_COMMIT_TIMESTAMP` | **proxy** — the batch time, not a true commit time |
| `cdc_snapshot_dt` | — | source snapshot partition the change came from |
| `cdc_processed_at` | — | when the diff job ran |
| *(source columns)* | — | the row's after-image (before-image for deletes) |

### The LSN mimic, precisely
A real LSN is a per-commit, strictly increasing position in the transaction log.
We have exactly one monotonic clock: **the batch (snapshot) ordering**. So:

```
cdc_seq = lpad(batch_epoch_seconds, 15)  ++  lpad(monotonic_id, 20)
          └── the real ordering signal ──┘     └── uniqueness only ──┘
```

- The **batch-epoch prefix** guarantees any change from a later snapshot sorts
  after any change from an earlier one. This is the property Silver needs.
- The **monotonic-id suffix** just makes rows unique within a batch. It is *not*
  a meaningful intra-batch order (keyed tables have ≤1 change per key per batch).

## Collapse to current state (Silver 1 — identical to log-based CDC)

```python
w = Window.partitionBy(*primary_key).orderBy(F.col("cdc_seq").desc())
current = (change_log
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")            # latest change wins
    .filter(F.col("Op") != "D")  # tombstones are removed from current state
    .select(*source_columns))
```

This is the canonical merge you'd write against DMS/Debezium CDC. Swapping the
real feed in later means **no change to this code** — only the producer changes.

## Expected vs. Actual — the fidelity contract

| Aspect | Expected (real DMS + MS-CDC) | Actual (snapshot-diff here) |
|---|---|---|
| Source of truth | transaction log | two consecutive full snapshots |
| Records per key per cycle | **many** (every commit) | **≤ 1** (net change only) |
| `cdc_seq` | true commit LSN | synthetic, **batch-ordered** |
| `cdc_commit_ts` | real commit time | batch time (proxy) |
| Intra-cycle history (A→B→C) | fully captured (A→B, B→C) | **collapsed** (only A→C seen) |
| Transient rows (insert then delete within a cycle) | both events captured | **invisible** (net zero) |
| Hard deletes | captured with timing | detected, but only at cycle boundary |
| Ordering granularity | per commit | **per batch** |
| Latency | near-real-time | batch cadence |

**What this means in practice:** the collapse always yields the correct *current
state* (the latest snapshot is authoritative). What is lost is the *history
between snapshots* — intermediate values and short-lived rows. For this domain
(append-only restaurant orders, rarely mutated, revenue-focused) that loss is
immaterial. For an event-history use case (e.g., order-status lifecycle) it would
not be acceptable, and true MS-CDC would be required.

## Migration path to real CDC (no Silver rewrite)

1. Move source to SQL Server **Standard/Enterprise**; enable MS-CDC
   ([../ingestion/sql/01_enable_cdc.sql](../ingestion/sql/01_enable_cdc.sql)).
2. Switch the DMS replication type to `full-load-and-cdc`; point the S3 target's
   CDC output at the same `cdc/<table>/` layout.
3. Map DMS headers → our columns: `AR_H_OPERATION→Op`, `AR_H_CHANGE_SEQ→cdc_seq`,
   `AR_H_COMMIT_TIMESTAMP→cdc_commit_ts` (via DMS transformation rules).
4. Retire `snapshot_diff.py`. **Silver 1 and everything downstream are unchanged**
   because they only depend on the change-log contract above.
