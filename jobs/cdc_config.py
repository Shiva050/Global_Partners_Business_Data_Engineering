"""Table specifications for the snapshot-diff CDC job.

The source is SQL Server Express, which cannot do MS-CDC. Instead we land full
snapshots via DMS full-load and *derive* an I/U/D change log by diffing
consecutive snapshots (see snapshot_diff.py). This module declares, per table,
the natural key used to align rows across two snapshots.
"""
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class TableSpec:
    name: str
    # Natural/composite key used to align rows across snapshots (also the key
    # validated for a static dimension). None => no unique identity (a multiset):
    # the CDC diff falls back to a count-based comparison of full rows.
    keys: Optional[List[str]]
    schema: str = "gpb"
    # Static conformed dimension: no meaningful change stream, so it skips CDC
    # entirely and is loaded straight to silver with an enforced typed schema.
    static: bool = False

    def snapshot_relpath(self, snapshot_date: str) -> str:
        """Path (relative to the bronze root) of this table's snapshot for a date."""
        return f"snapshots/dt={snapshot_date}/{self.schema}/{self.name}"

    def cdc_relpath(self, snapshot_date: str) -> str:
        """Path (relative to the bronze root) where the derived change log is written."""
        return f"cdc/{self.name}/dt={snapshot_date}"


# DMS full-load with IncludeOpForFullLoad + TimestampColumnName adds these two
# columns to every snapshot Parquet. They are DMS metadata, NOT source data, so
# they are excluded from the row comparison and re-derived by the diff.
# NOTE: the diff job lowercases all snapshot columns on read (source tables come
# through with mixed casing — order_* are UPPERCASE, date_dim is lowercase), so
# these are matched in lowercase.
DMS_META_COLS = ["op", "ingested_at"]

# Header columns the CDC diff attaches to every change record (see
# jobs/cdc_snapshot_diff.add_cdc_headers). Silver strips these to recover the
# source row. `Op` + `cdc_seq` drive the collapse to current state.
CDC_HEADER_COLS = ["Op", "cdc_seq", "cdc_commit_ts", "cdc_snapshot_dt", "cdc_processed_at"]


# ---------------------------------------------------------------------------
# Key choices (verified against the 2026-07-16 snapshot):
#   order_items         : (order_id, lineitem_id) -> unique (203,519 rows). Keyed diff.
#   date_dim            : date_key -> unique (365 rows). Keyed diff.
#   order_item_options  : NO unique key. 2,299 rows are fully-identical duplicates
#                         (same order/lineitem/group/name/price/qty), so it is a
#                         multiset. keys=None -> count-based diff preserves the
#                         exact multiplicity (which matters for revenue = sum of
#                         option_price * option_quantity across rows).
# ---------------------------------------------------------------------------
TABLES = {
    "order_items": TableSpec(
        name="order_items",
        keys=["order_id", "lineitem_id"],
    ),
    "order_item_options": TableSpec(
        name="order_item_options",
        keys=None,  # multiset — no unique identity
    ),
    "date_dim": TableSpec(
        name="date_dim",
        keys=["date_key"],
        static=True,  # static calendar dimension — no CDC, typed load only
    ),
}

# Tables that flow through the CDC pipeline (snapshot-diff -> collapse).
CDC_TABLES = {n: s for n, s in TABLES.items() if not s.static}
# Static dimensions loaded straight to silver (no CDC).
STATIC_TABLES = {n: s for n, s in TABLES.items() if s.static}
