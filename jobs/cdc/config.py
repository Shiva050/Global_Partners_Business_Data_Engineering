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
    keys: List[str]           # natural/composite key used to align rows across snapshots
    schema: str = "gpb"

    def snapshot_relpath(self, snapshot_date: str) -> str:
        """Path (relative to the bronze root) of this table's snapshot for a date."""
        return f"snapshots/dt={snapshot_date}/{self.schema}/{self.name}"

    def cdc_relpath(self, snapshot_date: str) -> str:
        """Path (relative to the bronze root) where the derived change log is written."""
        return f"cdc/{self.name}/dt={snapshot_date}"


# DMS full-load with IncludeOpForFullLoad + TimestampColumnName adds these two
# columns to every snapshot Parquet. They are DMS metadata, NOT source data, so
# they are excluded from the row comparison and re-derived by the diff.
DMS_META_COLS = ["Op", "ingested_at"]


# ---------------------------------------------------------------------------
# Key choices (documented assumptions):
#   order_items         : one row per item within an order -> (order_id, lineitem_id)
#   order_item_options  : one row per option on a line item -> add the option
#                         identity so a changed price/qty is detected as an UPDATE
#                         rather than delete+insert.
#   date_dim            : static calendar dimension keyed by the date.
# ---------------------------------------------------------------------------
TABLES = {
    "order_items": TableSpec(
        name="order_items",
        keys=["order_id", "lineitem_id"],
    ),
    "order_item_options": TableSpec(
        name="order_item_options",
        keys=["order_id", "lineitem_id", "option_group_name", "option_name"],
    ),
    "date_dim": TableSpec(
        name="date_dim",
        keys=["date_key"],
    ),
}
