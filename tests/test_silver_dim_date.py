"""Unit tests for the static date-dimension load (typing + dedup + casing)."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.silver_dim_date import build_dim_date

# Snapshot columns as they arrive from DMS: DMS metadata (Op/ingested_at) +
# source columns, with date_key as a string and booleans as strings, to prove
# the cast enforces the target types.
SNAP_COLS = ["Op", "ingested_at", "date_key", "year", "month", "week",
             "day_of_week", "is_weekend", "is_holiday", "holiday_name"]


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("dim-date-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _snap(spark, rows):
    return spark.createDataFrame(rows, SNAP_COLS)


def test_types_are_enforced(spark):
    rows = [("I", "2026-07-16", "2023-01-01", "2023", "1", "52",
             "Sunday", "true", "true", "New Year's Day")]
    out = build_dim_date(_snap(spark, rows))
    t = dict(out.dtypes)
    assert t["date_key"] == "date"
    assert t["year"] == "int" and t["month"] == "int" and t["week"] == "int"
    assert t["is_weekend"] == "boolean" and t["is_holiday"] == "boolean"
    assert t["day_of_week"] == "string" and t["holiday_name"] == "string"

    r = out.collect()[0]
    assert r["date_key"] == datetime.date(2023, 1, 1)
    assert r["year"] == 2023 and r["month"] == 1
    assert r["is_weekend"] is True


def test_dms_metadata_dropped(spark):
    rows = [("I", "2026-07-16", "2023-01-01", "2023", "1", "52",
             "Sunday", "false", "false", None)]
    out = build_dim_date(_snap(spark, rows))
    assert "op" not in out.columns and "ingested_at" not in out.columns and "Op" not in out.columns


def test_deduplicates_on_date_key(spark):
    rows = [
        ("I", "2026-07-16", "2023-01-01", "2023", "1", "52", "Sunday", "true", "true", "NYD"),
        ("I", "2026-07-16", "2023-01-01", "2023", "1", "52", "Sunday", "true", "true", "NYD"),
    ]
    out = build_dim_date(_snap(spark, rows))
    assert out.count() == 1
