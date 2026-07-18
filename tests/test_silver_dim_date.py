"""Unit tests for the generated conformed date dimension."""
import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.silver.dim_date import date_attributes, build_date_dim, typed_source_holidays

D = datetime.date


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


def _cal(spark, dates):
    return spark.createDataFrame([(d,) for d in dates], "date_key date")


def test_date_attributes(spark):
    out = {r["date_key"]: r for r in date_attributes(_cal(spark, [D(2023, 1, 1), D(2023, 1, 2)])).collect()}
    sun = out[D(2023, 1, 1)]      # Sunday
    assert sun["year"] == 2023 and sun["month"] == 1
    assert sun["day_of_week"] == "Sunday" and sun["is_weekend"] is True
    mon = out[D(2023, 1, 2)]      # Monday
    assert mon["day_of_week"] == "Monday" and mon["is_weekend"] is False


def test_build_date_dim_enriches_holidays(spark):
    cal = _cal(spark, [D(2023, 1, 1), D(2023, 7, 4), D(2024, 3, 1)])
    holidays = spark.createDataFrame(
        [(D(2023, 7, 4), True, "Independence Day")],
        "date_key date, is_holiday boolean, holiday_name string",
    )
    out = {r["date_key"]: r for r in build_date_dim(cal, holidays).collect()}

    assert out[D(2023, 7, 4)]["is_holiday"] is True
    assert out[D(2023, 7, 4)]["holiday_name"] == "Independence Day"
    # dates with no source holiday default to False (covers the non-2023 range too)
    assert out[D(2023, 1, 1)]["is_holiday"] is False
    assert out[D(2024, 3, 1)]["is_holiday"] is False
    assert out[D(2024, 3, 1)]["holiday_name"] is None


def test_typed_source_holidays_drops_metadata_and_types(spark):
    # Source snapshot arrives as text with DMS metadata; keep only typed holidays.
    cols = ("Op string, ingested_at string, date_key string, year string, month string, "
            "week string, day_of_week string, is_weekend string, is_holiday string, holiday_name string")
    snap = spark.createDataFrame([
        ("I", "2026-07-16", "2023-01-01", "2023", "1", "52", "Sunday", "true", "true", "New Year"),
        ("I", "2026-07-16", "2023-01-01", "2023", "1", "52", "Sunday", "true", "true", "New Year"),  # dup
    ], cols)
    out = typed_source_holidays(snap)
    assert set(out.columns) == {"date_key", "is_holiday", "holiday_name"}
    assert out.count() == 1                      # deduped on date_key
    r = out.collect()[0]
    assert r["date_key"] == D(2023, 1, 1) and r["is_holiday"] is True
