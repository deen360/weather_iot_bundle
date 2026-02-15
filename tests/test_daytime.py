import pytest
from pyspark.sql import functions as F
from datetime import datetime

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

from day_time.day_utils import add_daytime_flag  # <-- update import


# ---------------------------------------------------------
# Test: daytime case
# ---------------------------------------------------------
def test_add_daytime_flag_day(spark):
    data = [
        {
            "event_time": datetime(2025, 1, 1, 12, 0),  # Noon
            "sunrise": datetime(2025, 1, 1, 7, 0),
            "sunset": datetime(2025, 1, 1, 17, 0),
        }
    ]

    df = spark.createDataFrame(data)

    result = add_daytime_flag(
        df,
        datetime_col="event_time",
        sunrise_col="sunrise",
        sunset_col="sunset",
        output_col="is_day",
    )

    row = result.collect()[0]
    assert row["is_day"] == "Day"


# ---------------------------------------------------------
# Test: nighttime case
# ---------------------------------------------------------
def test_add_daytime_flag_night(spark):
    data = [
        {
            "event_time": datetime(2025, 1, 1, 3, 0),  # 3 AM
            "sunrise": datetime(2025, 1, 1, 7, 0),
            "sunset": datetime(2025, 1, 1, 17, 0),
        }
    ]

    df = spark.createDataFrame(data)

    result = add_daytime_flag(
        df,
        datetime_col="event_time",
        sunrise_col="sunrise",
        sunset_col="sunset",
        output_col="is_day",
    )

    row = result.collect()[0]
    assert row["is_day"] == "Night"


# ---------------------------------------------------------
# Test: edge case — equal to sunrise (should be Day)
# ---------------------------------------------------------
def test_add_daytime_flag_at_sunrise(spark):
    data = [
        {
            "event_time": datetime(2025, 1, 1, 7, 0),
            "sunrise": datetime(2025, 1, 1, 7, 0),
            "sunset": datetime(2025, 1, 1, 17, 0),
        }
    ]

    df = spark.createDataFrame(data)

    result = add_daytime_flag(
        df,
        datetime_col="event_time",
        sunrise_col="sunrise",
        sunset_col="sunset",
        output_col="is_day",
    )

    row = result.collect()[0]
    assert row["is_day"] == "Day"


# ---------------------------------------------------------
# Test: edge case — equal to sunset (should be Night)
# ---------------------------------------------------------
def test_add_daytime_flag_at_sunset(spark):
    data = [
        {
            "event_time": datetime(2025, 1, 1, 17, 0),
            "sunrise": datetime(2025, 1, 1, 7, 0),
            "sunset": datetime(2025, 1, 1, 17, 0),
        }
    ]

    df = spark.createDataFrame(data)

    result = add_daytime_flag(
        df,
        datetime_col="event_time",
        sunrise_col="sunrise",
        sunset_col="sunset",
        output_col="is_day",
    )

    row = result.collect()[0]
    assert row["is_day"] == "Night"
