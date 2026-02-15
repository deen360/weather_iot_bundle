
from databricks.connect import DatabricksSession 
import pytest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from local_time.local_utils import add_local_time

# --------------------------------------------
# Spark session fixture for all tests
# --------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        spark = DatabricksSession.builder.getOrCreate()
    )


# --------------------------------------------
# Test: positive timezone offset
# --------------------------------------------
def test_add_local_time_positive_offset(spark):
    df = spark.createDataFrame([
        {
            "event_utc": datetime(2025, 1, 1, 12, 0, 0),   # 12:00 UTC
            "offset": 3600,                                # +1 hour
        }
    ])

    result = add_local_time(
        df,
        datetime_col="event_utc",
        timezone_col="offset",
        output_col="local_time"
    ).collect()[0]

    expected = datetime(2025, 1, 1, 13, 0, 0)

    assert result["local_time"] == expected


# --------------------------------------------
# Test: negative timezone offset
# --------------------------------------------
def test_add_local_time_negative_offset(spark):
    df = spark.createDataFrame([
        {
            "event_utc": datetime(2025, 1, 1, 12, 0, 0),
            "offset": -5 * 3600,  # -5 hours
        }
    ])

    result = add_local_time(
        df,
        datetime_col="event_utc",
        timezone_col="offset",
        output_col="local_time"
    ).collect()[0]

    expected = datetime(2025, 1, 1, 7, 0, 0)

    assert result["local_time"] == expected


# --------------------------------------------
# Test: multiple rows
# --------------------------------------------
def test_add_local_time_multiple_rows(spark):
    df = spark.createDataFrame([
        {"event_utc": datetime(2025, 1, 1, 0, 0), "offset": 0},
        {"event_utc": datetime(2025, 1, 1, 0, 0), "offset": 7200},  # +2h
        {"event_utc": datetime(2025, 1, 1, 0, 0), "offset": -7200}, # -2h
    ])

    result = add_local_time(
        df,
        datetime_col="event_utc",
        timezone_col="offset",
        output_col="local_time"
    ).orderBy("offset").collect()

    assert result[0]["local_time"] == datetime(2024, 12, 31, 22, 0)  # -2h
    assert result[1]["local_time"] == datetime(2025, 1, 1, 0, 0)     #  0h
    assert result[2]["local_time"] == datetime(2025, 1, 1, 2, 0)     # +2h


# --------------------------------------------
# Test: null inputs
# --------------------------------------------
def test_add_local_time_nulls(spark):
    df = spark.createDataFrame([
        {"event_utc": None, "offset": 3600},
        {"event_utc": da_
