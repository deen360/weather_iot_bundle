from pyspark.sql import functions as F
from pyspark.sql import types as T


def add_local_time(df, datetime_col, timezone_col, output_col):
    """
    Computes the wall-clock time for the user in that city.
    Logic: UTC Timestamp + City Offset (in seconds).
    """
    return df.withColumn(
        output_col,
        F.from_unixtime(F.unix_timestamp(datetime_col) + F.col(timezone_col)).cast(
            T.TimestampType()
        ),
    )
