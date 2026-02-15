from pyspark.sql import functions as F


def add_daytime_flag(df, datetime_col, sunrise_col, sunset_col, output_col):
    """
    Adds a Boolean or String flag to segment data by solar cycle.
    Logic: Returns 'Day' if the event happened between sunrise and sunset, else 'Night'.
    """
    return df.withColumn(
        output_col,
        F.when(
            (F.col(datetime_col) >= F.col(sunrise_col))
            & (F.col(datetime_col) < F.col(sunset_col)),
            F.lit("Day"),
        ).otherwise(F.lit("Night")),
    )
