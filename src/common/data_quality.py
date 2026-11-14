"""
Reusable data quality validation utilities.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_null_values(
    df: DataFrame,
    columns: list
) -> dict:
    """
    Count null values in the specified columns.
    """
    return {
        column: df.filter(F.col(column).isNull()).count()
        for column in columns
    }


def check_duplicates(
    df: DataFrame,
    key_columns: list
) -> int:
    """
    Count duplicate records based on business keys.
    """
    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def check_negative_values(
    df: DataFrame,
    column: str
) -> int:
    """
    Count negative values in a numeric column.
    """
    return df.filter(F.col(column) < 0).count()


def check_row_count(df: DataFrame) -> int:
    """
    Return the total number of records.
    """
    return df.count()
