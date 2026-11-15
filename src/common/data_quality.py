
"""
Reusable data quality validation utilities.
"""

from typing import Dict, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _validate_columns(
    df: DataFrame,
    columns: Sequence[str],
) -> None:
    """
    Validate that the specified columns exist in the DataFrame.
    """

    missing_columns = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Columns not found in DataFrame: {missing_columns}"
        )


def check_null_values(
    df: DataFrame,
    columns: Sequence[str],
) -> Dict[str, int]:
    """
    Count null values in the specified columns.
    """

    _validate_columns(df, columns)

    return {
        column: df.filter(F.col(column).isNull()).count()
        for column in columns
    }


def check_duplicates(
    df: DataFrame,
    key_columns: Sequence[str],
) -> int:
    """
    Count duplicate records based on business keys.

    Returns the number of duplicate key groups,
    not the total number of excess duplicate rows.
    """

    if not key_columns:
        raise ValueError(
            "At least one key column must be provided."
        )

    _validate_columns(df, key_columns)

    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def check_negative_values(
    df: DataFrame,
    column: str,
) -> int:
    """
    Count negative values in a numeric column.
    """

    _validate_columns(df, [column])

    return df.filter(F.col(column) < 0).count()


def check_row_count(df: DataFrame) -> int:
    """
    Return the total number of records.
    """

    return df.count()
