"""
Reusable Apache Spark utilities.
"""

from pyspark.sql import DataFrame


def get_record_count(df: DataFrame) -> int:
    """
    Return the total number of records in a DataFrame.
    """
    return df.count()


def get_dataframe_columns(df: DataFrame) -> list:
    """
    Return the list of DataFrame column names.
    """
    return df.columns


def validate_required_columns(
    df: DataFrame,
    required_columns: list
) -> None:
    """
    Validate that all required columns exist.
    """

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )
