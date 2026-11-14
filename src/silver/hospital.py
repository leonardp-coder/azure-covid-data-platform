
"""
Silver transformation module for hospital admissions.

Input:
    Bronze hospital admissions Delta table.

Output:
    Silver Hospital_admission Delta table.

Responsibilities:
    - Standardize hospital admission attributes.
    - Preserve daily and weekly reporting periods.
    - Profile data quality.
    - Validate indicators and weekly period formats.
    - Check daily and weekly business-key duplicates.
    - Publish the transformed Silver dataset.

This is a portfolio refactor. It has not been executed
or independently validated against the Azure workspace.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    BRONZE_TABLES,
    SILVER_TABLES,
)
from src.common.logging_utils import get_logger


logger = get_logger(__name__)


# ------------------------------------------------------------
# DATASET BUSINESS RULES
# ------------------------------------------------------------

DAILY_INDICATORS = [
    "daily icu occupancy",
    "daily hospital occupancy",
]

WEEKLY_INDICATORS = [
    "weekly new icu admissions per 100k",
    "weekly new hospital admissions per 100k",
]

EXPECTED_INDICATORS = DAILY_INDICATORS + WEEKLY_INDICATORS

REQUIRED_COLUMNS = [
    "country",
    "indicator",
    "date",
    "year_week",
    "value",
    "source",
]


# ------------------------------------------------------------
# SOURCE EXTRACTION
# ------------------------------------------------------------

def read_bronze_hospital_admissions(
    spark: SparkSession,
) -> DataFrame:
    """Read hospital admissions from the Bronze Delta table."""

    table_name = BRONZE_TABLES["hospital_admissions"]

    logger.info("Reading Bronze table: %s", table_name)

    return spark.table(table_name)


# ------------------------------------------------------------
# SILVER TRANSFORMATION
# ------------------------------------------------------------

def transform_hospital_admissions(
    df: DataFrame,
) -> DataFrame:
    """
    Standardize hospital admissions attributes.

    Preserve the original business logic:
        - Trim country.
        - Lowercase and trim indicator.
        - Uppercase and trim year_week.
        - Drop url.
        - Retain source.
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required hospital columns: {missing_columns}"
        )

    logger.info("Starting hospital admissions transformation.")

    silver_df = (
        df
        .withColumn(
            "country",
            F.trim(F.col("country")),
        )
        .withColumn(
            "indicator",
            F.lower(F.trim(F.col("indicator"))),
        )
        .withColumn(
            "year_week",
            F.upper(F.trim(F.col("year_week"))),
        )
        .drop("url")
    )

    logger.info("Hospital admissions transformation completed.")

    return silver_df


# ------------------------------------------------------------
# BUSINESS-KEY DUPLICATE PROFILING
# ------------------------------------------------------------

def count_daily_duplicates(
    df: DataFrame,
) -> int:
    """
    Count duplicate daily records using:
        country + date + indicator
    """

    return (
        df
        .filter(F.col("indicator").isin(DAILY_INDICATORS))
        .groupBy("country", "date", "indicator")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def count_weekly_duplicates(
    df: DataFrame,
) -> int:
    """
    Count duplicate weekly records using:
        country + year_week + indicator
    """

    return (
        df
        .filter(F.col("indicator").isin(WEEKLY_INDICATORS))
        .groupBy("country", "year_week", "indicator")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


# ------------------------------------------------------------
# DATA QUALITY VALIDATION
# ------------------------------------------------------------

def validate_hospital_admissions(
    df: DataFrame,
) -> Dict:
    """
    Profile hospital admissions data quality.

    Returns validation metrics without modifying the DataFrame.

    Null dates are reported but are not automatically treated
    as failures because weekly records may legitimately have
    no daily date.
    """

    logger.info("Starting hospital admissions quality checks.")

    quality_metrics = (
        df
        .agg(
            F.count("*").alias("total_records"),

            F.count(
                F.when(F.col("country").isNull(), 1)
            ).alias("null_country"),

            F.count(
                F.when(F.col("indicator").isNull(), 1)
            ).alias("null_indicator"),

            F.count(
                F.when(F.col("date").isNull(), 1)
            ).alias("null_date"),

            F.count(
                F.when(F.col("year_week").isNull(), 1)
            ).alias("null_year_week"),

            F.count(
                F.when(F.col("value").isNull(), 1)
            ).alias("null_value"),

            F.count(
                F.when(F.col("value") < 0, 1)
            ).alias("negative_value"),

            F.count(
                F.when(F.col("source").isNull(), 1)
            ).alias("null_source"),
        )
        .first()
        .asDict()
    )

    # Unexpected or missing indicator values
    unexpected_indicators = (
        df
        .filter(
            F.col("indicator").isNull()
            | ~F.col("indicator").isin(EXPECTED_INDICATORS)
        )
        .select("indicator")
        .distinct()
        .collect()
    )

    unexpected_indicator_values = [
        row["indicator"]
        for row in unexpected_indicators
    ]

    # Validate ISO-style weekly periods, e.g. 2020-W11
    invalid_week_records = (
        df
        .filter(F.col("indicator").isin(WEEKLY_INDICATORS))
        .filter(
            F.col("year_week").isNull()
            | ~F.col("year_week").rlike(r"^\d{4}-W\d{2}$")
        )
    )

    invalid_week_count = invalid_week_records.count()

    # Check daily and weekly business keys separately
    daily_duplicate_count = count_daily_duplicates(df)

    weekly_duplicate_count = count_weekly_duplicates(df)

    validation_results = {
        "quality_metrics": quality_metrics,
        "unexpected_indicators": unexpected_indicator_values,
        "invalid_week_records": invalid_week_count,
        "daily_duplicate_groups": daily_duplicate_count,
        "weekly_duplicate_groups": weekly_duplicate_count,
    }

    logger.info(
        "Hospital admissions quality results: %s",
        validation_results,
    )

    if quality_metrics["negative_value"] > 0:
        logger.warning("Negative hospital values detected.")

    if unexpected_indicator_values:
        logger.warning("Unexpected hospital indicators detected.")

    if invalid_week_count > 0:
        logger.warning("Invalid weekly reporting periods detected.")

    if daily_duplicate_count > 0 or weekly_duplicate_count > 0:
        logger.warning("Duplicate hospital business keys detected.")

    logger.info("Hospital admissions quality profiling completed.")

    return validation_results


# ------------------------------------------------------------
# SILVER PUBLISHING
# ------------------------------------------------------------

def publish_silver_hospital_admissions(
    df: DataFrame,
) -> int:
    """
    Publish hospital admissions to the Silver Delta table.

    Uses overwrite mode to preserve the existing full-refresh
    processing approach.

    Returns the published record count.
    """

    target_table = SILVER_TABLES["hospital_admissions"]

    source_count = df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        target_table,
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    published_count = (
        df.sparkSession
        .table(target_table)
        .count()
    )

    if source_count != published_count:
        raise RuntimeError(
            f"Hospital record count mismatch. "
            f"Source: {source_count}, "
            f"Published: {published_count}"
        )

    logger.info(
        "Successfully published %s hospital records.",
        published_count,
    )

    return published_count


# ------------------------------------------------------------
# DATASET TRANSFORMATION ENTRY POINT
# ------------------------------------------------------------

def process_hospital_admissions(
    spark: SparkSession,
) -> Dict:
    """
    Execute the complete Silver hospital admissions workflow.
    """

    logger.info("Starting Silver hospital admissions workflow.")

    bronze_df = read_bronze_hospital_admissions(spark)

    silver_df = transform_hospital_admissions(bronze_df)

    validation_results = validate_hospital_admissions(silver_df)

    published_count = publish_silver_hospital_admissions(silver_df)

    validation_results["published_records"] = published_count

    logger.info("Silver hospital admissions workflow completed.")

    return validation_results
