
"""
Silver transformation module for COVID cases and deaths.

Input:
    Bronze cases_deaths Delta table.

Output:
    Silver cases_deaths Delta table.

Responsibilities:
    - Standardize source attributes.
    - Normalize date and indicator fields.
    - Classify geographic records.
    - Perform dataset-specific data quality checks.
    - Publish the transformed Silver dataset.

This is a portfolio refactor and has not been executed
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

EXPECTED_INDICATORS = {
    "confirmed cases",
    "deaths",
}

BUSINESS_KEY_COLUMNS = [
    "country",
    "date",
    "indicator",
]


# ------------------------------------------------------------
# SOURCE EXTRACTION
# ------------------------------------------------------------

def read_bronze_cases_deaths(
    spark: SparkSession,
) -> DataFrame:
    """Read the Bronze COVID cases and deaths table."""

    table_name = BRONZE_TABLES["cases_deaths"]

    logger.info("Reading Bronze table: %s", table_name)

    return spark.table(table_name)


# ------------------------------------------------------------
# SILVER TRANSFORMATION
# ------------------------------------------------------------

def transform_cases_deaths(
    df: DataFrame,
) -> DataFrame:
    """Standardize and transform COVID cases and deaths data."""

    logger.info("Starting cases and deaths transformation.")

    required_columns = [
        "country",
        "country_code",
        "continent",
        "indicator",
        "date",
        "daily_count",
        "population",
        "rate_14_day",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required source columns: {missing_columns}"
        )

    transformed_df = (
        df
        .withColumn(
            "country",
            F.trim(F.col("country")),
        )
        .withColumn(
            "country_code",
            F.upper(F.trim(F.col("country_code"))),
        )
        .withColumn(
            "continent",
            F.trim(F.col("continent")),
        )
        .withColumn(
            "indicator",
            F.lower(F.trim(F.col("indicator"))),
        )
        .withColumn(
            "date",
            F.to_date(F.col("date"), "dd-MM-yyyy"),
        )
        .withColumn(
            "geography_type",
            F.when(
                F.lower(F.trim(F.col("country"))).endswith("(total)"),
                F.lit("regional"),
            ).otherwise(F.lit("countries")),
        )
        .drop("source")
    )

    logger.info("Cases and deaths transformation completed.")

    return transformed_df


# ------------------------------------------------------------
# DATA QUALITY VALIDATION
# ------------------------------------------------------------

def validate_cases_deaths(
    df: DataFrame,
) -> Dict:
    """
    Run dataset-specific data quality checks.

    Intentional regional null country codes are not treated
    as failures.
    """

    logger.info("Starting cases and deaths quality checks.")

    required_columns = [
        "country",
        "date",
        "indicator",
        "daily_count",
    ]

    null_counts = {
        column: df.filter(
            F.col(column).isNull()
        ).count()
        for column in required_columns
    }

    negative_count = df.filter(
        F.col("daily_count") < 0
    ).count()

    unexpected_indicators = (
        df
        .filter(
            ~F.col("indicator").isin(
                *EXPECTED_INDICATORS
            )
        )
        .select("indicator")
        .distinct()
        .collect()
    )

    duplicate_count = (
        df
        .groupBy(*BUSINESS_KEY_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    invalid_geography_count = df.filter(
        F.col("geography_type").isNull()
    ).count()

    validation_results = {
        "null_counts": null_counts,
        "negative_daily_counts": negative_count,
        "unexpected_indicators": [
            row["indicator"]
            for row in unexpected_indicators
        ],
        "duplicate_business_keys": duplicate_count,
        "invalid_geography_records": invalid_geography_count,
    }

    logger.info(
        "Cases and deaths quality results: %s",
        validation_results,
    )

    failed_checks = []

    if any(null_counts.values()):
        failed_checks.append(
            "Required columns contain null values"
        )

    if negative_count > 0:
        failed_checks.append(
            "Negative daily counts detected"
        )

    if unexpected_indicators:
        failed_checks.append(
            "Unexpected indicators detected"
        )

    if duplicate_count > 0:
        failed_checks.append(
            "Duplicate business keys detected"
        )

    if invalid_geography_count > 0:
        failed_checks.append(
            "Invalid geography classifications detected"
        )

    if failed_checks:
        raise ValueError(
            f"Cases and deaths validation failed: {failed_checks}"
        )

    logger.info("Cases and deaths validation passed.")

    return validation_results


# ------------------------------------------------------------
# SILVER PUBLISHING
# ------------------------------------------------------------

def publish_silver_cases_deaths(
    spark: SparkSession,
    df: DataFrame,
) -> None:
    """
    Publish validated cases and deaths data to Silver Delta.

    Uses overwrite mode to match the existing full-refresh
    batch processing approach.
    """

    target_table = SILVER_TABLES["cases_deaths"]

    logger.info(
        "Publishing Silver dataset to %s",
        target_table,
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    target_count = spark.table(target_table).count()

    logger.info(
        "Silver publishing completed for %s. Rows: %s",
        target_table,
        target_count,
    )


# ------------------------------------------------------------
# DATASET TRANSFORMATION ENTRY POINT
# ------------------------------------------------------------

def process_cases_deaths(
    spark: SparkSession,
) -> Dict:
    """Execute the complete Silver cases and deaths workflow."""

    logger.info("Starting Silver cases and deaths workflow.")

    bronze_df = read_bronze_cases_deaths(spark)

    silver_df = transform_cases_deaths(bronze_df)

    validation_results = validate_cases_deaths(silver_df)

    publish_silver_cases_deaths(spark, silver_df)

    logger.info("Silver cases and deaths workflow completed.")

    return validation_results
