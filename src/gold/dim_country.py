
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["country_lookup"]
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"


def read_country_silver(spark: SparkSession) -> DataFrame:
    """Read the country lookup dataset from the Silver layer."""

    logger.info("Reading country lookup table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def build_dim_country(country_silver_df: DataFrame) -> DataFrame:
    """
    Build the country dimension.

    Preserve the original SHA-256 country key logic:
    uppercase(trim(country_code_3_digit)).
    """

    required_columns = [
        "country",
        "country_code_2_digit",
        "country_code_3_digit",
        "continent",
        "population",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in country_silver_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns in country Silver data: {missing_columns}"
        )

    country_dim_df = country_silver_df.select(
        F.sha2(
            F.upper(F.trim(F.col("country_code_3_digit"))),
            256,
        ).alias("country_key"),
        F.trim(F.col("country")).alias("country_name"),
        F.upper(F.trim(F.col("country_code_2_digit"))).alias(
            "country_code_2_digit"
        ),
        F.upper(F.trim(F.col("country_code_3_digit"))).alias(
            "country_code_3_digit"
        ),
        F.trim(F.col("continent")).alias("continent"),
        F.col("population").cast("long").alias("population"),
    )

    return country_dim_df


def validate_dim_country(country_dim_df: DataFrame) -> None:
    """Validate country dimension keys before publishing."""

    null_key_count = country_dim_df.filter(
        F.col("country_key").isNull()
    ).count()

    if null_key_count > 0:
        raise ValueError(
            f"Country dimension contains {null_key_count} null country keys."
        )

    duplicate_key_count = (
        country_dim_df
        .groupBy("country_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError(
            f"Country dimension contains {duplicate_key_count} duplicate keys."
        )

    logger.info("Country dimension key validation passed.")


def publish_dim_country(
    spark: SparkSession,
    country_dim_df: DataFrame,
) -> None:
    """Publish the country dimension as a Delta table."""

    source_count = country_dim_df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        TARGET_TABLE,
    )

    (
        country_dim_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Row count mismatch: source={source_count}, target={target_count}"
        )

    logger.info(
        "Country dimension published successfully. Rows: %s",
        target_count,
    )


def run_dim_country(spark: SparkSession) -> None:
    """Execute the country dimension transformation."""

    country_silver_df = read_country_silver(spark)

    country_dim_df = build_dim_country(country_silver_df)

    validate_dim_country(country_dim_df)

    publish_dim_country(spark, country_dim_df)
