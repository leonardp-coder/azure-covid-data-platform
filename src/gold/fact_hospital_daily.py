
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger
from src.silver.hospital import DAILY_INDICATORS


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["hospital_admissions"]

COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
DATE_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_date"

TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_hospital_daily"


def read_hospital_silver(
    spark: SparkSession,
) -> DataFrame:
    """Read hospital admissions from the Silver layer."""

    logger.info("Reading hospital data from %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def prepare_daily_data(
    hospital_df: DataFrame,
) -> DataFrame:
    """
    Select daily hospital indicators only.

    Weekly records remain in Silver and are processed
    separately by the weekly fact module.
    """

    logger.info("Selecting daily hospital indicators")

    daily_df = hospital_df.filter(
        F.col("indicator").isin(DAILY_INDICATORS)
    )

    logger.info(
        "Daily hospital records selected: %s",
        daily_df.count(),
    )

    return daily_df


def enrich_with_dimensions(
    spark: SparkSession,
    daily_df: DataFrame,
) -> DataFrame:
    """
    Enrich daily hospital records with country and date keys.
    """

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_date_df = spark.table(DATE_DIM_TABLE)

    logger.info("Joining daily hospital data with dimensions")

    fact_df = (
        daily_df.alias("h")
        .join(
            dim_country_df.alias("c"),
            F.col("h.country") == F.col("c.country_name"),
            "left",
        )
        .join(
            dim_date_df.alias("d"),
            F.col("h.date") == F.col("d.full_date"),
            "left",
        )
        .select(
            F.col("c.country_key").alias("country_key"),
            F.col("d.date_key").alias("date_key"),
            F.col("h.country").alias("country_name"),
            F.col("h.indicator").alias("indicator"),
            F.col("h.value").alias("value"),
            F.col("h.source").alias("source"),
        )
    )

    return fact_df


def validate_fact_hospital_daily(
    fact_df: DataFrame,
) -> None:
    """Validate daily hospital fact records and business grain."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError(
            "Daily hospital fact contains no records."
        )

    null_country_keys = fact_df.filter(
        F.col("country_key").isNull()
    ).count()

    null_date_keys = fact_df.filter(
        F.col("date_key").isNull()
    ).count()

    null_country_names = fact_df.filter(
        F.col("country_name").isNull()
    ).count()

    null_indicators = fact_df.filter(
        F.col("indicator").isNull()
    ).count()

    negative_values = fact_df.filter(
        F.col("value") < 0
    ).count()

    duplicate_grain = (
        fact_df
        .groupBy(
            "country_name",
            "date_key",
            "indicator",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_country_keys > 0:
        raise ValueError(
            f"Daily hospital fact has {null_country_keys} "
            "unmatched country keys."
        )

    if null_date_keys > 0:
        raise ValueError(
            f"Daily hospital fact has {null_date_keys} "
            "unmatched date keys."
        )

    if null_country_names > 0:
        raise ValueError(
            f"Daily hospital fact has {null_country_names} "
            "null country names."
        )

    if null_indicators > 0:
        raise ValueError(
            f"Daily hospital fact has {null_indicators} "
            "null indicators."
        )

    if negative_values > 0:
        raise ValueError(
            f"Daily hospital fact has {negative_values} "
            "negative values."
        )

    if duplicate_grain > 0:
        raise ValueError(
            f"Daily hospital fact has {duplicate_grain} "
            "duplicate business-key groups."
        )

    logger.info(
        "Daily hospital fact validation passed. Rows: %s",
        total_records,
    )


def publish_fact_hospital_daily(
    spark: SparkSession,
    fact_df: DataFrame,
) -> None:
    """Publish daily hospital fact data as a Delta table."""

    source_count = fact_df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        TARGET_TABLE,
    )

    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Daily hospital row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Daily hospital fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_hospital_daily(
    spark: SparkSession,
) -> None:
    """Execute the complete daily hospital fact pipeline."""

    hospital_df = read_hospital_silver(spark)

    daily_df = prepare_daily_data(hospital_df)

    fact_df = enrich_with_dimensions(spark, daily_df)

    validate_fact_hospital_daily(fact_df)

    publish_fact_hospital_daily(spark, fact_df)
