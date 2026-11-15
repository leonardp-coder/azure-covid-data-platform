
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger
from src.silver.hospital import WEEKLY_INDICATORS


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["hospital_admissions"]

COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_hospital_weekly"


def read_hospital_silver(
    spark: SparkSession,
) -> DataFrame:
    """Read hospital admissions from the Silver layer."""

    logger.info("Reading hospital data from %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def prepare_weekly_data(
    hospital_df: DataFrame,
) -> DataFrame:
    """Select weekly hospital indicators only."""

    logger.info("Selecting weekly hospital indicators")

    weekly_df = hospital_df.filter(
        F.col("indicator").isin(WEEKLY_INDICATORS)
    )

    logger.info(
        "Weekly hospital records selected: %s",
        weekly_df.count(),
    )

    return weekly_df


def enrich_with_country_dimension(
    spark: SparkSession,
    weekly_df: DataFrame,
) -> DataFrame:
    """
    Enrich weekly hospital records with country keys.

    The original notebook does not join the date dimension.
    """

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)

    logger.info("Joining weekly hospital data with country dimension")

    fact_df = (
        weekly_df.alias("h")
        .join(
            dim_country_df.alias("c"),
            F.col("h.country") == F.col("c.country_name"),
            "left",
        )
        .select(
            F.col("c.country_key").alias("country_key"),
            F.col("h.country").alias("country_name"),
            F.col("h.year_week"),
            F.split("h.year_week", "-W")
            .getItem(0)
            .cast("int")
            .alias("iso_year"),
            F.split("h.year_week", "-W")
            .getItem(1)
            .cast("int")
            .alias("iso_week"),
            F.col("h.indicator"),
            F.col("h.value"),
            F.col("h.source"),
        )
    )

    return fact_df


def validate_fact_hospital_weekly(
    fact_df: DataFrame,
) -> None:
    """Validate weekly hospital fact records and business grain."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError(
            "Weekly hospital fact contains no records."
        )

    invalid_week_records = fact_df.filter(
        F.col("year_week").isNull()
        | ~F.col("year_week").rlike(r"^\d{4}-W\d{2}$")
        | F.col("iso_year").isNull()
        | F.col("iso_week").isNull()
        | (F.col("iso_week") < 1)
        | (F.col("iso_week") > 53)
    ).count()

    negative_values = fact_df.filter(
        F.col("value") < 0
    ).count()

    duplicate_grain = (
        fact_df
        .groupBy(
            "country_name",
            "year_week",
            "indicator",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if invalid_week_records > 0:
        raise ValueError(
            f"Weekly hospital fact contains {invalid_week_records} "
            "invalid weekly periods."
        )

    if negative_values > 0:
        raise ValueError(
            f"Weekly hospital fact contains {negative_values} "
            "negative values."
        )

    if duplicate_grain > 0:
        raise ValueError(
            f"Weekly hospital fact contains {duplicate_grain} "
            "duplicate business-key groups."
        )

    logger.info(
        "Weekly hospital fact validation passed. Rows: %s",
        total_records,
    )

    logger.info(
        "Null country keys and null measure values are permitted "
        "where present in the source data."
    )


def publish_fact_hospital_weekly(
    spark: SparkSession,
    fact_df: DataFrame,
) -> None:
    """Publish weekly hospital fact data as a Delta table."""

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
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Weekly hospital row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Weekly hospital fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_hospital_weekly(
    spark: SparkSession,
) -> None:
    """Execute the complete weekly hospital fact pipeline."""

    hospital_df = read_hospital_silver(spark)

    weekly_df = prepare_weekly_data(hospital_df)

    fact_df = enrich_with_country_dimension(spark, weekly_df)

    validate_fact_hospital_weekly(fact_df)

    publish_fact_hospital_weekly(spark, fact_df)
