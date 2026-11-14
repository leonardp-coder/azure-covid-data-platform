
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["hospital_admissions"]
COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
DATE_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_date"
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_hospital_daily"


def read_hospital_silver() -> DataFrame:
    """Read hospital admission data from Silver."""

    logger.info("Reading hospital Silver table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def prepare_daily_data(
    hospital_silver_df: DataFrame,
) -> DataFrame:
    """
    Select daily hospital records.

    Daily records are identified by the presence of a date.
    """

    required_columns = [
        "country",
        "indicator",
        "date",
        "value",
        "source",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in hospital_silver_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required hospital columns: {missing_columns}"
        )

    return hospital_silver_df.filter(
        F.col("date").isNotNull()
    )


def enrich_with_dimensions(
    hospital_daily_df: DataFrame,
) -> DataFrame:
    """Join daily hospital records with country and date dimensions."""

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_date_df = spark.table(DATE_DIM_TABLE)

    return (
        hospital_daily_df.alias("h")
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


def validate_fact_hospital_daily(
    fact_df: DataFrame,
) -> None:
    """Validate daily hospital fact records."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError("Daily hospital fact contains no records.")

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

    if null_country_keys > 0:
        raise ValueError(
            f"Found {null_country_keys} unmatched country dimension keys."
        )

    if null_date_keys > 0:
        raise ValueError(
            f"Found {null_date_keys} unmatched date dimension keys."
        )

    if null_country_names > 0 or null_indicators > 0:
        raise ValueError(
            "Null country names or indicators found in daily fact."
        )

    if negative_values > 0:
        raise ValueError(
            f"Found {negative_values} negative hospital values."
        )

    duplicate_count = (
        fact_df
        .groupBy("country_name", "date_key", "indicator")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate daily fact records."
        )

    logger.info(
        "Daily hospital fact validation passed. Rows: %s",
        total_records,
    )


def publish_fact_hospital_daily(
    fact_df: DataFrame,
) -> None:
    """Publish daily hospital fact data as a Delta table."""

    source_count = fact_df.count()

    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Row count mismatch: source={source_count}, "
            f"target={target_count}"
        )

    logger.info(
        "Daily hospital fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_hospital_daily() -> None:
    """Execute the daily hospital fact transformation."""

    hospital_silver_df = read_hospital_silver()

    hospital_daily_df = prepare_daily_data(hospital_silver_df)

    fact_df = enrich_with_dimensions(hospital_daily_df)

    validate_fact_hospital_daily(fact_df)

    publish_fact_hospital_daily(fact_df)
