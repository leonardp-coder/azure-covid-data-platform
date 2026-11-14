
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["hospital_admissions"]
COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_hospital_weekly"


def read_hospital_silver() -> DataFrame:
    """Read hospital data from the Silver layer."""

    logger.info("Reading hospital Silver table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def prepare_weekly_data(
    hospital_silver_df: DataFrame,
) -> DataFrame:
    """Select weekly hospital records."""

    required_columns = [
        "country",
        "year_week",
        "indicator",
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
        F.col("year_week").isNotNull()
    )


def enrich_with_country_dimension(
    hospital_weekly_df: DataFrame,
) -> DataFrame:
    """
    Join weekly hospital records with country dimension.

    Extract ISO year and ISO week from the original year_week field.
    """

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)

    return (
        hospital_weekly_df.alias("h")
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


def validate_fact_hospital_weekly(
    fact_df: DataFrame,
) -> None:
    """Validate weekly hospital fact records."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError("Weekly hospital fact contains no records.")

    invalid_week_format = fact_df.filter(
        ~F.col("year_week").rlike(r"^\d{4}-W\d{2}$")
    ).count()

    if invalid_week_format > 0:
        raise ValueError(
            f"Found {invalid_week_format} invalid weekly period values."
        )

    invalid_iso_values = fact_df.filter(
        (F.col("iso_week") < 1)
        | (F.col("iso_week") > 53)
        | F.col("iso_year").isNull()
    ).count()

    if invalid_iso_values > 0:
        raise ValueError(
            f"Found {invalid_iso_values} invalid ISO year/week values."
        )

    null_country_keys = fact_df.filter(
        F.col("country_key").isNull()
    ).count()

    null_values = fact_df.filter(
        F.col("value").isNull()
    ).count()

    negative_values = fact_df.filter(
        F.col("value") < 0
    ).count()

    duplicate_count = (
        fact_df
        .groupBy("country_name", "year_week", "indicator")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    logger.info(
        "Weekly hospital validation: rows=%s, "
        "null_country_keys=%s, null_values=%s, negative_values=%s",
        total_records,
        null_country_keys,
        null_values,
        negative_values,
    )

    if negative_values > 0:
        raise ValueError("Negative weekly hospital values detected.")

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate weekly fact records."
        )


def publish_fact_hospital_weekly(
    fact_df: DataFrame,
) -> None:
    """Publish weekly hospital fact as a Delta table."""

    source_count = fact_df.count()

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
            f"Weekly fact row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Weekly hospital fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_hospital_weekly() -> None:
    """Execute weekly hospital fact transformation."""

    hospital_silver_df = read_hospital_silver()

    hospital_weekly_df = prepare_weekly_data(hospital_silver_df)

    fact_df = enrich_with_country_dimension(hospital_weekly_df)

    validate_fact_hospital_weekly(fact_df)

    publish_fact_hospital_weekly(fact_df)
