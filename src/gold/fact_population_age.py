
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["population_by_age"]
COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
AGE_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_age_group"
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_population_age"


def read_population_silver() -> DataFrame:
    """Read population data from the Silver layer."""

    logger.info("Reading population Silver table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def enrich_population_data(
    population_silver_df: DataFrame,
) -> DataFrame:
    """
    Enrich population data with country and age group keys.

    Country dimension matching uses the ISO alpha-2 code.
    """

    required_columns = [
        "age_indicator",
        "geo_code",
        "geography_type",
        "year",
        "population_percentage",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in population_silver_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required population columns: {missing_columns}"
        )

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_age_group_df = spark.table(AGE_DIM_TABLE)

    return (
        population_silver_df.alias("p")
        .join(
            dim_age_group_df.alias("a"),
            F.col("p.age_indicator") == F.col("a.age_indicator"),
            "left",
        )
        .join(
            dim_country_df.alias("c"),
            F.col("p.geo_code") == F.col("c.country_code_2_digit"),
            "left",
        )
        .select(
            F.col("c.country_key").alias("country_key"),
            F.col("a.age_group_key").alias("age_group_key"),
            F.col("p.geo_code"),
            F.col("p.geography_type"),
            F.col("p.year"),
            F.col("p.population_percentage"),
        )
    )


def validate_fact_population_age(
    fact_df: DataFrame,
) -> None:
    """Validate population fact records and business grain."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError("Population age fact contains no records.")

    null_age_keys = fact_df.filter(
        F.col("age_group_key").isNull()
    ).count()

    if null_age_keys > 0:
        raise ValueError(
            f"Found {null_age_keys} records without age group keys."
        )

    null_country_keys = fact_df.filter(
        F.col("country_key").isNull()
    ).count()

    null_population_values = fact_df.filter(
        F.col("population_percentage").isNull()
    ).count()

    negative_values = fact_df.filter(
        F.col("population_percentage") < 0
    ).count()

    above_100_values = fact_df.filter(
        F.col("population_percentage") > 100
    ).count()

    duplicate_count = (
        fact_df
        .groupBy("geo_code", "age_group_key", "year")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if negative_values > 0 or above_100_values > 0:
        raise ValueError(
            "Invalid population percentages detected. "
            "Values must be between 0 and 100."
        )

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate population fact records."
        )

    logger.info(
        "Population fact validation passed. "
        "Rows=%s, null_country_keys=%s, null_population_values=%s",
        total_records,
        null_country_keys,
        null_population_values,
    )


def publish_fact_population_age(
    fact_df: DataFrame,
) -> None:
    """Publish population age fact data as a Delta table."""

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
            f"Population fact row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Population age fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_population_age() -> None:
    """Execute the population age fact transformation."""

    population_silver_df = read_population_silver()

    fact_df = enrich_population_data(population_silver_df)

    validate_fact_population_age(fact_df)

    publish_fact_population_age(fact_df)
