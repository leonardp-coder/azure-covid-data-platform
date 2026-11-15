
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["population_by_age"]

COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
AGE_GROUP_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_age_group"

TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_population_age"


def read_population_silver(
    spark: SparkSession,
) -> DataFrame:
    """Read population data from the Silver layer."""

    logger.info("Reading population data from %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def enrich_with_dimensions(
    spark: SparkSession,
    population_df: DataFrame,
) -> DataFrame:
    """
    Enrich population data with country and age group keys.

    Preserves the original notebook's left joins and join conditions.
    """

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_age_group_df = spark.table(AGE_GROUP_DIM_TABLE)

    logger.info("Joining population data with Gold dimensions")

    fact_df = (
        population_df.alias("p")

        # Join age group dimension
        .join(
            dim_age_group_df.alias("a"),
            F.col("p.age_indicator")
            == F.col("a.age_indicator"),
            "left",
        )

        # Join country dimension using ISO alpha-2 code
        .join(
            dim_country_df.alias("c"),
            F.col("p.geo_code")
            == F.col("c.country_code_2_digit"),
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

    return fact_df


def validate_fact_population_age(
    fact_df: DataFrame,
) -> None:
    """Validate population fact records and business grain."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError(
            "Population age fact contains no records."
        )

    null_age_group_keys = fact_df.filter(
        F.col("age_group_key").isNull()
    ).count()

    negative_percentages = fact_df.filter(
        F.col("population_percentage") < 0
    ).count()

    invalid_percentages = fact_df.filter(
        (F.col("population_percentage") < 0)
        | (F.col("population_percentage") > 100)
    ).count()

    duplicate_grain = (
        fact_df
        .groupBy(
            "geo_code",
            "age_group_key",
            "year",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_age_group_keys > 0:
        raise ValueError(
            f"Population fact contains {null_age_group_keys} "
            "null age group keys."
        )

    if negative_percentages > 0:
        raise ValueError(
            f"Population fact contains {negative_percentages} "
            "negative population percentages."
        )

    if invalid_percentages > 0:
        raise ValueError(
            f"Population fact contains {invalid_percentages} "
            "percentages outside the 0-100 range."
        )

    if duplicate_grain > 0:
        raise ValueError(
            f"Population fact contains {duplicate_grain} "
            "duplicate business-key groups."
        )

    logger.info(
        "Population age fact validation passed. Rows: %s",
        total_records,
    )

    logger.info(
        "Null country keys and null population percentages "
        "are permitted where present in the source data."
    )


def publish_fact_population_age(
    spark: SparkSession,
    fact_df: DataFrame,
) -> None:
    """Publish population age fact data as a Delta table."""

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
            f"Population fact row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Population age fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_population_age(
    spark: SparkSession,
) -> None:
    """Execute the complete population age fact pipeline."""

    population_df = read_population_silver(spark)

    fact_df = enrich_with_dimensions(spark, population_df)

    validate_fact_population_age(fact_df)

    publish_fact_population_age(spark, fact_df)
