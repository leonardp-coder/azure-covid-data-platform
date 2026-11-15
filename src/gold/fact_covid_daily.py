
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["cases_deaths"]

COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
DATE_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_date"

TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_covid_daily"


def read_source_data(spark: SparkSession) -> DataFrame:
    """Read COVID cases and deaths from the Silver layer."""

    logger.info("Reading COVID data from %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def aggregate_cases_deaths(
    cases_silver_df: DataFrame,
) -> DataFrame:
    """
    Aggregate confirmed cases and deaths by geography and date.

    Preserves the original notebook's grouping and aggregation logic.
    """

    logger.info("Aggregating COVID cases and deaths")

    cases_pivot_df = (
        cases_silver_df
        .groupBy(
            "country",
            "country_code",
            "continent",
            "population",
            "date",
            "geography_type",
        )
        .agg(
            F.sum(
                F.when(
                    F.col("indicator") == "confirmed cases",
                    F.col("daily_count"),
                ).otherwise(0)
            ).alias("confirmed_cases"),

            F.sum(
                F.when(
                    F.col("indicator") == "deaths",
                    F.col("daily_count"),
                ).otherwise(0)
            ).alias("new_deaths"),

            F.max("rate_14_day").alias("rate_14_day"),
        )
    )

    logger.info(
        "COVID aggregation completed. Records: %s",
        cases_pivot_df.count(),
    )

    return cases_pivot_df


def enrich_with_dimensions(
    spark: SparkSession,
    cases_pivot_df: DataFrame,
) -> DataFrame:
    """
    Enrich aggregated COVID data with country and date dimension keys.
    """

    logger.info("Reading Gold dimensions")

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_date_df = spark.table(DATE_DIM_TABLE)

    logger.info("Joining COVID data with dimensions")

    cases_fact_df = (
        cases_pivot_df.alias("c")
        .join(
            dim_country_df.alias("country"),
            F.col("c.country_code")
            == F.col("country.country_code_3_digit"),
            "left",
        )
        .join(
            dim_date_df.alias("date"),
            F.col("c.date") == F.col("date.full_date"),
            "left",
        )
        .select(
            F.col("country.country_key").alias("country_key"),
            F.col("date.date_key").alias("date_key"),
            F.col("c.country_code").alias("country_code"),
            F.col("c.country").alias("geography_name"),
            F.col("c.continent").alias("continent"),
            F.col("c.geography_type").alias("geography_type"),
            F.col("c.confirmed_cases"),
            F.col("c.new_deaths"),
            F.col("c.rate_14_day"),
            F.col("c.population"),
        )
    )

    return cases_fact_df


def validate_fact_covid_daily(
    fact_df: DataFrame,
) -> None:
    """Validate COVID daily fact records and business grain."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError("COVID daily fact contains no records.")

    null_date_keys = fact_df.filter(
        F.col("date_key").isNull()
    ).count()

    null_country_keys = fact_df.filter(
        (F.col("geography_type") == "countries")
        & F.col("country_key").isNull()
    ).count()

    duplicate_grain = (
        fact_df
        .groupBy("geography_name", "date_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    negative_confirmed_cases = fact_df.filter(
        F.col("confirmed_cases") < 0
    ).count()

    negative_deaths = fact_df.filter(
        F.col("new_deaths") < 0
    ).count()

    if null_date_keys > 0:
        raise ValueError(
            f"COVID fact contains {null_date_keys} null date keys."
        )

    if null_country_keys > 0:
        raise ValueError(
            f"COVID fact contains {null_country_keys} "
            "country records without matching country keys."
        )

    if duplicate_grain > 0:
        raise ValueError(
            f"COVID fact contains {duplicate_grain} duplicate "
            "geography and date combinations."
        )

    if negative_confirmed_cases > 0 or negative_deaths > 0:
        raise ValueError(
            "COVID fact contains negative case or death measures."
        )

    logger.info(
        "COVID daily fact validation passed. Rows: %s",
        total_records,
    )

    logger.info(
        "Regional records with null country keys are permitted."
    )


def publish_fact_covid_daily(
    spark: SparkSession,
    fact_df: DataFrame,
) -> None:
    """Publish COVID daily fact data as a Delta table."""

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
            f"COVID fact row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "COVID daily fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_covid_daily(spark: SparkSession) -> None:
    """Execute the complete COVID daily fact pipeline."""

    cases_silver_df = read_source_data(spark)

    cases_pivot_df = aggregate_cases_deaths(cases_silver_df)

    fact_df = enrich_with_dimensions(spark, cases_pivot_df)

    validate_fact_covid_daily(fact_df)

    publish_fact_covid_daily(spark, fact_df)
