
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["cases_deaths"]
COUNTRY_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_country"
DATE_DIM_TABLE = f"{GOLD_SCHEMA_NAME}.dim_date"
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.fact_covid_daily"

EXPECTED_INDICATORS = ["confirmed cases", "deaths"]


def read_source_data() -> DataFrame:
    """Read Silver cases and deaths data."""

    logger.info("Reading source table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def aggregate_cases_deaths(
    cases_silver_df: DataFrame,
) -> DataFrame:
    """
    Pivot cases and deaths indicators into separate measures.

    Original grain:
    country + country_code + continent + population + date
    + geography_type.
    """

    required_columns = [
        "country",
        "country_code",
        "continent",
        "population",
        "date",
        "geography_type",
        "indicator",
        "daily_count",
        "rate_14_day",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in cases_silver_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required Silver columns: {missing_columns}"
        )

    return (
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


def enrich_with_dimensions(
    cases_pivot_df: DataFrame,
) -> DataFrame:
    """
    Enrich the aggregated data with country and date dimension keys.

    Left joins preserve regional aggregates and unmatched source rows.
    """

    dim_country_df = spark.table(COUNTRY_DIM_TABLE)
    dim_date_df = spark.table(DATE_DIM_TABLE)

    return (
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


def validate_fact_covid_daily(
    fact_df: DataFrame,
) -> None:
    """Validate fact records, keys and measures."""

    total_records = fact_df.count()

    if total_records == 0:
        raise ValueError("COVID daily fact table contains no records.")

    null_date_keys = fact_df.filter(
        F.col("date_key").isNull()
    ).count()

    if null_date_keys > 0:
        raise ValueError(
            f"Found {null_date_keys} records with null date keys."
        )

    null_country_keys = fact_df.filter(
        F.col("country_key").isNull()
    ).count()

    regional_null_keys = fact_df.filter(
        (F.col("geography_type") != "countries")
        & F.col("country_key").isNull()
    ).count()

    country_null_keys = fact_df.filter(
        (F.col("geography_type") == "countries")
        & F.col("country_key").isNull()
    ).count()

    if country_null_keys > 0:
        raise ValueError(
            f"Found {country_null_keys} country records without "
            "a matching country dimension key."
        )

    duplicate_grain_count = (
        fact_df
        .groupBy("geography_name", "date_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_grain_count > 0:
        raise ValueError(
            f"Found {duplicate_grain_count} duplicate geography/date keys."
        )

    negative_cases = fact_df.filter(
        F.col("confirmed_cases") < 0
    ).count()

    negative_deaths = fact_df.filter(
        F.col("new_deaths") < 0
    ).count()

    if negative_cases > 0 or negative_deaths > 0:
        raise ValueError(
            "Negative values detected in COVID fact measures."
        )

    logger.info(
        "COVID daily fact validation passed. "
        "Rows=%s, null_country_keys=%s, regional_null_keys=%s",
        total_records,
        null_country_keys,
        regional_null_keys,
    )


def publish_fact_covid_daily(
    fact_df: DataFrame,
) -> None:
    """Publish the COVID daily fact table as Delta."""

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
            f"Fact row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "COVID daily fact published successfully. Rows: %s",
        target_count,
    )


def run_fact_covid_daily() -> None:
    """Execute the COVID daily fact transformation."""

    cases_silver_df = read_source_data()

    cases_pivot_df = aggregate_cases_deaths(cases_silver_df)

    fact_df = enrich_with_dimensions(cases_pivot_df)

    validate_fact_covid_daily(fact_df)

    publish_fact_covid_daily(fact_df)
