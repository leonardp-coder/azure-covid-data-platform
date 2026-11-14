
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

GOLD_TABLES = {
    "dim_country": f"{GOLD_SCHEMA_NAME}.dim_country",
    "dim_date": f"{GOLD_SCHEMA_NAME}.dim_date",
    "dim_age_group": f"{GOLD_SCHEMA_NAME}.dim_age_group",
    "fact_covid_daily": f"{GOLD_SCHEMA_NAME}.fact_covid_daily",
    "fact_hospital_daily": f"{GOLD_SCHEMA_NAME}.fact_hospital_daily",
    "fact_hospital_weekly": f"{GOLD_SCHEMA_NAME}.fact_hospital_weekly",
    "fact_population_age": f"{GOLD_SCHEMA_NAME}.fact_population_age",
}

EXPECTED_COUNTS = {
    "dim_country": 211,
    "dim_date": 298,
    "dim_age_group": 6,
    "fact_covid_daily": 64635,
    "fact_hospital_daily": 8110,
    "fact_hospital_weekly": 867,
    "fact_population_age": 3816,
}

DIMENSION_KEYS = {
    "dim_country": "country_key",
    "dim_date": "date_key",
    "dim_age_group": "age_group_key",
}

FOREIGN_KEY_CHECKS = [
    ("fact_covid_daily", "country_key", "dim_country"),
    ("fact_covid_daily", "date_key", "dim_date"),
    ("fact_hospital_daily", "country_key", "dim_country"),
    ("fact_hospital_daily", "date_key", "dim_date"),
    ("fact_hospital_weekly", "country_key", "dim_country"),
    ("fact_population_age", "country_key", "dim_country"),
    ("fact_population_age", "age_group_key", "dim_age_group"),
]

FACT_GRAIN_CHECKS = {
    "fact_covid_daily": [
        "geography_name",
        "date_key",
    ],
    "fact_hospital_daily": [
        "country_name",
        "date_key",
        "indicator",
    ],
    "fact_hospital_weekly": [
        "country_name",
        "year_week",
        "indicator",
    ],
    "fact_population_age": [
        "geo_code",
        "age_group_key",
        "year",
    ],
}

MEASURE_CHECKS = {
    "fact_covid_daily": [
        "confirmed_cases",
        "new_deaths",
    ],
    "fact_hospital_daily": ["value"],
    "fact_hospital_weekly": ["value"],
    "fact_population_age": ["population_percentage"],
}


def read_gold_tables() -> dict:
    """Load all seven Gold tables."""

    logger.info("Reading Gold tables for audit.")

    return {
        table_name: spark.table(table_path)
        for table_name, table_path in GOLD_TABLES.items()
    }


def audit_row_counts(tables: dict) -> DataFrame:
    """Compare actual table counts with expected project counts."""

    results = []

    for table_name, df in tables.items():
        actual_count = df.count()
        expected_count = EXPECTED_COUNTS[table_name]

        status = (
            "PASS"
            if actual_count == expected_count
            else "FAIL"
        )

        results.append(
            (
                table_name,
                expected_count,
                actual_count,
                status,
            )
        )

    return spark.createDataFrame(
        results,
        [
            "table_name",
            "expected_count",
            "actual_count",
            "audit_status",
        ],
    )


def audit_dimension_keys(tables: dict) -> DataFrame:
    """Check dimension keys for nulls and duplicates."""

    results = []

    for table_name, key_column in DIMENSION_KEYS.items():
        df = tables[table_name]

        total_records = df.count()

        null_keys = df.filter(
            F.col(key_column).isNull()
        ).count()

        distinct_keys = (
            df.select(key_column)
            .distinct()
            .count()
        )

        duplicate_keys = (
            total_records - null_keys - distinct_keys
        )

        status = (
            "PASS"
            if null_keys == 0 and duplicate_keys == 0
            else "FAIL"
        )

        results.append(
            (
                table_name,
                total_records,
                null_keys,
                duplicate_keys,
                status,
            )
        )

    return spark.createDataFrame(
        results,
        [
            "dimension",
            "total_records",
            "null_keys",
            "duplicate_keys",
            "audit_status",
        ],
    )


def audit_foreign_keys(tables: dict) -> DataFrame:
    """Find non-null fact keys without a matching dimension key."""

    results = []

    for fact_name, key_column, dimension_name in FOREIGN_KEY_CHECKS:
        fact_df = tables[fact_name]
        dimension_df = tables[dimension_name]

        orphan_count = (
            fact_df
            .filter(F.col(key_column).isNotNull())
            .select(key_column)
            .distinct()
            .join(
                dimension_df.select(key_column).distinct(),
                on=key_column,
                how="left_anti",
            )
            .count()
        )

        status = "PASS" if orphan_count == 0 else "FAIL"

        results.append(
            (
                fact_name,
                key_column,
                orphan_count,
                status,
            )
        )

    return spark.createDataFrame(
        results,
        [
            "fact_table",
            "foreign_key",
            "orphan_keys",
            "audit_status",
        ],
    )


def audit_fact_grain(tables: dict) -> DataFrame:
    """Check uniqueness at each fact table's business grain."""

    results = []

    for table_name, grain_columns in FACT_GRAIN_CHECKS.items():
        df = tables[table_name]

        duplicate_count = (
            df.groupBy(*grain_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        status = (
            "PASS"
            if duplicate_count == 0
            else "FAIL"
        )

        results.append(
            (
                table_name,
                ", ".join(grain_columns),
                duplicate_count,
                status,
            )
        )

    return spark.createDataFrame(
        results,
        [
            "fact_table",
            "business_grain",
            "duplicate_groups",
            "audit_status",
        ],
    )


def audit_measures(tables: dict) -> DataFrame:
    """Check fact measures for null and negative values."""

    results = []

    for table_name, measures in MEASURE_CHECKS.items():
        df = tables[table_name]

        for measure in measures:
            result = df.agg(
                F.count("*").alias("total_records"),
                F.count(
                    F.when(F.col(measure).isNull(), 1)
                ).alias("null_values"),
                F.count(
                    F.when(F.col(measure) < 0, 1)
                ).alias("negative_values"),
            ).first()

            results.append(
                (
                    table_name,
                    measure,
                    result["total_records"],
                    result["null_values"],
                    result["negative_values"],
                )
            )

    return spark.createDataFrame(
        results,
        [
            "fact_table",
            "measure",
            "total_records",
            "null_values",
            "negative_values",
        ],
    )


def audit_population_percentage(
    population_df: DataFrame,
) -> DataFrame:
    """Check population percentages against the 0–100 range."""

    return population_df.agg(
        F.min("population_percentage").alias("minimum_percentage"),
        F.max("population_percentage").alias("maximum_percentage"),
        F.count(
            F.when(
                (F.col("population_percentage") < 0)
                | (F.col("population_percentage") > 100),
                1,
            )
        ).alias("invalid_percentages"),
    )


def run_gold_audit() -> dict:
    """Execute all Gold layer audit checks."""

    logger.info("Starting Gold layer audit.")

    tables = read_gold_tables()

    audit_results = {
        "row_counts": audit_row_counts(tables),
        "dimension_keys": audit_dimension_keys(tables),
        "foreign_keys": audit_foreign_keys(tables),
        "fact_grain": audit_fact_grain(tables),
        "measures": audit_measures(tables),
        "population_percentage": audit_population_percentage(
            tables["fact_population_age"]
        ),
    }

    logger.info("Gold layer audit checks completed.")

    return audit_results
