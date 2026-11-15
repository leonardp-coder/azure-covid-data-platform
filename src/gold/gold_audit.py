
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME
from src.common.logging_utils import get_logger


logger = get_logger(__name__)


GOLD_TABLES = [
    "dim_country",
    "dim_date",
    "dim_age_group",
    "fact_covid_daily",
    "fact_hospital_daily",
    "fact_hospital_weekly",
    "fact_population_age",
]


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
    "fact_hospital_daily": [
        "value",
    ],
    "fact_hospital_weekly": [
        "value",
    ],
    "fact_population_age": [
        "population_percentage",
    ],
}

def validate_gold_table_columns(
    gold_tables: Dict[str, DataFrame],
) -> None:
    """
    Validate required columns across Gold tables before auditing.
    """

    required_columns = {
        "dim_country": ["country_key"],
        "dim_date": ["date_key"],
        "dim_age_group": ["age_group_key"],
        "fact_covid_daily": [
            "country_key",
            "date_key",
            "geography_name",
            "confirmed_cases",
            "new_deaths",
        ],
        "fact_hospital_daily": [
            "country_key",
            "date_key",
            "country_name",
            "indicator",
            "value",
        ],
        "fact_hospital_weekly": [
            "country_key",
            "country_name",
            "year_week",
            "indicator",
            "value",
        ],
        "fact_population_age": [
            "country_key",
            "age_group_key",
            "geo_code",
            "year",
            "population_percentage",
        ],
    }

    for table_name, required in required_columns.items():
        missing = [
            column
            for column in required
            if column not in gold_tables[table_name].columns
        ]

        if missing:
            raise ValueError(
                f"Gold table {table_name} is missing columns: {missing}"
            )




def read_gold_tables(
    spark: SparkSession,
) -> Dict[str, DataFrame]:
    """Read all Gold tables required for auditing."""

    logger.info("Reading Gold tables for audit")

    return {
        table_name: spark.table(
            f"{GOLD_SCHEMA_NAME}.{table_name}"
        )
        for table_name in GOLD_TABLES
    }


def audit_row_counts(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Compare actual Gold table counts with project baselines."""

    results = []

    for table_name, df in gold_tables.items():
        actual_count = df.count()
        expected_count = EXPECTED_COUNTS[table_name]

        results.append(
            (
                table_name,
                expected_count,
                actual_count,
                "PASS"
                if actual_count == expected_count
                else "FAIL",
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


def audit_dimension_keys(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Check dimension keys for nulls and duplicates."""

    results = []

    for table_name, key_column in DIMENSION_KEYS.items():
        df = gold_tables[table_name]

        total_records = df.count()

        null_keys = df.filter(
            F.col(key_column).isNull()
        ).count()

        distinct_keys = df.select(
            key_column
        ).distinct().count()

        duplicate_keys = (
            total_records - null_keys - distinct_keys
        )

        results.append(
            (
                table_name,
                total_records,
                null_keys,
                duplicate_keys,
                "PASS"
                if null_keys == 0 and duplicate_keys == 0
                else "FAIL",
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


def audit_foreign_keys(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Identify non-null foreign keys without matching dimensions."""

    dimension_country = gold_tables["dim_country"]
    dimension_date = gold_tables["dim_date"]
    dimension_age_group = gold_tables["dim_age_group"]

    fact_tables = {
        "fact_covid_daily": gold_tables["fact_covid_daily"],
        "fact_hospital_daily": gold_tables["fact_hospital_daily"],
        "fact_hospital_weekly": gold_tables["fact_hospital_weekly"],
        "fact_population_age": gold_tables["fact_population_age"],
    }

    checks = [
        (
            "fact_covid_daily",
            "country_key",
            dimension_country,
        ),
        (
            "fact_covid_daily",
            "date_key",
            dimension_date,
        ),
        (
            "fact_hospital_daily",
            "country_key",
            dimension_country,
        ),
        (
            "fact_hospital_daily",
            "date_key",
            dimension_date,
        ),
        (
            "fact_hospital_weekly",
            "country_key",
            dimension_country,
        ),
        (
            "fact_population_age",
            "country_key",
            dimension_country,
        ),
        (
            "fact_population_age",
            "age_group_key",
            dimension_age_group,
        ),
    ]

    results = []

    for fact_name, key_column, dimension_df in checks:
        fact_df = fact_tables[fact_name]

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

        results.append(
            (
                fact_name,
                key_column,
                orphan_count,
                "PASS" if orphan_count == 0 else "FAIL",
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


def audit_fact_grain(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Identify duplicate records at each fact table's business grain."""

    results = []

    for table_name, grain_columns in FACT_GRAIN_CHECKS.items():
        df = gold_tables[table_name]

        duplicate_count = (
            df.groupBy(*grain_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        results.append(
            (
                table_name,
                ", ".join(grain_columns),
                duplicate_count,
                "PASS" if duplicate_count == 0 else "FAIL",
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


def audit_fact_measures(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Profile null and negative values in fact measures."""

    results = []

    for table_name, measures in MEASURE_CHECKS.items():
        df = gold_tables[table_name]

        for measure in measures:
            metrics = df.agg(
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
                    metrics["total_records"],
                    metrics["null_values"],
                    metrics["negative_values"],
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


def audit_population_percentages(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
) -> DataFrame:
    """Check population percentages against the 0-100 range."""

    population_df = gold_tables["fact_population_age"]

    return population_df.agg(
        F.min("population_percentage").alias(
            "minimum_percentage"
        ),
        F.max("population_percentage").alias(
            "maximum_percentage"
        ),
        F.count(
            F.when(
                (F.col("population_percentage") < 0)
                | (F.col("population_percentage") > 100),
                1,
            )
        ).alias("invalid_percentages"),
    )


def run_gold_audit(
    spark: SparkSession,
) -> Dict[str, DataFrame]:
    """Execute all Gold layer audit checks."""

    logger.info("Starting Gold layer audit")

    gold_tables = read_gold_tables(spark)

    validate_gold_table_columns(gold_tables)

    audit_results = {
        "row_counts": audit_row_counts(
            spark, gold_tables
        ),
        "dimension_keys": audit_dimension_keys(
            spark, gold_tables
        ),
        "foreign_keys": audit_foreign_keys(
            spark, gold_tables
        ),
        "fact_grain": audit_fact_grain(
            spark, gold_tables
        ),
        "fact_measures": audit_fact_measures(
            spark, gold_tables
        ),
        "population_percentages": audit_population_percentages(
            spark, gold_tables
        ),
    }

    logger.info("Gold layer audit completed")

    return audit_results
