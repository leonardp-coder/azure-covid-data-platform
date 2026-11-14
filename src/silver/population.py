
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import BRONZE_TABLES, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

START_YEAR = 2008
END_YEAR = 2019

YEAR_COLUMNS = [
    f"year_{year}"
    for year in range(START_YEAR, END_YEAR + 1)
]

AGGREGATE_GEO_CODES = [
    "EA18",
    "EA19",
    "EU27_2007",
    "EU27_2020",
    "EU28",
]

REVIEW_GEO_CODES = ["FX", "XK"]

REQUIRED_COLUMNS = ["age_geo"] + YEAR_COLUMNS


def read_bronze_population(spark) -> DataFrame:
    """Read the Bronze population dataset."""
    table_name = BRONZE_TABLES["population_by_age"]

    logger.info("Reading Bronze table: %s", table_name)

    return spark.table(table_name)


def validate_population_columns(df: DataFrame) -> None:
    """Ensure the expected source columns are available."""
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Population dataset is missing columns: {missing_columns}"
        )


def split_age_geography(df: DataFrame) -> DataFrame:
    """Separate the age indicator and geography code."""
    return (
        df
        .withColumn(
            "age_indicator",
            F.trim(F.split(F.col("age_geo"), ",").getItem(0))
        )
        .withColumn(
            "geo_code",
            F.upper(
                F.trim(F.split(F.col("age_geo"), ",").getItem(1))
            )
        )
        .withColumn(
            "age_group",
            F.regexp_replace(F.col("age_indicator"), "^PC_Y", "")
        )
    )


def classify_geography(df: DataFrame) -> DataFrame:
    """Classify regional aggregates and geography codes requiring review."""
    return (
        df
        .withColumn(
            "geography_type",
            F.when(
                F.col("geo_code").isin(AGGREGATE_GEO_CODES),
                F.lit("REGIONAL_AGGREGATE")
            )
            .when(
                F.col("geo_code").isin(REVIEW_GEO_CODES),
                F.lit("requires_review")
            )
            .otherwise(F.lit("Country_candidate"))
        )
    )


def unpivot_population_years(df: DataFrame) -> DataFrame:
    """Convert the 2008–2019 year columns into rows."""
    id_columns = [
        "age_geo",
        "age_indicator",
        "age_group",
        "geo_code",
        "geography_type",
    ]

    # Support either metadata naming convention while the
    # Bronze ingestion module is being aligned.
    if "_ingested_at" in df.columns:
        id_columns.append("_ingested_at")
    elif "ingested_at" in df.columns:
        df = df.withColumnRenamed("ingested_at", "_ingested_at")
        id_columns.append("_ingested_at")

    if "_source_file" in df.columns:
        id_columns.append("_source_file")

    return (
        df
        .unpivot(
            ids=id_columns,
            values=YEAR_COLUMNS,
            variableColumnName="year_column",
            valueColumnName="raw_population_value",
        )
        .withColumn(
            "year",
            F.regexp_extract(
                F.col("year_column"),
                r"(\d{4})$",
                1
            ).cast("int")
        )
        .drop("year_column")
    )


def clean_population_values(df: DataFrame) -> DataFrame:
    """Clean numeric population percentages and retain source flags."""
    return (
        df
        .withColumn(
            "raw_population_value",
            F.trim(F.col("raw_population_value"))
        )
        .withColumn(
            "source_flag",
            F.when(
                F.col("raw_population_value").rlike(
                    r"(?i)\s*[a-z]+\s*$"
                ),
                F.regexp_extract(
                    F.col("raw_population_value"),
                    r"(?i)([a-z]+)\s*$",
                    1
                )
            )
        )
        .withColumn(
            "population_percentage",
            F.when(
                F.col("raw_population_value").isNull()
                | (F.col("raw_population_value") == ":"),
                F.lit(None).cast("double")
            )
            .otherwise(
                F.regexp_replace(
                    F.col("raw_population_value"),
                    r"(?i)\s*[a-z]+\s*$",
                    ""
                ).cast("double")
            )
        )
    )


def validate_population_data(df: DataFrame) -> None:
    """Run data-quality checks on the transformed population data."""
    metrics = (
        df
        .agg(
            F.count("*").alias("total_records"),
            F.count("population_percentage").alias("numeric_values"),
            F.count(
                F.when(F.col("population_percentage").isNull(), 1)
            ).alias("null_values"),
            F.count(
                F.when(
                    F.col("raw_population_value").isNotNull()
                    & (F.col("raw_population_value") != ":")
                    & F.col("population_percentage").isNull(),
                    1
                )
            ).alias("invalid_numeric_values"),
            F.count(
                F.when(F.col("source_flag").isNotNull(), 1)
            ).alias("flagged_values"),
            F.count(
                F.when(F.col("population_percentage") < 0, 1)
            ).alias("negative_values"),
            F.count(
                F.when(F.col("population_percentage") > 100, 1)
            ).alias("values_above_100"),
        )
        .first()
        .asDict()
    )

    logger.info("Population quality metrics: %s", metrics)

    if metrics["invalid_numeric_values"] > 0:
        raise ValueError(
            "Population data contains values that could not be converted "
            "to numeric percentages."
        )

    if metrics["negative_values"] > 0:
        raise ValueError(
            "Population data contains negative percentages."
        )

    if metrics["values_above_100"] > 0:
        raise ValueError(
            "Population data contains percentages above 100."
        )

    # Check that each original age/geography record has
    # exactly one row for each year from 2008 through 2019.
    incomplete_years = (
        df
        .groupBy("age_geo")
        .agg(
            F.count("*").alias("year_records"),
            F.countDistinct("year").alias("distinct_years"),
        )
        .filter(
            (F.col("year_records") != len(YEAR_COLUMNS))
            | (F.col("distinct_years") != len(YEAR_COLUMNS))
        )
        .count()
    )

    if incomplete_years > 0:
        logger.warning(
            "%s age/geography records have incomplete year coverage.",
            incomplete_years
        )


def prepare_silver_population(df: DataFrame) -> DataFrame:
    """Apply the complete population Silver transformation."""
    validate_population_columns(df)

    transformed_df = split_age_geography(df)
    transformed_df = classify_geography(transformed_df)
    transformed_df = unpivot_population_years(transformed_df)
    transformed_df = clean_population_values(transformed_df)

    validate_population_data(transformed_df)

    output_columns = [
        "age_indicator",
        "age_group",
        "geo_code",
        "geography_type",
        "year",
        "population_percentage",
    ]

    if "_ingested_at" in transformed_df.columns:
        output_columns.append("_ingested_at")

    if "_source_file" in transformed_df.columns:
        output_columns.append("_source_file")

    output_columns.append("source_flag")

    return transformed_df.select(*output_columns)


def write_silver_population(df: DataFrame, spark) -> None:
    """Write the transformed dataset to the Silver Delta table."""
    target_table = SILVER_TABLES["population_by_age"]

    source_count = df.count()

    logger.info(
        "Writing %s records to %s",
        source_count,
        target_table
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    target_count = spark.table(target_table).count()

    if source_count != target_count:
        raise ValueError(
            f"Population row-count mismatch. "
            f"Source: {source_count}, Target: {target_count}"
        )

    logger.info(
        "Silver population load completed. Rows written: %s",
        target_count
    )


def process_population(spark) -> None:
    """Run the complete Bronze-to-Silver population process."""
    logger.info("Starting Silver population transformation.")

    bronze_df = read_bronze_population(spark)
    silver_df = prepare_silver_population(bronze_df)

    write_silver_population(silver_df, spark)

    logger.info("Silver population transformation completed.")
