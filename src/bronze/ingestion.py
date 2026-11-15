
"""
Bronze ingestion module for the Azure COVID Data Engineering project.

Responsibilities:
    1. Read source CSV and compressed TSV datasets.
    2. Add ingestion metadata.
    3. Apply dataset-specific source preparation.
    4. Publish datasets as Bronze Delta tables.
    5. Reconcile source and published record counts.

Note:
    This module implements full-refresh Bronze ingestion.
    It is a portfolio refactor and has not been executed
    or independently validated against the Azure workspace.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    BRONZE_SCHEMA,
    BRONZE_TABLES,
    CATALOG,
    LANDING_PATH,
)

from src.common.logging_utils import get_logger


logger = get_logger(__name__)


# ------------------------------------------------------------
# SOURCE DATASET CONFIGURATION
# ------------------------------------------------------------

SOURCE_FILES = {
    "cases_deaths": {
        "path": f"{LANDING_PATH}/cases_deaths/cases_deaths.csv",
        "file_name": "cases_deaths.csv",
        "format": "csv",
    },
    "hospital_admissions": {
        "path": f"{LANDING_PATH}/hospital_admissions/hospital_admissions.csv",
        "file_name": "hospital_admissions.csv",
        "format": "csv",
    },
    "country_lookup": {
        "path": f"{LANDING_PATH}/country_lookup/country_lookup.csv",
        "file_name": "country_lookup.csv",
        "format": "csv",
    },
    "population_by_age": {
        "path": f"{LANDING_PATH}/population/population_by_age.tsv.gz",
        "file_name": "population_by_age.tsv.gz",
        "format": "tsv",
    },
}


POPULATION_COLUMNS = [
    "age_geo",
    "year_2008",
    "year_2009",
    "year_2010",
    "year_2011",
    "year_2012",
    "year_2013",
    "year_2014",
    "year_2015",
    "year_2016",
    "year_2017",
    "year_2018",
    "year_2019",
]


# ------------------------------------------------------------
# SOURCE READING FUNCTIONS
# ------------------------------------------------------------

def read_csv_file(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Read a CSV source file with its header and inferred schema.
    """

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )


def read_population_file(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Read the compressed tab-separated population dataset.

    Spark automatically handles supported gzip-compressed files.
    """

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", "\t")
        .csv(file_path)
    )


def read_source_dataset(
    spark: SparkSession,
    dataset_name: str,
) -> DataFrame:
    """
    Select the appropriate reader based on dataset configuration.
    """

    if dataset_name not in SOURCE_FILES:
        raise ValueError(
            f"Unsupported source dataset: {dataset_name}"
        )

    source = SOURCE_FILES[dataset_name]

    logger.info(
        "Reading source dataset: %s",
        dataset_name,
    )

    if source["format"] == "csv":
        return read_csv_file(spark, source["path"])

    if source["format"] == "tsv":
        return read_population_file(spark, source["path"])

    raise ValueError(
        f"Unsupported file format: {source['format']}"
    )


# ------------------------------------------------------------
# INGESTION METADATA AND SOURCE PREPARATION
# ------------------------------------------------------------

def add_ingestion_metadata(
    df: DataFrame,
    source_file: str,
) -> DataFrame:
    """
    Add standardized ingestion timestamp and source filename.
    """

    return (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
    )


def prepare_population_dataset(
    df: DataFrame,
) -> DataFrame:
    """
    Apply the established population column naming convention.
    """

    if len(df.columns) != len(POPULATION_COLUMNS):
        raise ValueError(
            "Population schema mismatch. "
            f"Expected {len(POPULATION_COLUMNS)} columns, "
            f"found {len(df.columns)}."
        )

    return df.toDF(*POPULATION_COLUMNS)


def prepare_bronze_dataset(
    dataset_name: str,
    df: DataFrame,
) -> DataFrame:
    """
    Apply dataset-specific preparation before Bronze publishing.
    """

    if dataset_name not in SOURCE_FILES:
        raise ValueError(
            f"Unsupported source dataset: {dataset_name}"
        )

    if dataset_name == "population_by_age":
        df = prepare_population_dataset(df)

    source_file = SOURCE_FILES[dataset_name]["file_name"]

    return add_ingestion_metadata(
        df,
        source_file,
    )


# ------------------------------------------------------------
# BRONZE DELTA PUBLISHING
# ------------------------------------------------------------

def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> int:
    """
    Publish a DataFrame to a Bronze Delta table.

    Uses overwrite mode because this project implements
    a full-refresh batch ingestion pattern.

    Returns the number of records published.
    """

    source_count = df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        table_name,
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    published_count = spark.table(table_name).count()

    if source_count != published_count:
        raise RuntimeError(
            f"Record count mismatch for {table_name}. "
            f"Source: {source_count}, "
            f"Published: {published_count}"
        )

    logger.info(
        "Successfully published %s records to %s",
        published_count,
        table_name,
    )

    return published_count


# ------------------------------------------------------------
# BRONZE INGESTION ORCHESTRATION
# ------------------------------------------------------------

def ingest_bronze_datasets(
    spark: SparkSession,
) -> Dict[str, int]:
    """
    Execute the complete Bronze ingestion process.

    Reads, prepares and publishes all configured datasets.

    Returns:
        Dictionary containing published record counts
        for each dataset.
    """

    logger.info("Starting Bronze ingestion process.")

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}"
    )

    ingestion_results: Dict[str, int] = {}

    try:
        for dataset_name in SOURCE_FILES:

            logger.info(
                "Processing dataset: %s",
                dataset_name,
            )

            source_df = read_source_dataset(
                spark,
                dataset_name,
            )

            bronze_df = prepare_bronze_dataset(
                dataset_name,
                source_df,
            )

            target_table = BRONZE_TABLES[dataset_name]

            published_count = write_bronze_table(
                spark,
                bronze_df,
                target_table,
            )

            ingestion_results[dataset_name] = published_count

        logger.info(
            "Bronze ingestion completed successfully. "
            "Dataset results: %s",
            ingestion_results,
        )

        return ingestion_results

    except Exception:
        logger.exception("Bronze ingestion failed.")
        raise
