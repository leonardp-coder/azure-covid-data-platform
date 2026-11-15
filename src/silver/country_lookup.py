
"""
Silver transformation module for country lookup.

Input:
    Bronze country_lookup Delta table.

Output:
    Silver country_lookup Delta table.

Responsibilities:
    - Standardize country names and geographic codes.
    - Normalize continent and population attributes.
    - Add missing geography reference records.
    - Validate country reference data.
    - Publish the enriched Silver dataset.

This is a portfolio refactor. It has not been executed
or independently validated against the Azure workspace.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    BRONZE_TABLES,
    SILVER_TABLES,
)
from src.common.logging_utils import get_logger


logger = get_logger(__name__)


# ------------------------------------------------------------
# DATASET CONFIGURATION
# ------------------------------------------------------------

REQUIRED_COLUMNS = [
    "country",
    "country_code_2_digit",
    "country_code_3_digit",
    "continent",
    "population",
]


# Reference records required to reconcile source datasets.
# Population is intentionally NULL because it is not supplied here.
ADDITIONAL_COUNTRIES = [
    ("Montserrat", "MS", "MSR", "America", None),
    ("Kosovo", "XK", "RKS", "Europe", None),
    ("Wallis and Futuna", "WF", "WLF", "Oceania", None),
]


# ------------------------------------------------------------
# SOURCE EXTRACTION
# ------------------------------------------------------------

def read_bronze_country_lookup(
    spark: SparkSession,
) -> DataFrame:
    """Read the Bronze country lookup table."""

    table_name = BRONZE_TABLES["country_lookup"]

    logger.info("Reading Bronze table: %s", table_name)

    return spark.table(table_name)


# ------------------------------------------------------------
# SOURCE VALIDATION
# ------------------------------------------------------------

def validate_country_lookup_columns(
    df: DataFrame,
) -> None:
    """Validate the source lookup structure."""

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Country lookup is missing columns: {missing_columns}"
        )


# ------------------------------------------------------------
# COUNTRY LOOKUP STANDARDIZATION
# ------------------------------------------------------------

def standardize_country_lookup(
    df: DataFrame,
) -> DataFrame:
    """Standardize country names, codes, continent and population."""

    return (
        df
        .select(*REQUIRED_COLUMNS)
        .withColumn(
            "country",
            F.trim(F.col("country")),
        )
        .withColumn(
            "country_code_2_digit",
            F.upper(F.trim(F.col("country_code_2_digit"))),
        )
        .withColumn(
            "country_code_3_digit",
            F.upper(F.trim(F.col("country_code_3_digit"))),
        )
        .withColumn(
            "continent",
            F.trim(F.col("continent")),
        )
        .withColumn(
            "population",
            F.col("population").cast("long"),
        )
    )


# ------------------------------------------------------------
# REFERENCE DATA ENRICHMENT
# ------------------------------------------------------------

def add_reference_countries(
    spark: SparkSession,
    df: DataFrame,
) -> DataFrame:
    """Add missing geography references required by the COVID source."""

    additional_df = spark.createDataFrame(
        ADDITIONAL_COUNTRIES,
        schema=df.schema,
    )

    return df.unionByName(additional_df)


# ------------------------------------------------------------
# DATA QUALITY VALIDATION
# ------------------------------------------------------------

def validate_country_lookup(
    df: DataFrame,
) -> None:
    """Validate country reference records and code uniqueness."""

    required_value_columns = [
        "country",
        "country_code_3_digit",
        "continent",
    ]

    null_counts = {
        column: df.filter(
            F.col(column).isNull()
        ).count()
        for column in required_value_columns
    }

    if any(count > 0 for count in null_counts.values()):
        raise ValueError(
            f"Required country lookup fields contain NULLs: {null_counts}"
        )

    duplicate_codes = (
        df
        .groupBy("country_code_3_digit")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_codes > 0:
        raise ValueError(
            f"Found {duplicate_codes} duplicate "
            "three-character country codes."
        )

    duplicate_names = (
        df
        .groupBy("country")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_names > 0:
        logger.warning(
            "Found %s duplicate country names.",
            duplicate_names,
        )

    logger.info(
        "Country lookup validation completed. Records: %s",
        df.count(),
    )


# ------------------------------------------------------------
# SILVER DATASET PREPARATION
# ------------------------------------------------------------

def prepare_silver_country_lookup(
    spark: SparkSession,
    bronze_df: DataFrame,
) -> DataFrame:
    """Prepare the standardized and enriched Silver country lookup."""

    validate_country_lookup_columns(bronze_df)

    silver_df = standardize_country_lookup(bronze_df)

    silver_df = add_reference_countries(
        spark,
        silver_df,
    )

    validate_country_lookup(silver_df)

    return silver_df


# ------------------------------------------------------------
# SILVER PUBLISHING
# ------------------------------------------------------------

def write_silver_country_lookup(
    spark: SparkSession,
    df: DataFrame,
) -> None:
    """Write the Silver country lookup Delta table."""

    target_table = SILVER_TABLES["country_lookup"]

    source_count = df.count()

    logger.info(
        "Writing %s records to %s",
        source_count,
        target_table,
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
            f"Country lookup row-count mismatch. "
            f"Source: {source_count}, Target: {target_count}"
        )

    logger.info(
        "Silver country lookup completed. Rows written: %s",
        target_count,
    )


# ------------------------------------------------------------
# DATASET TRANSFORMATION ENTRY POINT
# ------------------------------------------------------------

def process_country_lookup(
    spark: SparkSession,
) -> None:
    """Run the complete Bronze-to-Silver country lookup process."""

    logger.info("Starting Silver country lookup transformation.")

    bronze_df = read_bronze_country_lookup(spark)

    silver_df = prepare_silver_country_lookup(
        spark,
        bronze_df,
    )

    write_silver_country_lookup(
        spark,
        silver_df,
    )

    logger.info("Silver country lookup transformation completed.")
