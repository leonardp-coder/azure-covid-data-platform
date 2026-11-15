
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["population_by_age"]
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.dim_age_group"


def read_population_silver(spark: SparkSession) -> DataFrame:
    """Read population data from the Silver layer."""

    logger.info("Reading population data from %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def build_dim_age_group(population_df: DataFrame) -> DataFrame:
    """Build the age group dimension."""

    logger.info("Building age group dimension")

    age_groups_df = (
        population_df
        .groupBy("age_indicator", "age_group")
        .agg(F.count("*").alias("record_count"))
        .orderBy("age_group")
    )

    window_spec = Window.orderBy("age_group")

    dim_age_group_df = (
        age_groups_df
        .withColumn(
            "age_group_key",
            F.row_number().over(window_spec),
        )
        .select(
            "age_group_key",
            "age_indicator",
            "age_group",
        )
    )

    return dim_age_group_df


def validate_dim_age_group(dim_age_group_df: DataFrame) -> None:
    """Validate age group dimension records and keys."""

    total_records = dim_age_group_df.count()

    distinct_keys = (
        dim_age_group_df
        .select("age_group_key")
        .distinct()
        .count()
    )

    null_keys = dim_age_group_df.filter(
        F.col("age_group_key").isNull()
    ).count()

    expected_records = 6

    if null_keys > 0:
        raise ValueError(
            f"Age group dimension has {null_keys} null keys."
        )

    if total_records != distinct_keys:
        raise ValueError(
            "Age group dimension contains duplicate keys."
        )

    if total_records != expected_records:
        raise ValueError(
            f"Unexpected age group count: expected={expected_records}, "
            f"actual={total_records}"
        )

    logger.info(
        "Age group dimension validation passed. Rows: %s",
        total_records,
    )


def publish_dim_age_group(
    spark: SparkSession,
    dim_age_group_df: DataFrame,
) -> None:
    """Publish the age group dimension as a Delta table."""

    source_count = dim_age_group_df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        TARGET_TABLE,
    )

    (
        dim_age_group_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Age group row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Age group dimension published successfully. Rows: %s",
        target_count,
    )


def run_dim_age_group(spark: SparkSession) -> None:
    """Execute the age group dimension pipeline."""

    population_df = read_population_silver(spark)

    dim_age_group_df = build_dim_age_group(population_df)

    validate_dim_age_group(dim_age_group_df)

    publish_dim_age_group(spark, dim_age_group_df)
