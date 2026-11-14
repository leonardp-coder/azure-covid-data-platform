
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import GOLD_SCHEMA_NAME, SILVER_TABLES
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

SOURCE_TABLE = SILVER_TABLES["population_by_age"]
TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.dim_age_group"


def read_population_silver() -> DataFrame:
    """Read population data from the Silver layer."""

    logger.info("Reading population Silver table: %s", SOURCE_TABLE)

    return spark.table(SOURCE_TABLE)


def build_dim_age_group(
    population_silver_df: DataFrame,
) -> DataFrame:
    """
    Build the age group dimension.

    Preserve the original grouping and row_number key-generation logic.
    """

    required_columns = ["age_indicator", "age_group"]

    missing_columns = [
        column
        for column in required_columns
        if column not in population_silver_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required population columns: {missing_columns}"
        )

    age_group_df = (
        population_silver_df
        .groupBy("age_indicator", "age_group")
        .count()
        .orderBy("age_group")
    )

    age_window = Window.orderBy("age_group")

    dim_age_group_df = (
        age_group_df
        .withColumn(
            "age_group_key",
            F.row_number().over(age_window),
        )
        .select(
            "age_group_key",
            "age_indicator",
            "age_group",
        )
    )

    return dim_age_group_df


def validate_dim_age_group(
    dim_age_group_df: DataFrame,
) -> None:
    """Validate age group dimension records and keys."""

    total_records = dim_age_group_df.count()

    distinct_keys = (
        dim_age_group_df
        .select("age_group_key")
        .distinct()
        .count()
    )

    distinct_groups = (
        dim_age_group_df
        .select("age_group")
        .distinct()
        .count()
    )

    null_keys = dim_age_group_df.filter(
        F.col("age_group_key").isNull()
    ).count()

    if null_keys > 0:
        raise ValueError("Age group dimension contains null keys.")

    if total_records != distinct_keys:
        raise ValueError("Duplicate age group keys detected.")

    if total_records != distinct_groups:
        raise ValueError(
            "An age group has multiple dimension records."
        )

    logger.info(
        "Age group dimension validation passed. Records: %s",
        total_records,
    )


def publish_dim_age_group(
    dim_age_group_df: DataFrame,
) -> None:
    """Publish the age group dimension as a Delta table."""

    source_count = dim_age_group_df.count()

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


def run_dim_age_group() -> None:
    """Execute the age group dimension transformation."""

    population_silver_df = read_population_silver()

    dim_age_group_df = build_dim_age_group(population_silver_df)

    validate_dim_age_group(dim_age_group_df)

    publish_dim_age_group(dim_age_group_df)
