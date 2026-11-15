
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import GOLD_SCHEMA_NAME
from src.common.logging_utils import get_logger


logger = get_logger(__name__)

TARGET_TABLE = f"{GOLD_SCHEMA_NAME}.dim_date"

START_DATE = "2020-01-02"
END_DATE = "2020-10-25"


def build_dim_date(spark: SparkSession) -> DataFrame:
    """
    Generate the date dimension for the project.

    Preserves the original calendar boundaries and date attributes.
    """

    logger.info(
        "Building date dimension from %s to %s",
        START_DATE,
        END_DATE,
    )

    dim_date_df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(START_DATE)),
                    F.to_date(F.lit(END_DATE)),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("full_date")
        )
        .withColumn(
            "date_key",
            F.date_format("full_date", "yyyyMMdd").cast("int"),
        )
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
    )

    return dim_date_df


def validate_dim_date(dim_date_df: DataFrame) -> None:
    """Validate date dimension keys and calendar coverage."""

    total_records = dim_date_df.count()

    distinct_keys = (
        dim_date_df
        .select("date_key")
        .distinct()
        .count()
    )

    null_keys = dim_date_df.filter(
        F.col("date_key").isNull()
    ).count()

    expected_records = 298

    if null_keys > 0:
        raise ValueError(
            f"Date dimension has {null_keys} null date keys."
        )

    if total_records != distinct_keys:
        raise ValueError(
            "Date dimension contains duplicate date keys."
        )

    if total_records != expected_records:
        raise ValueError(
            f"Unexpected date count: expected={expected_records}, "
            f"actual={total_records}"
        )

    logger.info(
        "Date dimension validation passed. Rows: %s",
        total_records,
    )


def publish_dim_date(
    spark: SparkSession,
    dim_date_df: DataFrame,
) -> None:
    """Publish the date dimension as a Delta table."""

    source_count = dim_date_df.count()

    logger.info(
        "Publishing %s records to %s",
        source_count,
        TARGET_TABLE,
    )

    (
        dim_date_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    target_count = spark.table(TARGET_TABLE).count()

    if source_count != target_count:
        raise RuntimeError(
            f"Date dimension row count mismatch: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(
        "Date dimension published successfully. Rows: %s",
        target_count,
    )


def run_dim_date(spark: SparkSession) -> None:
    """Execute the date dimension transformation."""

    dim_date_df = build_dim_date(spark)

    validate_dim_date(dim_date_df)

    publish_dim_date(spark, dim_date_df)
