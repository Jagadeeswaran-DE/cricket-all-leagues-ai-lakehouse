from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession


def table_name(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - catalog exceptions vary by runtime.
        return False


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def append_audit_row(
    spark: SparkSession,
    catalog: str,
    schema: str,
    run_id: str,
    task_name: str,
    status: str,
    input_count: int = 0,
    output_count: int = 0,
    inserted_count: int = 0,
    updated_count: int = 0,
    skipped_count: int = 0,
    quarantine_count: int = 0,
    error_message: str | None = None,
    run_mode: str = "incremental",
) -> None:
    ensure_schema(spark, catalog, schema)
    target = table_name(catalog, schema, "pipeline_task_audit")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {target} (
          run_id STRING, task_name STRING, run_mode STRING, status STRING,
          input_count BIGINT, output_count BIGINT, inserted_count BIGINT,
          updated_count BIGINT, deleted_count BIGINT, skipped_count BIGINT,
          quarantine_count BIGINT, error_message STRING, recorded_at TIMESTAMP
        ) USING DELTA"""
    )
    row = [
        (
            run_id,
            task_name,
            run_mode,
            status,
            int(input_count),
            int(output_count),
            int(inserted_count),
            int(updated_count),
            0,
            int(skipped_count),
            int(quarantine_count),
            error_message,
            datetime.now(UTC),
        )
    ]
    spark.createDataFrame(
        row,
        "run_id string, task_name string, run_mode string, status string, input_count long, "
        "output_count long, inserted_count long, updated_count long, deleted_count long, "
        "skipped_count long, quarantine_count long, error_message string, recorded_at timestamp",
    ).write.format("delta").mode("append").saveAsTable(target)
