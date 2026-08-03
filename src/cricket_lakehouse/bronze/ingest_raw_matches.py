from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, lit, regexp_extract
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

DEFAULT_SOURCE_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/extracted/*.json"

METRIC_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", LongType(), False),
        StructField("recorded_at", TimestampType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--table-prefix", default="")
    parser.add_argument("--source-path", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--incremental", default="true")
    return parser.parse_args()


def table_name(args: argparse.Namespace, name: str) -> str:
    return f"{args.catalog}.{args.schema}.{args.table_prefix}{name}"


def unprefixed_table_name(args: argparse.Namespace, name: str) -> str:
    return f"{args.catalog}.{args.schema}.{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - Spark raises different catalog exceptions by runtime.
        return False


def incremental_source_paths(spark: SparkSession, args: argparse.Namespace) -> list[str]:
    manifest_table = unprefixed_table_name(args, "pipeline_extracted_file_manifest")
    if not table_exists(spark, manifest_table):
        return []
    return [
        row.extracted_path
        for row in spark.table(manifest_table)
        .where((col("run_id") == args.run_id) & (col("status") == "extracted"))
        .select("extracted_path")
        .distinct()
        .collect()
    ]


def write_metric(spark: SparkSession, args: argparse.Namespace, metric_name: str, metric_value: int) -> None:
    metrics_table = unprefixed_table_name(args, "pipeline_task_metrics")
    row = [(args.run_id, "bronze_ingest_raw_matches", metric_name, int(metric_value))]
    (
        spark.createDataFrame(row, ["run_id", "stage", "metric_name", "metric_value"])
        .withColumn("recorded_at", current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(metrics_table)
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-bronze-ingest-raw-matches").getOrCreate()
    target_table = table_name(args, "bronze_raw_matches")
    incremental = args.incremental.lower() == "true"
    source_paths = incremental_source_paths(spark, args) if incremental else []

    if incremental and not source_paths:
        write_metric(spark, args, "candidate_files", 0)
        write_metric(spark, args, "inserted_matches", 0)
        return

    raw_matches = (
        spark.read.format("text")
        .option("wholetext", "true")
        .load(source_paths or args.source_path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("match_id", regexp_extract("source_file", r"([^/]+)\.json$", 1))
        .withColumn("pipeline_run_id", lit(args.run_id))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
    )

    candidate_count = raw_matches.select("match_id").distinct().count()

    if incremental and table_exists(spark, target_table):
        existing_matches = spark.table(target_table).select("match_id").distinct()
        raw_matches = raw_matches.join(existing_matches, ["match_id"], "left_anti")

    inserted_count = raw_matches.select("match_id").distinct().count()
    write_mode = "append" if incremental else "overwrite"
    writer = raw_matches.write.format("delta").mode(write_mode)
    if incremental:
        writer = writer.option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")
    if not table_exists(spark, target_table):
        writer = writer.partitionBy("ingestion_date")
    writer.saveAsTable(target_table)

    write_metric(spark, args, "candidate_files", candidate_count)
    write_metric(spark, args, "inserted_matches", inserted_count)


if __name__ == "__main__":
    main()
