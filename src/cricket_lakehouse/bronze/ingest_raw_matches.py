from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))
from pyspark.sql.functions import (
    coalesce,
    col,
    current_date,
    current_timestamp,
    get_json_object,
    lit,
    regexp_extract,
    regexp_replace,
    sha2,
    split,
    when,
)

from cricket_lakehouse.common.audit import append_audit_row, table_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--table-prefix", default="")
    parser.add_argument("--source-path", default="/Volumes/cricket/cricket_all/cricket_all_raw/extracted/*.json")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--incremental", default="true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-bronze-ingest-raw-matches").getOrCreate()
    target = table_name(args.catalog, args.schema, f"{args.table_prefix}bronze_raw_matches")
    manifest = table_name(args.catalog, args.schema, "pipeline_extracted_file_manifest")
    incremental = args.incremental.lower() == "true" and args.run_mode != "bootstrap"
    if not spark.catalog.tableExists(manifest):
        append_audit_row(spark, args.catalog, args.schema, args.run_id, "bronze_ingest_raw_matches", "SUCCEEDED", run_mode=args.run_mode)
        return
    candidates = spark.table(manifest).where("(file_extension = '.json' OR source_filename LIKE '%.json') AND extraction_status = 'extracted'")
    if incremental:
        candidates = candidates.where(col("ingestion_run_id") == args.run_id)
    source_paths = [
        row.extracted_path
        for row in candidates.select("extracted_path").distinct().collect()
        if row.extracted_path
    ]
    if not source_paths:
        append_audit_row(spark, args.catalog, args.schema, args.run_id, "bronze_ingest_raw_matches", "SUCCEEDED", run_mode=args.run_mode)
        return
    raw = spark.read.format("text").option("wholetext", "true").load(args.source_path if not incremental else source_paths)
    raw = raw.withColumnRenamed("value", "raw_json").withColumn("source_path", col("_metadata.file_path"))
    metadata = candidates.select("extracted_path", "zip_path", "zip_sha256", "file_sha256", "source_revision", "match_id_candidate").dropDuplicates(["extracted_path"])
    raw = raw.join(metadata, regexp_replace(raw.source_path, "^file:", "") == metadata.extracted_path, "left").drop("extracted_path")
    raw = raw.withColumn("source_file", regexp_extract(col("source_path"), r"([^/]+)$", 1))
    raw = raw.withColumn("match_id", coalesce(col("match_id_candidate"), when(col("source_file").rlike(r"[0-9]+\.json$"), split(col("source_file"), "\\.").getItem(0)), lit("fallback_").concat(sha2(col("raw_json"), 256).substr(1, 32))))
    raw = raw.withColumn("source_file_sha256", coalesce(col("file_sha256"), sha2(col("raw_json"), 256))).withColumn("source_revision", coalesce(col("source_revision"), get_json_object("raw_json", "$.meta.revision"), lit("0"))).withColumn("data_version", get_json_object("raw_json", "$.meta.data_version")).withColumn("source_created_date", get_json_object("raw_json", "$.meta.created").cast("date")).withColumn("record_status", lit("valid")).withColumn("parse_error", lit(None).cast("string")).withColumn("ingestion_run_id", lit(args.run_id)).withColumn("pipeline_run_id", lit(args.run_id)).withColumn("ingestion_timestamp", current_timestamp()).withColumn("ingestion_date", current_date())
    projected = raw.select("match_id", "raw_json", "source_file", "source_path", "zip_path", "zip_sha256", "source_file_sha256", "source_revision", "data_version", "source_created_date", "ingestion_run_id", "pipeline_run_id", "ingestion_timestamp", "ingestion_date", "record_status", "parse_error")
    candidate_count = projected.select("match_id").distinct().count()
    if not spark.catalog.tableExists(target):
        projected.write.format("delta").option("mergeSchema", "true").mode("overwrite").partitionBy("ingestion_date").saveAsTable(target)
        projected.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name(args.catalog, args.schema, "bronze_raw_match_versions"))
        inserted_count = candidate_count
        updated_count = 0
    else:
        projected.createOrReplaceTempView("cricket_bronze_batch")
        versions = table_name(args.catalog, args.schema, "bronze_raw_match_versions")
        projected.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(versions)
        before = spark.table(target).select("match_id").distinct().count()
        spark.sql(f"""MERGE INTO {target} AS target USING cricket_bronze_batch AS source ON target.match_id = source.match_id
          WHEN MATCHED AND target.source_file_sha256 <> source.source_file_sha256 THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""")
        after = spark.table(target).select("match_id").distinct().count()
        inserted_count = max(0, after - before)
        updated_count = max(0, candidate_count - inserted_count)
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "bronze_ingest_raw_matches", "SUCCEEDED", candidate_count, candidate_count, inserted_count, updated_count, run_mode=args.run_mode)


if __name__ == "__main__":
    main()
