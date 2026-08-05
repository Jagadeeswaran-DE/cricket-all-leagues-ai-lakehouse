from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession

from cricket_lakehouse.common.audit import append_audit_row, ensure_schema, table_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--serving-schema", default="cricinsights_src2")
    parser.add_argument("--raw-volume-path", default="/Volumes/cricket/cricket_all/cricket_all_raw")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--dry-run", default="false")
    return parser.parse_args()


def ensure_operational_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    ensure_schema(spark, catalog, schema)
    tables = {
        "pipeline_run_context": "run_id STRING, run_mode STRING, dry_run BOOLEAN, started_at TIMESTAMP, completed_at TIMESTAMP, status STRING",
        "pipeline_source_manifest": "source_id STRING, source_name STRING, source_type STRING, source_url STRING, target_path STRING, source_period STRING, etag STRING, last_modified STRING, content_length BIGINT, sha256 STRING, download_status STRING, download_started_at TIMESTAMP, download_completed_at TIMESTAMP, first_seen_at TIMESTAMP, last_seen_at TIMESTAMP, ingestion_run_id STRING, error_class STRING, error_message STRING, created_at TIMESTAMP, updated_at TIMESTAMP",
        "pipeline_zip_manifest": "run_id STRING, zip_path STRING, zip_name STRING, zip_size_bytes BIGINT, zip_modified_at TIMESTAMP, zip_sha256 STRING, source_id STRING, ingestion_run_id STRING, status STRING, extraction_started_at TIMESTAMP, extraction_completed_at TIMESTAMP, json_file_count BIGINT, register_file_count BIGINT, invalid_file_count BIGINT, error_class STRING, error_message STRING, created_at TIMESTAMP, updated_at TIMESTAMP, extracted_json_files BIGINT, processed_at TIMESTAMP",
        "pipeline_extracted_file_manifest": "zip_sha256 STRING, zip_path STRING, relative_member_path STRING, extracted_path STRING, source_filename STRING, file_extension STRING, file_size_bytes BIGINT, file_crc BIGINT, file_sha256 STRING, match_id_candidate STRING, source_revision STRING, extraction_status STRING, bronze_ingestion_status STRING, ingestion_run_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP, run_id STRING, match_id STRING, status STRING, processed_at TIMESTAMP, error_class STRING, error_message STRING",
        "pipeline_task_audit": "run_id STRING, task_name STRING, run_mode STRING, status STRING, input_count BIGINT, output_count BIGINT, inserted_count BIGINT, updated_count BIGINT, deleted_count BIGINT, skipped_count BIGINT, quarantine_count BIGINT, error_message STRING, recorded_at TIMESTAMP, started_at TIMESTAMP, completed_at TIMESTAMP, duration_seconds DOUBLE",
        "pipeline_task_progress": "run_id STRING, task_name STRING, current_item STRING, processed_count BIGINT, total_count BIGINT, percent_complete DOUBLE, status STRING, message STRING, updated_at TIMESTAMP",
        "pipeline_data_quality_results": "run_id STRING, check_name STRING, table_name STRING, severity STRING, status STRING, failed_record_count BIGINT, sample_failure_json STRING, checked_at TIMESTAMP",
        "pipeline_unresolved_people": "run_id STRING, match_id STRING, person_name STRING, source_role STRING, source_file STRING, recorded_at TIMESTAMP",
        "pipeline_unresolved_competitions": "run_id STRING, event_name STRING, match_id STRING, source_file STRING, recorded_at TIMESTAMP",
        "config_focus_leagues": "league_name STRING, enabled BOOLEAN, priority INT, valid_from DATE, valid_to DATE",
    }
    for name, columns in tables.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name(catalog, schema, name)} ({columns}) USING DELTA")


def ensure_columns(spark: SparkSession, target: str, columns: dict[str, str]) -> None:
    if not spark.catalog.tableExists(target):
        return
    existing = {field.name.lower() for field in spark.table(target).schema.fields}
    for name, dtype in columns.items():
        if name.lower() not in existing:
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({name} {dtype})")


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-initialize-run").getOrCreate()
    ensure_operational_tables(spark, args.catalog, args.schema)
    ensure_schema(spark, args.catalog, args.serving_schema)
    ensure_columns(
        spark,
        table_name(args.catalog, args.schema, "pipeline_zip_manifest"),
        {
            "zip_sha256": "STRING", "source_id": "STRING", "ingestion_run_id": "STRING",
            "extraction_started_at": "TIMESTAMP", "extraction_completed_at": "TIMESTAMP",
            "json_file_count": "BIGINT", "register_file_count": "BIGINT", "invalid_file_count": "BIGINT",
            "error_class": "STRING", "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP",
        },
    )
    ensure_columns(
        spark,
        table_name(args.catalog, args.schema, "pipeline_extracted_file_manifest"),
        {
            "zip_sha256": "STRING", "relative_member_path": "STRING", "source_filename": "STRING",
            "file_extension": "STRING", "file_size_bytes": "BIGINT", "file_crc": "BIGINT",
            "file_sha256": "STRING", "match_id_candidate": "STRING", "source_revision": "STRING",
            "extraction_status": "STRING", "bronze_ingestion_status": "STRING", "ingestion_run_id": "STRING",
            "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP", "error_class": "STRING", "error_message": "STRING",
        },
    )
    ensure_columns(
        spark,
        table_name(args.catalog, args.schema, "pipeline_task_audit"),
        {
            "started_at": "TIMESTAMP",
            "completed_at": "TIMESTAMP",
            "duration_seconds": "DOUBLE",
        },
    )
    ensure_columns(
        spark,
        table_name(args.catalog, args.schema, "bronze_raw_matches"),
        {
            "source_zip": "STRING",
            "source_zip_sha256": "STRING",
            "source_file_sha256": "STRING",
            "source_revision": "STRING",
            "data_version": "STRING",
            "source_created_date": "DATE",
            "record_status": "STRING",
            "parse_error": "STRING",
            "match_id_rule": "STRING",
            "source_file": "STRING",
            "source_path": "STRING",
            "ingestion_run_id": "STRING",
        },
    )
    ensure_columns(
        spark,
        table_name(args.catalog, args.schema, "silver_deliveries"),
        {"innings_number": "INT", "delivery_sequence": "INT"},
    )
    for relative in ("zips", "zips/historical", "zips/incremental", "zips/register", "zips/quarantine", "extracted", "extracted/matches", "extracted/register", "extracted/quarantine", "metadata/coverage", "metadata/missing", "metadata/withheld", "metadata/downloads", "checkpoints", "logs"):
        os.makedirs(os.path.join(args.raw_volume_path, relative), exist_ok=True)
    context = [(args.run_id, args.run_mode, args.dry_run.lower() == "true", "RUNNING",)]
    spark.createDataFrame(context, "run_id string, run_mode string, dry_run boolean, status string").write.format("delta").mode("append").saveAsTable(table_name(args.catalog, args.schema, "pipeline_run_context"))
    append_audit_row(
        spark,
        args.catalog,
        args.schema,
        args.run_id,
        "initialize_run",
        "SUCCEEDED",
        run_mode=args.run_mode,
        started_at=task_started_at,
    )


if __name__ == "__main__":
    main()
