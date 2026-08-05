from __future__ import annotations

import argparse
import glob
import os
import posixpath
import shutil
import sys
import tempfile
import zipfile
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession

from cricket_lakehouse.common.audit import append_audit_row, table_name
from cricket_lakehouse.common.hash_utils import sha256_file

DEFAULT_ZIP_SOURCE_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/zips/**/*.zip"
DEFAULT_EXTRACTED_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/extracted"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--zip-source-path", default=DEFAULT_ZIP_SOURCE_PATH)
    parser.add_argument("--extract-output-path", default=DEFAULT_EXTRACTED_PATH)
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--dry-run", default="false")
    return parser.parse_args()


def safe_member_path(member_name: str) -> str | None:
    normalized = posixpath.normpath(member_name.replace("\\", "/"))
    if normalized in {"", ".", ".."} or normalized.startswith(("/", "../")):
        return None
    if not normalized.lower().endswith((".json", ".csv")):
        return None
    return normalized


def existing_zip_hashes(spark: SparkSession, manifest: str) -> set[str]:
    if not spark.catalog.tableExists(manifest):
        return set()
    return {row.zip_sha256 for row in spark.table(manifest).where("status = 'extracted'").select("zip_sha256").distinct().collect() if row.zip_sha256}


def row_schema():
    return "run_id string, zip_path string, zip_name string, zip_size_bytes long, zip_modified_at timestamp, zip_sha256 string, source_id string, ingestion_run_id string, status string, extraction_started_at timestamp, extraction_completed_at timestamp, json_file_count long, register_file_count long, invalid_file_count long, error_class string, error_message string, created_at timestamp, updated_at timestamp, extracted_json_files long, processed_at timestamp"


def file_schema():
    return "zip_sha256 string, zip_path string, relative_member_path string, extracted_path string, source_filename string, file_extension string, file_size_bytes long, file_crc long, file_sha256 string, match_id_candidate string, source_revision string, extraction_status string, bronze_ingestion_status string, ingestion_run_id string, created_at timestamp, updated_at timestamp, run_id string, match_id string, status string, processed_at timestamp, error_class string, error_message string"


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-extract-new-zip-files").getOrCreate()
    zip_manifest = table_name(args.catalog, args.schema, "pipeline_zip_manifest")
    file_manifest = table_name(args.catalog, args.schema, "pipeline_extracted_file_manifest")
    os.makedirs(args.extract_output_path, exist_ok=True)
    processed_hashes = existing_zip_hashes(spark, zip_manifest)
    zip_rows, file_rows = [], []
    failed = 0

    for zip_path in sorted(glob.glob(args.zip_source_path, recursive=True)):
        if not os.path.isfile(zip_path):
            continue
        stat = os.stat(zip_path)
        modified = datetime.fromtimestamp(stat.st_mtime, UTC)
        started = datetime.now(UTC)
        checksum = sha256_file(zip_path)
        if checksum in processed_hashes:
            print(f"Skipping already processed ZIP: {zip_path}", flush=True)
            zip_rows.append((args.run_id, zip_path, os.path.basename(zip_path), stat.st_size, modified, checksum, None, args.run_id, "skipped_already_processed", started, started, 0, 0, 0, None, None, started, started, 0, started))
            continue

        print(f"Extracting ZIP: {zip_path} ({stat.st_size:,} bytes)", flush=True)
        json_count = register_count = invalid_count = 0
        temp_dir = tempfile.mkdtemp(prefix="cricket_extract_", dir=args.extract_output_path)
        try:
            if args.dry_run.lower() != "true":
                with zipfile.ZipFile(zip_path) as archive:
                    for member in archive.infolist():
                        relative = safe_member_path(member.filename)
                        if relative is None:
                            if member.filename.lower().endswith((".json", ".csv")):
                                invalid_count += 1
                            continue
                        output_name = os.path.basename(relative)
                        candidate = os.path.join(temp_dir, output_name)
                        with archive.open(member) as source, open(candidate, "wb") as target:
                            shutil.copyfileobj(source, target, length=1024 * 1024)
                        file_hash = sha256_file(candidate)
                        final_name = output_name
                        final_path = os.path.join(args.extract_output_path, final_name)
                        if os.path.exists(final_path) and sha256_file(final_path) != file_hash:
                            final_name = f"{os.path.splitext(output_name)[0]}__{file_hash[:12]}{os.path.splitext(output_name)[1]}"
                            final_path = os.path.join(args.extract_output_path, final_name)
                        os.replace(candidate, final_path)
                        extension = os.path.splitext(final_name)[1].lower()
                        match_candidate = os.path.splitext(output_name)[0] if extension == ".json" else None
                        file_rows.append((checksum, zip_path, relative, final_path, os.path.basename(final_name), extension, member.file_size, member.CRC, file_hash, match_candidate, file_hash, "extracted", "pending", args.run_id, started, datetime.now(UTC), args.run_id, match_candidate, "extracted", datetime.now(UTC), None, None))
                        json_count += extension == ".json"
                        register_count += extension == ".csv"
                        if (json_count + register_count) % 1000 == 0:
                            print(f"  extracted {json_count:,} JSON and {register_count:,} register files", flush=True)
            completed = datetime.now(UTC)
            zip_rows.append((args.run_id, zip_path, os.path.basename(zip_path), stat.st_size, modified, checksum, None, args.run_id, "dry_run" if args.dry_run.lower() == "true" else "extracted", started, completed, json_count, register_count, invalid_count, None, None, started, completed, json_count, completed))
            processed_hashes.add(checksum)
            print(f"Completed ZIP: {zip_path} ({json_count:,} JSON, {register_count:,} register files)", flush=True)
        except (OSError, RuntimeError, zipfile.BadZipFile, ValueError) as error:
            failed += 1
            completed = datetime.now(UTC)
            zip_rows.append((args.run_id, zip_path, os.path.basename(zip_path), stat.st_size, modified, checksum, None, args.run_id, "failed", started, completed, json_count, register_count, invalid_count, type(error).__name__, str(error), started, completed, json_count, completed))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    if zip_rows:
        spark.createDataFrame(zip_rows, row_schema()).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(zip_manifest)
    if file_rows:
        spark.createDataFrame(file_rows, file_schema()).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(file_manifest)
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "extract_new_zip_files", "FAILED" if failed else "SUCCEEDED", len(zip_rows), sum(row[11] for row in zip_rows), sum(row[11] for row in zip_rows), skipped_count=sum(row[11] == 0 for row in zip_rows), quarantine_count=sum(row[13] for row in zip_rows), run_mode=args.run_mode)
    if failed:
        raise RuntimeError(f"{failed} ZIP file(s) failed extraction")


if __name__ == "__main__":
    main()
