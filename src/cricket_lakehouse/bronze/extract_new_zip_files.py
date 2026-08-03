from __future__ import annotations

import argparse
import glob
import os
import posixpath
import zipfile
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

DEFAULT_ZIP_SOURCE_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/zips/*.zip"
DEFAULT_EXTRACTED_PATH = "/Volumes/cricket/cricket_all/cricket_all_raw/extracted"


ZIP_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("zip_path", StringType(), False),
        StructField("zip_name", StringType(), False),
        StructField("zip_size_bytes", LongType(), False),
        StructField("zip_modified_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("extracted_json_files", LongType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("error_message", StringType(), True),
    ]
)

FILE_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("zip_path", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("extracted_path", StringType(), False),
        StructField("status", StringType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--zip-source-path", default=DEFAULT_ZIP_SOURCE_PATH)
    parser.add_argument("--extract-output-path", default=DEFAULT_EXTRACTED_PATH)
    parser.add_argument("--run-id", default="manual")
    return parser.parse_args()


def table_name(args: argparse.Namespace, name: str) -> str:
    return f"{args.catalog}.{args.schema}.{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - Spark raises different catalog exceptions by runtime.
        return False


def processed_zip_keys(spark: SparkSession, manifest_table: str) -> set[tuple[str, int, int]]:
    if not table_exists(spark, manifest_table):
        return set()

    rows = (
        spark.table(manifest_table)
        .where("status = 'extracted'")
        .select("zip_path", "zip_size_bytes", "zip_modified_at")
        .collect()
    )
    keys: set[tuple[str, int, int]] = set()
    for row in rows:
        modified_epoch = int(row.zip_modified_at.timestamp()) if row.zip_modified_at else 0
        keys.add((row.zip_path, int(row.zip_size_bytes), modified_epoch))
    return keys


def safe_extract_name(member_name: str) -> str | None:
    normalized = posixpath.normpath(member_name.replace("\\", "/"))
    if normalized.startswith(("../", "/")) or normalized == ".":
        return None
    if not normalized.lower().endswith(".json"):
        return None
    return posixpath.basename(normalized)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-extract-new-zip-files").getOrCreate()
    zip_manifest_table = table_name(args, "pipeline_zip_manifest")
    file_manifest_table = table_name(args, "pipeline_extracted_file_manifest")

    os.makedirs(args.extract_output_path, exist_ok=True)
    already_processed = processed_zip_keys(spark, zip_manifest_table)
    zip_manifest_rows = []
    file_manifest_rows = []

    for zip_path in sorted(glob.glob(args.zip_source_path)):
        processed_at = datetime.now(UTC)
        zip_stat = os.stat(zip_path)
        zip_modified_at = datetime.fromtimestamp(zip_stat.st_mtime, UTC)
        zip_key = (zip_path, int(zip_stat.st_size), int(zip_stat.st_mtime))

        if zip_key in already_processed:
            zip_manifest_rows.append(
                (
                    args.run_id,
                    zip_path,
                    os.path.basename(zip_path),
                    int(zip_stat.st_size),
                    zip_modified_at,
                    "skipped_already_processed",
                    0,
                    processed_at,
                    None,
                )
            )
            continue

        extracted_count = 0
        try:
            with zipfile.ZipFile(zip_path) as archive:
                for member in archive.infolist():
                    file_name = safe_extract_name(member.filename)
                    if file_name is None:
                        continue

                    output_path = os.path.join(args.extract_output_path, file_name)
                    match_id = os.path.splitext(file_name)[0]
                    if os.path.exists(output_path):
                        file_manifest_rows.append(
                            (args.run_id, zip_path, match_id, output_path, "skipped_file_exists", processed_at)
                        )
                        continue

                    with archive.open(member) as source, open(output_path, "wb") as target:
                        target.write(source.read())
                    extracted_count += 1
                    file_manifest_rows.append(
                        (args.run_id, zip_path, match_id, output_path, "extracted", processed_at)
                    )

            zip_manifest_rows.append(
                (
                    args.run_id,
                    zip_path,
                    os.path.basename(zip_path),
                    int(zip_stat.st_size),
                    zip_modified_at,
                    "extracted",
                    extracted_count,
                    processed_at,
                    None,
                )
            )
        except (OSError, RuntimeError, zipfile.BadZipFile) as error:
            zip_manifest_rows.append(
                (
                    args.run_id,
                    zip_path,
                    os.path.basename(zip_path),
                    int(zip_stat.st_size),
                    zip_modified_at,
                    "failed",
                    extracted_count,
                    processed_at,
                    str(error),
                )
            )

    spark.createDataFrame(zip_manifest_rows, ZIP_MANIFEST_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(zip_manifest_table)
    spark.createDataFrame(file_manifest_rows, FILE_MANIFEST_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(file_manifest_table)

    failed_count = sum(1 for row in zip_manifest_rows if row[5] == "failed")
    if failed_count:
        raise RuntimeError(f"{failed_count} zip file(s) failed extraction")


if __name__ == "__main__":
    main()
