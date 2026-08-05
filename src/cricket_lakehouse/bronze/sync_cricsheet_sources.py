from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession

from cricket_lakehouse.common.audit import append_audit_row, ensure_schema, table_name
from cricket_lakehouse.common.hash_utils import sha256_file

DEFAULT_SOURCES = [
    {"source_name": "all_json", "source_type": "match_archive", "source_url": "https://cricsheet.org/downloads/all_json.zip", "source_period": "all"},
    {"source_name": "people", "source_type": "register", "source_url": "https://cricsheet.org/register/people.csv", "source_period": "current"},
    {"source_name": "names", "source_type": "register", "source_url": "https://cricsheet.org/register/names.csv", "source_period": "current"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--raw-volume-path", default="/Volumes/cricket/cricket_all/cricket_all_raw")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--dry-run", default="false")
    parser.add_argument("--sources-json", default=json.dumps(DEFAULT_SOURCES))
    return parser.parse_args()


def source_target(raw_root: str, source: dict[str, str]) -> str:
    if source["source_type"] == "register":
        return os.path.join(raw_root, "zips", "register", source["source_name"] + ".csv")
    subdir = "historical" if source.get("source_period") == "all" else "incremental"
    return os.path.join(raw_root, "zips", subdir, source["source_name"] + ".zip")


def head(url: str) -> dict[str, str]:
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "cricket-ai-lakehouse/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return {key.lower(): value for key, value in response.headers.items()}


def download(url: str, target: str) -> tuple[int, str]:
    temporary = target + ".part"
    request = urllib.request.Request(url, headers={"User-Agent": "cricket-ai-lakehouse/1.0"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=300) as response, open(temporary, "wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
            size = os.path.getsize(temporary)
            if size == 0:
                raise ValueError("downloaded source is empty")
            os.replace(temporary, target)
            return size, sha256_file(target)
        except (OSError, urllib.error.URLError, ValueError):
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    raise RuntimeError("unreachable")


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-sync-cricsheet-sources").getOrCreate()
    ensure_schema(spark, args.catalog, args.schema)
    manifest = table_name(args.catalog, args.schema, "pipeline_source_manifest")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {manifest} (source_id STRING, source_name STRING, source_type STRING, source_url STRING, target_path STRING, source_period STRING, etag STRING, last_modified STRING, content_length BIGINT, sha256 STRING, download_status STRING, download_started_at TIMESTAMP, download_completed_at TIMESTAMP, first_seen_at TIMESTAMP, last_seen_at TIMESTAMP, ingestion_run_id STRING, error_class STRING, error_message STRING, created_at TIMESTAMP, updated_at TIMESTAMP) USING DELTA")
    sources = json.loads(args.sources_json)
    if args.run_mode == "incremental":
        # Incremental runs consume user-landed ZIPs from /zips/. The complete
        # archive is reserved for daily/bootstrap runs to avoid re-downloading
        # tens of thousands of matches on every manual incremental run.
        sources = [source for source in sources if source.get("source_type") != "match_archive"]
    source_rows = []
    failures = 0
    for source in sources:
        target = source_target(args.raw_volume_path, source)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        source_id = source["source_url"]
        started = datetime.now(UTC)
        existing = spark.table(manifest).where(f"source_id = '{source_id.replace(chr(39), chr(39) + chr(39))}'").orderBy("updated_at", ascending=False).limit(1).collect() if spark.catalog.tableExists(manifest) else []
        try:
            headers = head(source["source_url"])
            etag = headers.get("etag")
            last_modified = headers.get("last-modified")
            length = int(headers.get("content-length") or 0)
            unchanged = bool(existing and existing[0].download_status == "downloaded" and (etag and existing[0].etag == etag or last_modified and existing[0].last_modified == last_modified) and os.path.exists(target))
            if args.dry_run.lower() == "true" or unchanged:
                status, size, checksum = ("skipped_unchanged" if unchanged else "dry_run"), length, existing[0].sha256 if existing else None
            else:
                size, checksum = download(source["source_url"], target)
                status = "downloaded"
            now = datetime.now(UTC)
            source_rows.append((source_id, source["source_name"], source["source_type"], source["source_url"], target, source.get("source_period"), etag, last_modified, size, checksum, status, started, now, started, now, args.run_id, None, None, started, now))
        except Exception as error:  # noqa: BLE001 - failure is persisted before task failure.
            failures += 1
            now = datetime.now(UTC)
            source_rows.append((source_id, source["source_name"], source["source_type"], source["source_url"], target, source.get("source_period"), None, None, None, None, "failed", started, now, started, now, args.run_id, type(error).__name__, str(error), started, now))
    if source_rows:
        spark.createDataFrame(source_rows, "source_id string, source_name string, source_type string, source_url string, target_path string, source_period string, etag string, last_modified string, content_length long, sha256 string, download_status string, download_started_at timestamp, download_completed_at timestamp, first_seen_at timestamp, last_seen_at timestamp, ingestion_run_id string, error_class string, error_message string, created_at timestamp, updated_at timestamp").write.format("delta").mode("append").saveAsTable(manifest)
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "sync_cricsheet_sources", "FAILED" if failures else "SUCCEEDED", len(sources), len(source_rows) - failures, len([r for r in source_rows if r[10] == "downloaded"]), skipped_count=len([r for r in source_rows if r[10] in {"skipped_unchanged", "dry_run"}]), quarantine_count=0, run_mode=args.run_mode, started_at=task_started_at)
    if failures:
        raise RuntimeError(f"{failures} source download(s) failed")


if __name__ == "__main__":
    main()
