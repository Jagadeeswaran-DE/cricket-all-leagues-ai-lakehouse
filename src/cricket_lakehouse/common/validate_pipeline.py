from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from cricket_lakehouse.common.audit import append_audit_row, table_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--serving-schema", default="cricinsights_src2")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--layer", default="silver")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"cricket-validate-{args.layer}").getOrCreate()
    checks: list[tuple[str, str, str, int, dict[str, object]]] = []
    deliveries = table_name(args.catalog, args.schema, "silver_deliveries")
    if spark.catalog.tableExists(deliveries):
        delivery_frame = spark.table(deliveries)
        required_key_columns = {"match_id", "innings_number", "over_number", "delivery_sequence"}
        if required_key_columns.issubset(delivery_frame.columns):
            keyed = delivery_frame.where(col("innings_number").isNotNull() & col("delivery_sequence").isNotNull())
            duplicates = keyed.groupBy("match_id", "innings_number", "over_number", "delivery_sequence").count().where(col("count") > 1).count()
        else:
            duplicates = 0
        checks.append(("silver_delivery_key_unique", deliveries, "CRITICAL", duplicates, {"key": "match_id,innings_number,over_number,delivery_sequence"}))
        negative = delivery_frame.where((col("total_runs") < 0) | (col("batter_runs") < 0)).count()
        checks.append(("silver_delivery_runs_nonnegative", deliveries, "CRITICAL", negative, {}))
    matches = table_name(args.catalog, args.schema, "silver_matches")
    if spark.catalog.tableExists(matches):
        checks.append(("silver_match_id_unique", matches, "CRITICAL", spark.table(matches).groupBy("match_id").count().where(col("count") > 1).count(), {}))
    results = []
    for check_name, table, severity, failures, sample in checks:
        results.append((args.run_id, check_name, table, severity, "PASS" if failures == 0 else "FAIL", failures, json.dumps(sample), datetime.now(UTC)))
    target = table_name(args.catalog, args.schema, "pipeline_data_quality_results")
    if results:
        spark.createDataFrame(results, "run_id string, check_name string, table_name string, severity string, status string, failed_record_count long, sample_failure_json string, checked_at timestamp").write.format("delta").mode("append").saveAsTable(target)
    critical_failures = sum(1 for row in results if row[3] == "CRITICAL" and row[4] == "FAIL")
    append_audit_row(spark, args.catalog, args.schema, args.run_id, f"validate_{args.layer}", "FAILED" if critical_failures else "SUCCEEDED", len(checks), len(results), run_mode=args.run_mode)
    if critical_failures:
        raise RuntimeError(f"{critical_failures} critical {args.layer} data quality check(s) failed")


if __name__ == "__main__":
    main()
