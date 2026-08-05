from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession

from cricket_lakehouse.common.audit import append_audit_row, table_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--status", default="SUCCEEDED")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-finalize-run").getOrCreate()
    context = table_name(args.catalog, args.schema, "pipeline_run_context")
    if spark.catalog.tableExists(context):
        spark.sql(f"""UPDATE {context} SET completed_at = current_timestamp(), status = '{args.status}' WHERE run_id = '{args.run_id.replace("'", "''")}'""")
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "finalize_run_summary", args.status, run_mode=args.run_mode, started_at=task_started_at)


if __name__ == "__main__":
    main()
