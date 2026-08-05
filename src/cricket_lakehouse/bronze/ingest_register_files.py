from __future__ import annotations

import argparse
import os
import sys

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, explode, lit, map_from_arrays, struct, to_json

from cricket_lakehouse.common.audit import append_audit_row, table_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-ingest-register-files").getOrCreate()
    manifest = table_name(args.catalog, args.schema, "pipeline_source_manifest")
    paths = []
    if spark.catalog.tableExists(manifest):
        paths = [row.target_path for row in spark.table(manifest).where("source_type = 'register' AND download_status IN ('downloaded', 'skipped_unchanged')").select("target_path").distinct().collect() if row.target_path and os.path.exists(row.target_path)]
    if not paths:
        append_audit_row(spark, args.catalog, args.schema, args.run_id, "ingest_register_files", "SUCCEEDED", run_mode=args.run_mode)
        return
    people_path = [path for path in paths if path.endswith("people.csv")]
    names_path = [path for path in paths if path.endswith("names.csv")]
    people = None
    if people_path:
        people = spark.read.option("header", "true").option("escape", '"').csv(people_path).withColumn("source_file", lit("people.csv"))
        people.select("identifier", "name", "unique_name", "source_file", to_json(struct(*[col(c) for c in people.columns])).alias("raw_json")).write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name(args.catalog, args.schema, "dim_people"))
        external_columns = [c for c in people.columns if c.startswith("key_")]
        if external_columns:
            pairs = []
            for column_name in external_columns:
                pairs.extend([lit(column_name[4:]), col(column_name).cast("string")])
            people.select(col("identifier").alias("person_id"), explode(map_from_arrays(array(*[pairs[i] for i in range(0, len(pairs), 2)]), array(*[pairs[i] for i in range(1, len(pairs), 2)]))).alias("external_source", "external_id")).where("external_id IS NOT NULL AND trim(external_id) <> ''").write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name(args.catalog, args.schema, "bridge_person_external_ids"))
    if names_path:
        spark.read.option("header", "true").option("escape", '"').csv(names_path).withColumn("source_file", lit("names.csv")).write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name(args.catalog, args.schema, "dim_person_names"))
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "ingest_register_files", "SUCCEEDED", len(paths), 1 if people_path else 0, 1 if people_path else 0, run_mode=args.run_mode)


if __name__ == "__main__":
    main()
