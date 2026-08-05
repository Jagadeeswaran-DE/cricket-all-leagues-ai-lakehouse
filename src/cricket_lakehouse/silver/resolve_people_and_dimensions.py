from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

from pyspark.sql import SparkSession

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))
from pyspark.sql.functions import col, explode, from_json, lit, map_entries

from cricket_lakehouse.common.audit import append_audit_row, table_name

DIMENSION_SCHEMA = "STRUCT<info: STRUCT<event: STRUCT<name: STRING>, teams: ARRAY<STRING>, venue: STRING, city: STRING, dates: ARRAY<STRING>, gender: STRING, season: STRING, registry: STRUCT<people: MAP<STRING, STRING>>, officials: STRUCT<match_referees: ARRAY<STRING>, tv_umpires: ARRAY<STRING>, umpires: ARRAY<STRING>, reserve_umpire: STRING>>>"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    parser.add_argument("--incremental", default="true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-resolve-people-and-dimensions").getOrCreate()
    bronze_table = table_name(args.catalog, args.schema, "bronze_raw_matches")
    if not spark.catalog.tableExists(bronze_table):
        append_audit_row(spark, args.catalog, args.schema, args.run_id, "resolve_people_and_dimensions", "SUCCEEDED", run_mode=args.run_mode, started_at=task_started_at)
        return
    bronze = spark.table(bronze_table)
    if args.incremental.lower() == "true":
        bronze = bronze.where(col("pipeline_run_id") == args.run_id)
    parsed = bronze.select("match_id", "source_file", from_json("raw_json", DIMENSION_SCHEMA).alias("record"))
    if not parsed.take(1):
        append_audit_row(spark, args.catalog, args.schema, args.run_id, "resolve_people_and_dimensions", "SUCCEEDED", run_mode=args.run_mode, started_at=task_started_at)
        return

    matches = parsed.select("match_id", "source_file", "record.info.*").withColumn("league_name", col("event.name")).drop("event")
    matches.select("match_id", "league_name", "gender", "season", "venue", "city", "dates", "teams", "source_file").write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "silver_match_teams"))
    matches.select("match_id", explode("teams").alias("team_name"), "league_name", "season", "gender", "source_file").write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "dim_team"))
    matches.select("match_id", "venue", "city", "source_file").where(col("venue").isNotNull()).dropDuplicates(["venue", "city"]).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "dim_venue"))
    matches.select("match_id", "league_name", "season", "gender", "source_file").where(col("league_name").isNotNull()).dropDuplicates().write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "dim_competition"))

    people = parsed.select("match_id", "source_file", explode(map_entries("record.info.registry.people")).alias("person" )).select("match_id", "source_file", col("person.key").alias("person_name"), col("person.value").alias("person_id")).withColumn("source_role", lit("registry"))
    people.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "silver_player_registry"))
    if spark.catalog.tableExists(table_name(args.catalog, args.schema, "dim_people")):
        unresolved = people.join(spark.table(table_name(args.catalog, args.schema, "dim_people")).select(col("identifier").alias("person_id")).distinct(), "person_id", "left_anti").withColumn("run_id", lit(args.run_id)).withColumn("recorded_at", lit(None).cast("timestamp"))
        unresolved.select("run_id", "match_id", "person_name", "source_role", "source_file", "recorded_at").write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(table_name(args.catalog, args.schema, "pipeline_unresolved_people"))
    append_audit_row(spark, args.catalog, args.schema, args.run_id, "resolve_people_and_dimensions", "SUCCEEDED", parsed.count(), matches.count(), people.count(), run_mode=args.run_mode, started_at=task_started_at)


if __name__ == "__main__":
    main()
