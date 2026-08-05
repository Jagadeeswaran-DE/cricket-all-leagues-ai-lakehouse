from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, from_json, lit, lower, sha2, when

from cricket_lakehouse.common.audit import append_audit_row

MATCH_SCHEMA = """
STRUCT<
  meta: STRUCT<data_version: STRING, created: STRING, revision: INT>,
  info: STRUCT<
    balls_per_over: INT,
    city: STRING,
    dates: ARRAY<STRING>,
    event: STRUCT<name: STRING, match_number: STRING, stage: STRING, group: STRING>,
    gender: STRING,
    match_type: STRING,
    outcome: STRUCT<
      winner: STRING,
      result: STRING,
      by: STRUCT<runs: INT, wickets: INT>,
      eliminator: STRING,
      method: STRING
    >,
    overs: INT,
    player_of_match: ARRAY<STRING>,
    players: MAP<STRING, ARRAY<STRING>>,
    registry: STRUCT<people: MAP<STRING, STRING>>,
    season: STRING,
    team_type: STRING,
    teams: ARRAY<STRING>,
    toss: STRUCT<decision: STRING, winner: STRING>,
    venue: STRING
  >
>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--table-prefix", default="")
    parser.add_argument("--bronze-schema", default="")
    parser.add_argument("--bronze-table-prefix", default="")
    parser.add_argument("--target-leagues", default="")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--incremental", default="false")
    return parser.parse_args()


def table_name(args: argparse.Namespace, name: str) -> str:
    return f"{args.catalog}.{args.schema}.{args.table_prefix}{name}"


def prefixed_table_name(args: argparse.Namespace, prefix: str, name: str) -> str:
    source_schema = args.bronze_schema or args.schema
    return f"{args.catalog}.{source_schema}.{prefix}{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - Spark raises different catalog exceptions by runtime.
        return False


def target_leagues(args: argparse.Namespace) -> list[str]:
    raw_value = args.target_leagues or ""
    return [league.strip() for league in raw_value.split(",") if league.strip()]


def league_group_expr() -> object:
    return (
        when(lower(col("league_name")).contains("indian premier league"), lit("IPL"))
        .when(lower(col("league_name")).contains("big bash"), lit("Big Bash"))
        .when(lower(col("league_name")).contains("the hundred"), lit("The Hundred"))
        .otherwise(col("league_name"))
    )


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-silver-build-matches").getOrCreate()
    task_name = "cricinsights_silver_build_matches" if args.bronze_schema else "all_silver_build_matches"

    incremental = args.incremental.lower() == "true"
    bronze_source = spark.table(prefixed_table_name(args, args.bronze_table_prefix, "bronze_raw_matches"))
    if incremental and "pipeline_run_id" not in bronze_source.columns:
        append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
        return

    bronze = bronze_source.select(
        "match_id",
        "source_file",
        "raw_json",
        "pipeline_run_id",
        "ingestion_timestamp",
        from_json(col("raw_json"), MATCH_SCHEMA).alias("record"),
    )
    if incremental:
        bronze = bronze.where(col("pipeline_run_id") == args.run_id)

    leagues = target_leagues(args)
    if leagues:
        bronze = bronze.where(col("record.info.event.name").isin(leagues))

    matches = bronze.select(
        col("match_id"),
        col("source_file"),
        col("record.meta.data_version").alias("data_version"),
        col("record.meta.created").cast("date").alias("source_created_date"),
        col("record.meta.revision").alias("source_revision"),
        element_at(col("record.info.dates"), 1).cast("date").alias("match_date"),
        col("record.info.season").cast("string").alias("season"),
        col("record.info.event.name").alias("league_name"),
        col("record.info.event.match_number").cast("string").alias("event_match_number"),
        col("record.info.match_type").alias("match_type"),
        col("record.info.gender").alias("gender"),
        col("record.info.city").alias("city"),
        col("record.info.venue").alias("venue"),
        col("record.info.teams").alias("teams"),
        element_at(col("record.info.teams"), 1).alias("team_1"),
        element_at(col("record.info.teams"), 2).alias("team_2"),
        col("record.info.toss.winner").alias("toss_winner"),
        col("record.info.toss.decision").alias("toss_decision"),
        col("record.info.outcome.winner").alias("winner"),
        col("record.info.outcome.by.runs").alias("win_by_runs"),
        col("record.info.outcome.by.wickets").alias("win_by_wickets"),
        col("record.info.outcome.result").alias("result"),
        col("record.info.player_of_match").alias("players_of_match"),
        col("pipeline_run_id"),
        col("ingestion_timestamp"),
        sha2(col("raw_json"), 256).alias("match_record_hash"),
    ).withColumn("league_group", league_group_expr()).dropDuplicates(["match_id"])

    target_table = table_name(args, "silver_matches")
    match_count = matches.select("match_id").count()
    if match_count == 0:
        append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
        return

    if incremental and table_exists(spark, target_table):
        matches.createOrReplaceTempView("cricket_silver_match_batch")
        spark.sql(f"DELETE FROM {target_table} WHERE EXISTS (SELECT 1 FROM cricket_silver_match_batch b WHERE {target_table}.match_id = b.match_id)")

    write_mode = "append" if incremental else "overwrite"
    writer = matches.write.format("delta").mode(write_mode)
    if incremental:
        writer = writer.option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")
    if not table_exists(spark, target_table):
        writer = writer.partitionBy("season")
    writer.saveAsTable(target_table)
    append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", match_count, match_count, match_count, run_mode="incremental", started_at=task_started_at)


if __name__ == "__main__":
    main()
