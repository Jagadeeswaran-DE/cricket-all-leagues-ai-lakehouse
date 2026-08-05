from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    element_at,
    explode_outer,
    from_json,
    lit,
    lower,
    posexplode_outer,
    when,
)

from cricket_lakehouse.common.audit import append_audit_row

DELIVERY_SCHEMA = """
STRUCT<
  info: STRUCT<
    dates: ARRAY<STRING>,
    event: STRUCT<name: STRING>,
    gender: STRING,
    season: STRING,
    venue: STRING
  >,
  innings: ARRAY<STRUCT<
    team: STRING,
    overs: ARRAY<STRUCT<
      over: INT,
      deliveries: ARRAY<STRUCT<
        actual_delivery: STRING,
        batter: STRING,
        bowler: STRING,
        non_striker: STRING,
        runs: STRUCT<batter: INT, extras: INT, total: INT>,
        extras: STRUCT<wides: INT, noballs: INT, byes: INT, legbyes: INT, penalty: INT>,
        wickets: ARRAY<STRUCT<
          kind: STRING,
          player_out: STRING,
          fielders: ARRAY<STRUCT<name: STRING>>
        >>
      >>
    >>
  >>
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
    spark = SparkSession.builder.appName("cricket-silver-build-deliveries").getOrCreate()
    task_name = "cricinsights_silver_build_deliveries" if args.bronze_schema else "all_silver_build_deliveries"

    incremental = args.incremental.lower() == "true"
    bronze_source = spark.table(prefixed_table_name(args, args.bronze_table_prefix, "bronze_raw_matches"))
    if incremental and "pipeline_run_id" not in bronze_source.columns:
        append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
        return

    bronze = bronze_source.select(
        "match_id",
        "pipeline_run_id",
        from_json(col("raw_json"), DELIVERY_SCHEMA).alias("record"),
    )
    if incremental:
        bronze = bronze.where(col("pipeline_run_id") == args.run_id)

    leagues = target_leagues(args)
    if leagues:
        bronze = bronze.where(col("record.info.event.name").isin(leagues))

    innings_df = bronze.select(
        "match_id",
        "pipeline_run_id",
        col("record.info.season").cast("string").alias("season"),
        col("record.info.event.name").alias("league_name"),
        col("record.info.event.name").alias("competition_name"),
        col("record.info.gender").alias("gender"),
        element_at(col("record.info.dates"), 1).cast("date").alias("match_date"),
        col("record.info.venue").alias("venue"),
        posexplode_outer("record.innings").alias("innings_index", "inning"),
    ).withColumn("league_group", league_group_expr())

    overs_df = innings_df.select(
        "match_id",
        "pipeline_run_id",
        "season",
        "league_name",
        "league_group",
        "competition_name",
        "gender",
        "match_date",
        "venue",
        "innings_index",
        col("inning.team").alias("batting_team"),
        posexplode_outer("inning.overs").alias("over_index", "over_record"),
    )

    deliveries = overs_df.select(
        "match_id",
        "pipeline_run_id",
        "season",
        "league_name",
        "league_group",
        "competition_name",
        "gender",
        "match_date",
        "venue",
        "innings_index",
        "batting_team",
        col("over_record.over").alias("over_number"),
        posexplode_outer("over_record.deliveries").alias("delivery_index", "delivery"),
    ).select(
        "match_id",
        "pipeline_run_id",
        "season",
        "league_name",
        "league_group",
        "competition_name",
        "gender",
        "match_date",
        "venue",
        "innings_index",
        "batting_team",
        "over_number",
        "delivery_index",
        (col("innings_index") + 1).alias("innings_number"),
        (col("delivery_index") + 1).alias("delivery_sequence"),
        col("delivery.actual_delivery").alias("actual_delivery"),
        col("delivery.batter").alias("batter"),
        col("delivery.bowler").alias("bowler"),
        col("delivery.non_striker").alias("non_striker"),
        col("delivery.runs.batter").alias("batter_runs"),
        col("delivery.runs.extras").alias("extras_runs"),
        col("delivery.runs.total").alias("total_runs"),
        col("delivery.extras").alias("extras"),
        col("delivery.wickets").alias("wickets"),
    )

    wickets = deliveries.select(
        "match_id",
        "pipeline_run_id",
        "season",
        "league_name",
        "league_group",
        "competition_name",
        "gender",
        "match_date",
        "innings_index",
        "batting_team",
        "over_number",
        "delivery_index",
        "actual_delivery",
        "batter",
        "bowler",
        explode_outer("wickets").alias("wicket"),
    ).where(col("wicket").isNotNull())

    wickets = wickets.select(
        "match_id",
        "pipeline_run_id",
        "season",
        "league_name",
        "league_group",
        "competition_name",
        "gender",
        "match_date",
        "innings_index",
        "batting_team",
        "over_number",
        "delivery_index",
        "actual_delivery",
        "batter",
        "bowler",
        col("wicket.kind").alias("wicket_kind"),
        col("wicket.player_out").alias("player_out"),
        col("wicket.fielders").alias("fielders"),
    )

    deliveries_table = table_name(args, "silver_deliveries")
    wickets_table = table_name(args, "silver_wickets")

    delivery_count = deliveries.select("match_id").count()
    wicket_count = wickets.count()
    if delivery_count == 0:
        append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
        return

    if incremental and table_exists(spark, deliveries_table):
        deliveries.createOrReplaceTempView("cricket_silver_delivery_batch")
        spark.sql(f"DELETE FROM {deliveries_table} WHERE EXISTS (SELECT 1 FROM cricket_silver_delivery_batch b WHERE {deliveries_table}.match_id = b.match_id)")
        wickets.createOrReplaceTempView("cricket_silver_wicket_batch")
        if table_exists(spark, wickets_table):
            spark.sql(f"DELETE FROM {wickets_table} WHERE EXISTS (SELECT 1 FROM cricket_silver_wicket_batch b WHERE {wickets_table}.match_id = b.match_id)")

    write_mode = "append" if incremental else "overwrite"
    deliveries_writer = deliveries.write.format("delta").mode(write_mode)
    if incremental:
        deliveries_writer = deliveries_writer.option("mergeSchema", "true")
    else:
        deliveries_writer = deliveries_writer.option("overwriteSchema", "true")
    if not table_exists(spark, deliveries_table):
        deliveries_writer = deliveries_writer.partitionBy("season")
    deliveries_writer.saveAsTable(deliveries_table)

    wickets_writer = wickets.write.format("delta").mode(write_mode)
    if incremental:
        wickets_writer = wickets_writer.option("mergeSchema", "true")
    else:
        wickets_writer = wickets_writer.option("overwriteSchema", "true")
    if not table_exists(spark, wickets_table):
        wickets_writer = wickets_writer.partitionBy("season")
    wickets_writer.saveAsTable(wickets_table)
    append_audit_row(spark, args.catalog, args.bronze_schema or args.schema, args.run_id, task_name, "SUCCEEDED", delivery_count, delivery_count, wicket_count, run_mode="incremental", started_at=task_started_at)


if __name__ == "__main__":
    main()
