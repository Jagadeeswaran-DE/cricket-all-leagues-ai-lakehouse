from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    expr,
    max,
    round,
    sum,
    when,
)

from cricket_lakehouse.common.audit import append_audit_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--schema", default="cricket_all")
    parser.add_argument("--table-prefix", default="")
    parser.add_argument("--audit-schema", default="")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--incremental", default="false")
    return parser.parse_args()


def full_table_name(args: argparse.Namespace, name: str) -> str:
    return f"{args.catalog}.{args.schema}.{args.table_prefix}{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - Spark raises different catalog exceptions by runtime.
        return False


def replace_where_for_seasons(seasons: list[str]) -> str:
    quoted = []
    for season in seasons:
        escaped = season.replace("'", "''")
        quoted.append(f"'{escaped}'")
    return f"season IN ({', '.join(quoted)})"


def write_delta_table(
    spark: SparkSession,
    args: argparse.Namespace,
    output_table: str,
    frame: object,
    incremental: bool,
    affected_seasons: list[str],
) -> None:
    target_table = full_table_name(args, output_table)
    if incremental and table_exists(spark, target_table):
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where_for_seasons(affected_seasons))
            .saveAsTable(target_table)
        )
        return

    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("season")
        .saveAsTable(target_table)
    )


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-gold-build-analytics").getOrCreate()
    task_name = "cricinsights_gold_build_analytics" if args.schema == "cricinsights_src2" else "all_gold_build_analytics"
    audit_schema = args.audit_schema or args.schema

    deliveries = spark.table(full_table_name(args, "silver_deliveries"))
    wickets = spark.table(full_table_name(args, "silver_wickets"))
    matches = spark.table(full_table_name(args, "silver_matches"))
    incremental = args.incremental.lower() == "true"

    affected_seasons: list[str] = []
    if incremental:
        if "pipeline_run_id" not in matches.columns:
            append_audit_row(spark, args.catalog, audit_schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
            return
        affected_seasons = [
            row.season
            for row in matches.where(col("pipeline_run_id") == args.run_id)
            .select("season")
            .where(col("season").isNotNull())
            .distinct()
            .collect()
        ]
        if not affected_seasons:
            append_audit_row(spark, args.catalog, audit_schema, args.run_id, task_name, "SUCCEEDED", run_mode="incremental", started_at=task_started_at)
            return
        deliveries = deliveries.where(col("season").isin(affected_seasons))
        wickets = wickets.where(col("season").isin(affected_seasons))
        matches = matches.where(col("season").isin(affected_seasons))

    league_dimensions = ["league_group", "league_name", "gender", "season"]

    legal_ball = ~(
        col("extras").getField("wides").isNotNull() | col("extras").getField("noballs").isNotNull()
    )

    batting = (
        deliveries.groupBy(*league_dimensions, col("batter").alias("player"))
        .agg(
            countDistinct("match_id").alias("matches"),
            sum("batter_runs").alias("runs"),
            sum(when(legal_ball, 1).otherwise(0)).alias("balls_faced"),
            sum(when(col("batter_runs") == 4, 1).otherwise(0)).alias("fours"),
            sum(when(col("batter_runs") == 6, 1).otherwise(0)).alias("sixes"),
        )
        .withColumn(
            "strike_rate",
            round(when(col("balls_faced") > 0, col("runs") * 100 / col("balls_faced")), 2),
        )
    )

    bowling_wickets = wickets.where(
        ~col("wicket_kind").isin("run out", "retired hurt", "retired out", "obstructing the field")
    ).groupBy(*league_dimensions, "bowler").agg(count("*").alias("wickets"))

    bowling = (
        deliveries.groupBy(*league_dimensions, "bowler")
        .agg(
            countDistinct("match_id").alias("matches"),
            sum(when(legal_ball, 1).otherwise(0)).alias("legal_balls"),
            sum("total_runs").alias("runs_conceded"),
            sum(when(col("total_runs") == 0, 1).otherwise(0)).alias("dot_balls"),
        )
        .join(bowling_wickets, [*league_dimensions, "bowler"], "left")
        .fillna({"wickets": 0})
        .withColumn("overs", round(col("legal_balls") / 6, 2))
        .withColumn(
            "economy",
            round(when(col("legal_balls") > 0, col("runs_conceded") * 6 / col("legal_balls")), 2),
        )
    )

    team_match_summary = (
        deliveries.groupBy("match_id", *league_dimensions, "match_date", "batting_team")
        .agg(
            sum("total_runs").alias("team_runs"),
            sum(when(col("wickets").isNotNull(), expr("size(wickets)")).otherwise(0)).alias("wickets_lost"),
            sum(when(legal_ball, 1).otherwise(0)).alias("legal_balls"),
        )
        .withColumn("run_rate", round(col("team_runs") * 6 / col("legal_balls"), 2))
    )

    league_season_summary = (
        matches.groupBy(*league_dimensions, "match_type")
        .agg(
            countDistinct("match_id").alias("matches"),
            countDistinct("venue").alias("venues"),
            countDistinct("winner").alias("winning_teams"),
        )
        .join(
            team_match_summary.groupBy(*league_dimensions).agg(
                round(avg("team_runs"), 2).alias("avg_team_score")
            ),
            league_dimensions,
            "left",
        )
    )

    ai_player_cards = (
        batting.alias("bat")
        .join(
            bowling.withColumnRenamed("bowler", "player").alias("bowl"),
            [*league_dimensions, "player"],
            "full",
        )
        .select(
            *league_dimensions,
            "player",
            col("bat.matches").alias("batting_matches"),
            col("bat.runs").alias("batting_runs"),
            col("bat.balls_faced").alias("balls_faced"),
            col("bat.strike_rate").alias("batting_strike_rate"),
            col("bat.fours").alias("fours"),
            col("bat.sixes").alias("sixes"),
            col("bowl.matches").alias("bowling_matches"),
            col("bowl.legal_balls").alias("bowling_legal_balls"),
            col("bowl.runs_conceded").alias("runs_conceded"),
            col("bowl.wickets").alias("bowling_wickets"),
            col("bowl.economy").alias("bowling_economy"),
        )
    )

    ai_team_season_cards = (
        team_match_summary.groupBy(*league_dimensions, col("batting_team").alias("team"))
        .agg(
            countDistinct("match_id").alias("innings"),
            round(avg("team_runs"), 2).alias("avg_score"),
            max("team_runs").alias("highest_score"),
            round(avg("run_rate"), 2).alias("avg_run_rate"),
        )
    )

    ai_match_facts = matches.select(
        "match_id",
        "league_group",
        "league_name",
        "gender",
        "season",
        "match_date",
        "match_type",
        "venue",
        "city",
        "team_1",
        "team_2",
        "toss_winner",
        "toss_decision",
        "winner",
        "win_by_runs",
        "win_by_wickets",
        "result",
        "players_of_match",
    )

    outputs = {
        "gold_player_batting_stats": batting,
        "gold_bowler_stats": bowling,
        "gold_team_match_summary": team_match_summary,
        "gold_league_season_summary": league_season_summary,
        "gold_ai_player_cards": ai_player_cards,
        "gold_ai_team_season_cards": ai_team_season_cards,
        "gold_ai_match_facts": ai_match_facts,
    }

    for output_table, frame in outputs.items():
        write_delta_table(spark, args, output_table, frame, incremental, affected_seasons)
    append_audit_row(spark, args.catalog, audit_schema, args.run_id, task_name, "SUCCEEDED", matches.count(), len(outputs), len(outputs), run_mode="incremental", started_at=task_started_at)


if __name__ == "__main__":
    main()
