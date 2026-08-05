from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, countDistinct, lit, max, round, when

from cricket_lakehouse.common.audit import append_audit_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--source-schema", default="cricket_all")
    parser.add_argument("--target-schema", default="cricinsights_src2")
    parser.add_argument("--target-leagues", default="")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    return parser.parse_args()


def table_name(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def target_leagues(args: argparse.Namespace) -> list[str]:
    raw_value = args.target_leagues or ""
    return [league.strip() for league in raw_value.split(",") if league.strip()]


def with_segment(frame: DataFrame, segment: str) -> DataFrame:
    return frame.withColumn("league_segment", lit(segment))


def league_group_expression() -> object:
    return (
        when(col("league_name") == "Indian Premier League", lit("IPL"))
        .when(col("league_name").isin("Big Bash League", "Women's Big Bash League"), lit("Big Bash"))
        .when(
            col("league_name").isin("The Hundred Men's Competition", "The Hundred Women's Competition"),
            lit("The Hundred"),
        )
        .otherwise(col("league_name"))
    )


def ensure_league_group(frame: DataFrame) -> DataFrame:
    if "league_group" in frame.columns:
        return frame
    return frame.withColumn("league_group", league_group_expression())


def ensure_ai_dimensions(frame: DataFrame) -> DataFrame:
    with_league_group = ensure_league_group(frame)
    if "gender" in with_league_group.columns:
        return with_league_group
    return with_league_group.withColumn("gender", lit("unknown"))


def write_table(frame: DataFrame, target_table: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("season")
        .saveAsTable(target_table)
    )


def build_match_facts(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    matches = ensure_ai_dimensions(spark.table(table_name(args.catalog, args.source_schema, "silver_matches")))
    return matches.select(
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


def build_player_cards(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    league_dimensions = ["league_group", "league_name", "gender", "season"]
    batting = ensure_ai_dimensions(
        spark.table(table_name(args.catalog, args.source_schema, "gold_player_batting_stats"))
    )
    bowling = ensure_ai_dimensions(
        spark.table(table_name(args.catalog, args.source_schema, "gold_bowler_stats"))
    )
    return (
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


def build_team_season_cards(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    league_dimensions = ["league_group", "league_name", "gender", "season"]
    team_match_summary = ensure_ai_dimensions(
        spark.table(table_name(args.catalog, args.source_schema, "gold_team_match_summary"))
    )
    return (
        team_match_summary.groupBy(*league_dimensions, col("batting_team").alias("team"))
        .agg(
            countDistinct("match_id").alias("innings"),
            round(avg("team_runs"), 2).alias("avg_score"),
            max("team_runs").alias("highest_score"),
            round(avg("run_rate"), 2).alias("avg_run_rate"),
        )
    )


def main() -> None:
    args = parse_args()
    task_started_at = datetime.now(UTC)
    spark = SparkSession.builder.appName("cricket-gold-build-league-segments").getOrCreate()
    leagues = target_leagues(args)
    if not leagues:
        raise ValueError("--target-leagues must contain at least one league name")

    source_frames = {
        "gold_ai_match_facts": build_match_facts(spark, args),
        "gold_ai_player_cards": build_player_cards(spark, args),
        "gold_ai_team_season_cards": build_team_season_cards(spark, args),
    }

    for source_table, source in source_frames.items():
        focus = with_segment(source.where(col("league_name").isin(leagues)), "focus_leagues")
        other = with_segment(source.where(~col("league_name").isin(leagues)), "other_leagues")

        write_table(
            focus,
            table_name(args.catalog, args.target_schema, f"{source_table}_focus_leagues"),
        )
        write_table(
            other,
            table_name(args.catalog, args.target_schema, f"{source_table}_other_leagues"),
        )

    append_audit_row(
        spark,
        args.catalog,
        args.source_schema,
        args.run_id,
        "build_league_segment_tables",
        "SUCCEEDED",
        len(source_frames),
        len(source_frames) * 2,
        len(source_frames) * 2,
        run_mode=args.run_mode,
        started_at=task_started_at,
    )


if __name__ == "__main__":
    main()
