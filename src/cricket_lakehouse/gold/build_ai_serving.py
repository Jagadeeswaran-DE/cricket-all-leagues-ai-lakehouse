from __future__ import annotations

import argparse
import os
import sys

_script_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else "")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_script_file)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from cricket_lakehouse.common.audit import append_audit_row, table_name
from cricket_lakehouse.common.competition_config import DEFAULT_FOCUS_LEAGUES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--source-schema", default="cricket_all")
    parser.add_argument("--target-schema", default="cricinsights_src2")
    parser.add_argument("--focus-leagues", default=",")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--run-mode", default="incremental")
    return parser.parse_args()


def focus_values(spark: SparkSession, args: argparse.Namespace) -> list[str]:
    values = [item.strip() for item in args.focus_leagues.split(",") if item.strip()]
    config = table_name(args.catalog, args.source_schema, "config_focus_leagues")
    if not values and spark.catalog.tableExists(config):
        values = [row.league_name for row in spark.table(config).where("enabled = true").select("league_name").collect()]
    return values or list(DEFAULT_FOCUS_LEAGUES)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-build-ai-serving-tables").getOrCreate()
    focus = focus_values(spark, args)
    outputs = ["gold_ai_match_facts", "gold_ai_player_cards", "gold_ai_team_season_cards"]
    total = 0
    for name in outputs:
        source = table_name(args.catalog, args.source_schema, name)
        if not spark.catalog.tableExists(source):
            continue
        frame = spark.table(source)
        if "league_name" not in frame.columns:
            continue
        focus_frame = frame.where(col("league_name").isin(focus)).withColumn("league_segment", lit("focus_leagues"))
        other_frame = frame.where(~col("league_name").isin(focus)).withColumn("league_segment", lit("other_leagues"))
        for segment, data in (("focus_leagues", focus_frame), ("other_leagues", other_frame)):
            data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name(args.catalog, args.target_schema, f"{name}_{segment}"))
            total += data.count()
    append_audit_row(spark, args.catalog, args.source_schema, args.run_id, "build_ai_serving_tables", "SUCCEEDED", total, total, run_mode=args.run_mode)


if __name__ == "__main__":
    main()
