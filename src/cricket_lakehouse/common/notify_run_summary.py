from __future__ import annotations

import argparse
import json
import smtplib
import urllib.request
from email.message import EmailMessage

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import LongType, StringType, StructField, StructType

SUMMARY_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("summary_text", StringType(), False),
        StructField("zip_files_extracted", LongType(), False),
        StructField("json_files_extracted", LongType(), False),
        StructField("bronze_inserted_matches", LongType(), False),
    ]
)

TABLE_COUNT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("new_count", LongType(), False),
        StructField("old_count", LongType(), False),
        StructField("total_count", LongType(), False),
    ]
)

COUNTED_TABLES = [
    ("all_bronze_raw_matches", "all", "bronze_raw_matches"),
    ("all_silver_matches", "all", "silver_matches"),
    ("all_silver_deliveries", "all", "silver_deliveries"),
    ("all_silver_wickets", "all", "silver_wickets"),
    ("all_gold_player_batting_stats", "all", "gold_player_batting_stats"),
    ("all_gold_bowler_stats", "all", "gold_bowler_stats"),
    ("all_gold_team_match_summary", "all", "gold_team_match_summary"),
    ("all_gold_league_season_summary", "all", "gold_league_season_summary"),
    ("showcase_silver_matches", "showcase", "silver_matches"),
    ("showcase_silver_deliveries", "showcase", "silver_deliveries"),
    ("showcase_silver_wickets", "showcase", "silver_wickets"),
    ("showcase_gold_ai_player_cards", "showcase", "gold_ai_player_cards"),
    ("showcase_gold_ai_team_season_cards", "showcase", "gold_ai_team_season_cards"),
    ("showcase_gold_ai_match_facts", "showcase", "gold_ai_match_facts"),
    ("focus_gold_ai_match_facts", "showcase", "gold_ai_match_facts_focus_leagues"),
    ("other_gold_ai_match_facts", "showcase", "gold_ai_match_facts_other_leagues"),
    ("focus_gold_ai_player_cards", "showcase", "gold_ai_player_cards_focus_leagues"),
    ("other_gold_ai_player_cards", "showcase", "gold_ai_player_cards_other_leagues"),
    ("focus_gold_ai_team_season_cards", "showcase", "gold_ai_team_season_cards_focus_leagues"),
    ("other_gold_ai_team_season_cards", "showcase", "gold_ai_team_season_cards_other_leagues"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="cricket")
    parser.add_argument("--all-schema", default="cricket_all")
    parser.add_argument("--showcase-schema", default="cricinsights_src2")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--pipeline-name", default="cricket_incremental_zip_pipeline")
    parser.add_argument("--email-enabled", default="false")
    parser.add_argument("--email-to", default="")
    parser.add_argument("--email-from", default="")
    parser.add_argument("--smtp-host", default="")
    parser.add_argument("--smtp-port", type=int, default=587)
    parser.add_argument("--smtp-user", default="")
    parser.add_argument("--smtp-password-secret-scope", default="")
    parser.add_argument("--smtp-password-secret-key", default="")
    parser.add_argument("--google-chat-enabled", default="false")
    parser.add_argument("--google-chat-webhook-secret-scope", default="")
    parser.add_argument("--google-chat-webhook-secret-key", default="")
    return parser.parse_args()


def table_name(args: argparse.Namespace, schema: str, name: str) -> str:
    return f"{args.catalog}.{schema}.{name}"


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001 - Spark raises different catalog exceptions by runtime.
        return False


def scalar_sql(spark: SparkSession, sql: str, default: int = 0) -> int:
    try:
        row = spark.sql(sql).first()
        return int(row[0]) if row and row[0] is not None else default
    except Exception:  # noqa: BLE001 - Summary email should not fail on one missing metric.
        return default


def count_table(spark: SparkSession, table: str) -> int:
    if not table_exists(spark, table):
        return 0
    return scalar_sql(spark, f"SELECT COUNT(*) FROM {table}")


def table_columns(spark: SparkSession, table: str) -> list[str]:
    if not table_exists(spark, table):
        return []
    try:
        return spark.table(table).columns
    except Exception:  # noqa: BLE001 - Summary should not fail on one missing table.
        return []


def sql_string(value: str) -> str:
    return value.replace("'", "''")


def latest_prior_total_count(spark: SparkSession, audit_table: str, display_name: str, run_id: str) -> int | None:
    if not table_exists(spark, audit_table):
        return None
    try:
        row = spark.sql(
            f"""
            SELECT total_count
            FROM {audit_table}
            WHERE table_name = '{sql_string(display_name)}'
              AND run_id <> '{sql_string(run_id)}'
            ORDER BY recorded_at DESC
            LIMIT 1
            """
        ).first()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:  # noqa: BLE001 - Fall back to current-table calculation.
        return None


def build_table_count_rows(spark: SparkSession, args: argparse.Namespace) -> list[tuple[str, str, int, int, int]]:
    audit_table = table_name(args, args.all_schema, "pipeline_table_run_counts")
    rows = []
    for display_name, schema_key, raw_table_name in COUNTED_TABLES:
        schema = args.all_schema if schema_key == "all" else args.showcase_schema
        full_name = table_name(args, schema, raw_table_name)
        total_count = count_table(spark, full_name)
        columns = table_columns(spark, full_name)
        run_count: int | None = None
        if "pipeline_run_id" in columns:
            run_count = scalar_sql(
                spark,
                f"""
                SELECT COUNT(*)
                FROM {full_name}
                WHERE pipeline_run_id = '{sql_string(args.run_id)}'
                """,
            )

        prior_total = latest_prior_total_count(spark, audit_table, display_name, args.run_id)
        if run_count is not None:
            new_count = run_count
            old_count = max(total_count - new_count, 0)
        elif prior_total is not None:
            old_count = prior_total
            new_count = max(total_count - old_count, 0)
        else:
            old_count = total_count
            new_count = 0

        rows.append((args.run_id, display_name, int(new_count), int(old_count), int(total_count)))
    return rows


def format_count_table(rows: list[tuple[str, str, int, int, int]]) -> str:
    headers = ("table_name", "new_count", "old_count", "total_count")
    data_rows = [(row[1], f"{row[2]:,}", f"{row[3]:,}", f"{row[4]:,}") for row in rows]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in data_rows))
        for index in range(len(headers))
    ]

    header_line = " | ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))
    separator = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(
            [
                row[0].ljust(widths[0]),
                row[1].rjust(widths[1]),
                row[2].rjust(widths[2]),
                row[3].rjust(widths[3]),
            ]
        )
        for row in data_rows
    ]
    return "\n".join([header_line, separator, *body])


def read_secret(spark: SparkSession, scope: str, key: str) -> str:
    if not scope or not key:
        return ""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark).secrets.get(scope=scope, key=key)
    except Exception:  # noqa: BLE001 - DBUtils availability differs between runtimes.
        return ""


def build_summary(
    spark: SparkSession, args: argparse.Namespace
) -> tuple[str, dict[str, int], list[tuple[str, str, int, int, int]]]:
    zip_manifest = table_name(args, args.all_schema, "pipeline_zip_manifest")
    metrics = table_name(args, args.all_schema, "pipeline_task_metrics")

    zip_files_extracted = scalar_sql(
        spark,
        f"""
        SELECT COUNT(*)
        FROM {zip_manifest}
        WHERE run_id = '{args.run_id}' AND status = 'extracted'
        """,
    )
    json_files_extracted = scalar_sql(
        spark,
        f"""
        SELECT COALESCE(SUM(extracted_json_files), 0)
        FROM {zip_manifest}
        WHERE run_id = '{args.run_id}' AND status = 'extracted'
        """,
    )
    bronze_inserted = scalar_sql(
        spark,
        f"""
        SELECT COALESCE(MAX(metric_value), 0)
        FROM {metrics}
        WHERE run_id = '{args.run_id}'
          AND stage = 'bronze_ingest_raw_matches'
          AND metric_name = 'inserted_matches'
        """,
    )

    table_count_rows = build_table_count_rows(spark, args)

    lines = [
        f"Pipeline: {args.pipeline_name}",
        f"Run ID: {args.run_id}",
        "Status: summary task completed. Check Databricks job task states for any upstream failure.",
        "",
        "New data processed:",
        f"- ZIP files extracted: {zip_files_extracted}",
        f"- JSON files extracted: {json_files_extracted}",
        f"- New bronze matches inserted: {bronze_inserted}",
        "",
        "Table counts:",
        "```",
        format_count_table(table_count_rows),
        "```",
    ]
    return "\n".join(lines), {
        "zip_files_extracted": zip_files_extracted,
        "json_files_extracted": json_files_extracted,
        "bronze_inserted_matches": bronze_inserted,
    }, table_count_rows


def write_summary_table(
    spark: SparkSession, args: argparse.Namespace, summary_text: str, metrics: dict[str, int]
) -> None:
    summary_table = table_name(args, args.all_schema, "pipeline_run_summaries")
    rows = [
        (
            args.run_id,
            args.pipeline_name,
            "completed",
            summary_text,
            metrics["zip_files_extracted"],
            metrics["json_files_extracted"],
            metrics["bronze_inserted_matches"],
        )
    ]
    (
        spark.createDataFrame(rows, SUMMARY_SCHEMA)
        .withColumn("recorded_at", current_timestamp())
        .withColumn("showcase_schema", lit(args.showcase_schema))
        .write.format("delta")
        .mode("append")
        .saveAsTable(summary_table)
    )


def write_table_count_audit(
    spark: SparkSession, args: argparse.Namespace, table_count_rows: list[tuple[str, str, int, int, int]]
) -> None:
    audit_table = table_name(args, args.all_schema, "pipeline_table_run_counts")
    (
        spark.createDataFrame(table_count_rows, TABLE_COUNT_SCHEMA)
        .withColumn("recorded_at", current_timestamp())
        .withColumn("showcase_schema", lit(args.showcase_schema))
        .write.format("delta")
        .mode("append")
        .saveAsTable(audit_table)
    )


def send_email(spark: SparkSession, args: argparse.Namespace, summary_text: str) -> None:
    if args.email_enabled.lower() != "true":
        return

    missing = [
        name
        for name, value in {
            "email-to": args.email_to,
            "email-from": args.email_from,
            "smtp-host": args.smtp_host,
            "smtp-user": args.smtp_user,
        }.items()
        if not value
    ]
    password = read_secret(
        spark, args.smtp_password_secret_scope, args.smtp_password_secret_key
    )
    if not password:
        missing.append("smtp-password-secret")
    if missing:
        raise ValueError(f"Email enabled but missing configuration: {', '.join(missing)}")

    message = EmailMessage()
    message["Subject"] = f"CricInsights pipeline summary - run {args.run_id}"
    message["From"] = args.email_from
    message["To"] = args.email_to
    message.set_content(summary_text)

    with smtplib.SMTP(args.smtp_host, args.smtp_port) as smtp:
        smtp.starttls()
        smtp.login(args.smtp_user, password)
        smtp.send_message(message)


def send_google_chat(spark: SparkSession, args: argparse.Namespace, summary_text: str) -> None:
    if args.google_chat_enabled.lower() != "true":
        return

    webhook_url = read_secret(
        spark, args.google_chat_webhook_secret_scope, args.google_chat_webhook_secret_key
    )
    if not webhook_url:
        raise ValueError("Google Chat webhook enabled but webhook secret is missing")

    payload = json.dumps({"text": summary_text}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("cricket-notify-run-summary").getOrCreate()
    summary_text, metrics, table_count_rows = build_summary(spark, args)
    write_summary_table(spark, args, summary_text, metrics)
    write_table_count_audit(spark, args, table_count_rows)
    print(summary_text)
    send_email(spark, args, summary_text)
    send_google_chat(spark, args, summary_text)


if __name__ == "__main__":
    main()
