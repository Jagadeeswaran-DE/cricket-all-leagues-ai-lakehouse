from __future__ import annotations

import argparse
import json
import re
import smtplib
import urllib.request
from email.message import EmailMessage

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import LongType, StringType, StructField, StructType

SLOW_TASK_SECONDS = 10 * 60

EXPECTED_TASKS = [
    ("Control", "initialize_run"),
    ("Source", "sync_cricsheet_sources"),
    ("Bronze", "extract_new_zip_files"),
    ("Bronze", "ingest_register_files"),
    ("Bronze", "bronze_ingest_raw_matches"),
    ("Silver", "all_silver_build_matches"),
    ("Silver", "all_silver_build_deliveries"),
    ("Silver", "resolve_people_and_dimensions"),
    ("Silver", "validate_silver"),
    ("Gold", "all_gold_build_analytics"),
    ("Silver / AI", "cricinsights_silver_build_matches"),
    ("Silver / AI", "cricinsights_silver_build_deliveries"),
    ("Gold / AI", "cricinsights_gold_build_analytics"),
    ("Gold / AI", "build_league_segment_tables"),
    ("Gold / AI", "build_ai_serving_tables"),
    ("Gold", "validate_gold_and_serving"),
    ("Control", "finalize_run_summary"),
]

SUMMARY_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("summary_text", StringType(), False),
        StructField("zip_files_extracted", LongType(), False),
        StructField("json_files_extracted", LongType(), False),
        StructField("bronze_inserted_matches", LongType(), False),
        StructField("total_json_files", LongType(), False),
        StructField("new_json_files", LongType(), False),
        StructField("slow_task_count", LongType(), False),
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


def format_table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    if not rows:
        return "No records."
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    header_line = " | ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))
    separator = "-+-".join("-" * width for width in widths)
    body = [" | ".join(row[index].ljust(widths[index]) for index in range(len(headers))) for row in rows]
    return "\n".join([header_line, separator, *body])


def format_duration(seconds: float | int | None) -> str:
    total = max(0, int(seconds or 0))
    minutes, remaining = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {remaining:02d}s"


def source_fetch_rows(spark: SparkSession, args: argparse.Namespace) -> list[tuple[str, ...]]:
    manifest = table_name(args, args.all_schema, "pipeline_source_manifest")
    if not table_exists(spark, manifest):
        return []
    rows = spark.sql(
        f"""
        SELECT source_name, source_type, download_status, content_length,
               download_started_at, download_completed_at, error_message
        FROM {manifest}
        WHERE ingestion_run_id = '{sql_string(args.run_id)}'
        ORDER BY source_type, source_name
        """
    ).collect()
    result = []
    for row in rows:
        started, completed = row.download_started_at, row.download_completed_at
        duration = (completed - started).total_seconds() if started and completed else 0
        result.append(
            (
                str(row.source_name or "-"),
                str(row.source_type or "-"),
                str(row.download_status or "-").upper(),
                f"{int(row.content_length or 0):,}",
                format_duration(duration),
                str(row.error_message or "")[:40],
            )
        )
    return result


def file_inventory(spark: SparkSession, args: argparse.Namespace) -> list[tuple[str, ...]]:
    zip_manifest = table_name(args, args.all_schema, "pipeline_zip_manifest")
    file_manifest = table_name(args, args.all_schema, "pipeline_extracted_file_manifest")
    zip_total = scalar_sql(
        spark,
        f"SELECT COUNT(DISTINCT zip_sha256) FROM {zip_manifest} WHERE status IN ('extracted', 'skipped_already_processed')",
    ) if table_exists(spark, zip_manifest) else 0
    zip_new = scalar_sql(
        spark,
        f"SELECT COUNT(DISTINCT zip_sha256) FROM {zip_manifest} WHERE run_id = '{sql_string(args.run_id)}' AND status = 'extracted'",
    ) if table_exists(spark, zip_manifest) else 0
    rows = [("ZIP", zip_new, zip_total)]
    for label, predicate in (("JSON", "lower(file_extension) = '.json'"), ("CSV", "lower(file_extension) = '.csv'")):
        total = scalar_sql(
            spark,
            f"SELECT COUNT(DISTINCT extracted_path) FROM {file_manifest} WHERE {predicate} AND extraction_status = 'extracted'",
        ) if table_exists(spark, file_manifest) else 0
        new = scalar_sql(
            spark,
            f"SELECT COUNT(DISTINCT extracted_path) FROM {file_manifest} WHERE run_id = '{sql_string(args.run_id)}' AND {predicate} AND extraction_status = 'extracted'",
        ) if table_exists(spark, file_manifest) else 0
        rows.append((label, new, total))
    return [(label, f"{new:,}", f"{total:,}") for label, new, total in rows]


def extraction_progress_rows(spark: SparkSession, args: argparse.Namespace) -> list[tuple[str, ...]]:
    progress_table = table_name(args, args.all_schema, "pipeline_task_progress")
    if not table_exists(spark, progress_table):
        return []
    latest: dict[str, object] = {}
    for row in spark.sql(
        f"""
        SELECT * FROM {progress_table}
        WHERE run_id = '{sql_string(args.run_id)}'
          AND task_name = 'extract_new_zip_files'
        ORDER BY updated_at
        """
    ).collect():
        latest[row.current_item] = row
    return [
        (
            str(row.current_item).replace("\\", "/").rsplit("/", 1)[-1],
            f"{int(row.processed_count):,}/{int(row.total_count):,}",
            f"{float(row.percent_complete):.1f}%",
            str(row.status),
            str(row.updated_at)[:19],
        )
        for row in latest.values()
    ]


def latest_task_audit_rows(spark: SparkSession, args: argparse.Namespace) -> dict[str, object]:
    audit = table_name(args, args.all_schema, "pipeline_task_audit")
    if not table_exists(spark, audit):
        return {}
    latest: dict[str, object] = {}
    for row in spark.sql(
        f"SELECT * FROM {audit} WHERE run_id = '{sql_string(args.run_id)}' ORDER BY recorded_at"
    ).collect():
        latest[row.task_name] = row.asDict()
    return latest


def task_status_rows(spark: SparkSession, args: argparse.Namespace) -> list[tuple[str, ...]]:
    latest = latest_task_audit_rows(spark, args)
    rows = []
    for layer, task_name in EXPECTED_TASKS:
        audit = latest.get(task_name, {})
        rows.append(
            (
                layer,
                task_name,
                str(audit.get("status", "NOT_RECORDED")),
                format_duration(audit.get("duration_seconds", 0)),
                f"{int(audit.get('input_count') or 0):,}",
                f"{int(audit.get('output_count') or 0):,}",
                f"{int(audit.get('inserted_count') or 0):,}",
                f"{int(audit.get('updated_count') or 0):,}",
                str(audit.get("completed_at") or "-")[:19],
            )
        )
    return rows


def slow_task_rows(task_rows: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    rows = []
    for row in task_rows:
        duration_text = row[3]
        match = re.fullmatch(r"(?:(\d+)h )?(\d+)m (\d+)s", duration_text)
        seconds = (
            int(match.group(1) or 0) * 3600
            + int(match.group(2)) * 60
            + int(match.group(3))
            if match
            else 0
        )
        if seconds > SLOW_TASK_SECONDS:
            rows.append((row[0], row[1], row[2], duration_text))
    return rows


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
) -> tuple[
    str,
    dict[str, int],
    list[tuple[str, str, int, int, int]],
    list[tuple[str, ...]],
    list[tuple[str, ...]],
    list[tuple[str, ...]],
]:
    zip_manifest = table_name(args, args.all_schema, "pipeline_zip_manifest")

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
        SELECT COALESCE(MAX(inserted_count), 0)
        FROM {table_name(args, args.all_schema, "pipeline_task_audit")}
        WHERE run_id = '{sql_string(args.run_id)}'
          AND task_name = 'bronze_ingest_raw_matches'
        """,
    )

    table_count_rows = build_table_count_rows(spark, args)
    source_rows = source_fetch_rows(spark, args)
    inventory_rows = file_inventory(spark, args)
    progress_rows = extraction_progress_rows(spark, args)
    task_rows = task_status_rows(spark, args)
    slow_rows = slow_task_rows(task_rows)
    task_statuses = {row[2] for row in task_rows}
    run_status = "FAILED" if "FAILED" in task_statuses else "INCOMPLETE" if "NOT_RECORDED" in task_statuses else "SUCCEEDED"
    total_json_files = int(inventory_rows[1][2].replace(",", "")) if len(inventory_rows) > 1 else 0
    new_json_files = int(inventory_rows[1][1].replace(",", "")) if len(inventory_rows) > 1 else 0

    lines = [
        f"Pipeline: {args.pipeline_name}",
        f"Run ID: {args.run_id}",
        f"Status: {run_status}",
        "",
        "1. Source fetch",
        "```",
        format_table(("source", "type", "status", "bytes", "duration", "error"), source_rows),
        "```",
        "",
        "2. File arrival: new this run vs total available",
        "```",
        format_table(("file_type", "new_this_run", "total_available"), inventory_rows),
        "```",
        "Extraction progress checkpoints:",
        "```",
        format_table(("zip", "processed/total", "percent", "status", "updated"), progress_rows),
        "```",
        f"- ZIP files extracted this run: {zip_files_extracted}",
        f"- JSON files extracted this run: {json_files_extracted}",
        f"- New bronze matches inserted: {bronze_inserted}",
        "",
        "3. Layer and task status",
        "```",
        format_table(
            ("layer", "task", "status", "duration", "input", "output", "inserted", "updated", "completed"),
            task_rows,
        ),
        "```",
        "",
        "Slow tasks (>10 minutes):",
        "```",
        format_table(("layer", "task", "status", "duration"), slow_rows)
        if slow_rows
        else "No completed task exceeded 10 minutes.",
        "```",
        "",
        "4. Table counts",
        "```",
        format_count_table(table_count_rows),
        "```",
    ]
    return "\n".join(lines), {
        "zip_files_extracted": zip_files_extracted,
        "json_files_extracted": json_files_extracted,
        "bronze_inserted_matches": bronze_inserted,
        "total_json_files": total_json_files,
        "new_json_files": new_json_files,
        "slow_task_count": len(slow_rows),
    }, table_count_rows, source_rows, inventory_rows, task_rows


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
            metrics["total_json_files"],
            metrics["new_json_files"],
            metrics["slow_task_count"],
        )
    ]
    (
        spark.createDataFrame(rows, SUMMARY_SCHEMA)
        .withColumn("recorded_at", current_timestamp())
        .withColumn("showcase_schema", lit(args.showcase_schema))
        .write.format("delta")
        .option("mergeSchema", "true")
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

    chunks: list[str] = []
    current: list[str] = []
    current_length = 0
    for block in summary_text.split("\n\n"):
        block_length = len(block) + (2 if current else 0)
        if current and current_length + block_length > 7000:
            chunks.append("\n\n".join(current))
            current = []
            current_length = 0
        if len(block) > 7000:
            chunks.extend(block[index : index + 7000] for index in range(0, len(block), 7000))
        else:
            current.append(block)
            current_length += len(block) + (2 if len(current) > 1 else 0)
    if current:
        chunks.append("\n\n".join(current))
    for index, chunk in enumerate(chunks, start=1):
        message = chunk if len(chunks) == 1 else f"CricInsights pipeline summary ({index}/{len(chunks)})\n\n{chunk}"
        payload = json.dumps({"text": message}).encode("utf-8")
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
    summary_text, metrics, table_count_rows, _, _, _ = build_summary(spark, args)
    write_summary_table(spark, args, summary_text, metrics)
    write_table_count_audit(spark, args, table_count_rows)
    print(summary_text)
    send_email(spark, args, summary_text)
    send_google_chat(spark, args, summary_text)


if __name__ == "__main__":
    main()
