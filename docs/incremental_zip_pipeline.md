# Incremental ZIP Pipeline

## Purpose

This pipeline lets CricInsights accept new Cricsheet-style ZIP files in a Unity Catalog volume, extract them, load only new matches, refresh downstream Delta tables, and record a detailed run summary.

## Default Locations

ZIP landing path:

```text
/Volumes/cricket/cricket_all/cricket_all_raw/zips/*.zip
```

Extracted JSON path:

```text
/Volumes/cricket/cricket_all/cricket_all_raw/extracted
```

Whole-data schema:

```text
cricket.cricket_all
```

CricInsights chatbot schema:

```text
cricket.cricinsights_src2
```

Focus-versus-other AI serving tables:

```text
cricket.cricinsights_src2.gold_ai_match_facts_focus_leagues
cricket.cricinsights_src2.gold_ai_match_facts_other_leagues
cricket.cricinsights_src2.gold_ai_player_cards_focus_leagues
cricket.cricinsights_src2.gold_ai_player_cards_other_leagues
cricket.cricinsights_src2.gold_ai_team_season_cards_focus_leagues
cricket.cricinsights_src2.gold_ai_team_season_cards_other_leagues
```

## Job

```powershell
databricks bundle run cricket_incremental_zip_pipeline_job -t dev --profile jagadeeswaran
```

## Daily Schedule and Source Download

The bundle runs the job daily at 10:00 Asia/Kolkata and checks the official
Cricsheet sources before processing the landing directory below:

```text
/Volumes/cricket/cricket_all/cricket_all_raw/zips/
```

The job downloads the official `all_json.zip`, `people.csv`, and `names.csv`
into the Volume when their source metadata changes. Unchanged sources are
skipped. ZIPs uploaded by users to the same root are processed by the same run.

## Task Flow

1. `extract_new_zip_files`
   - Finds ZIP files in the configured volume path.
   - Skips ZIPs already recorded as extracted.
   - Extracts only JSON files.
   - Records ZIP-level and file-level manifests.

2. `bronze_ingest_raw_matches`
   - Reads only JSON files extracted in the current run.
   - Skips match IDs already present in bronze.
   - Appends new raw JSON records.

3. `all_silver_build_matches` and `all_silver_build_deliveries`
   - Parses only new bronze rows for the current run.
   - Appends unseen match IDs into all-data silver tables.

4. `all_gold_build_analytics`
   - Detects seasons touched by the current run.
   - Rewrites only affected season partitions in all-data gold tables.

5. `cricinsights_silver_build_matches` and `cricinsights_silver_build_deliveries`
   - Parses only current-run rows matching IPL, Big Bash/WBBL, and The Hundred men's/women's competitions.
   - Appends matching data into `cricket.cricinsights_src2`.

6. `cricinsights_gold_build_analytics`
   - Refreshes affected season partitions in CricInsights gold tables.

7. `build_league_segment_tables`
   - Publishes IPL, Big Bash/WBBL, and The Hundred men's/women's data into `_focus_leagues` tables.
   - Publishes every other competition into `_other_leagues` tables.

8. `notify_run_summary`
   - Writes a summary row to `cricket.cricket_all.pipeline_run_summaries`.
   - Writes per-table old/new/total counts to `cricket.cricket_all.pipeline_table_run_counts`.
   - Sends detailed email if SMTP variables and secrets are configured.
   - Sends a Google Chat message if webhook variables and secrets are configured.

## Audit Tables

- `cricket.cricket_all.pipeline_zip_manifest`
- `cricket.cricket_all.pipeline_extracted_file_manifest`
- `cricket.cricket_all.pipeline_task_metrics`
- `cricket.cricket_all.pipeline_run_summaries`
- `cricket.cricket_all.pipeline_table_run_counts`

## Notification Format

The Google Chat notification includes:

- source fetch status for each official source, including bytes and duration;
- ZIP, JSON, and register-file counts as new in the run versus total available;
- every Control, Source, Bronze, Silver, Gold, and AI task with status,
  duration, completion time, and processing counts;
- a slow-task section for completed tasks taking more than 10 minutes; and
- table-level `new_count`, `old_count`, and `total_count` values.

Because the notification task runs after the pipeline, the slow-task section
identifies completed slow tasks. Live alerts while a task is still running need
a separate monitoring job.

Google Chat and email summaries include a monospaced table:

```text
table_name | new_count | old_count | total_count
```

For bronze and silver tables, `new_count` is counted from `pipeline_run_id`.
For gold aggregate tables, `new_count` is the increase versus the latest prior
audited total.

## Email Setup

Set bundle variables for:

- `summary_email_enabled = "true"`
- `summary_email_to`
- `summary_email_from`
- `smtp_host`
- `smtp_port`
- `smtp_user`
- `smtp_password_secret_scope`
- `smtp_password_secret_key`

The SMTP password must be stored in Databricks Secrets.

## Google Chat Webhook Setup

Create an incoming webhook in the Google Chat space and store the full webhook URL in Databricks Secrets.

```powershell
databricks secrets create-scope cricinsights-alerts --profile jagadeeswaran
databricks secrets put-secret cricinsights-alerts google-chat-webhook-url --profile jagadeeswaran
```

Deploy the job with Google Chat enabled:

```powershell
databricks bundle deploy -t dev --profile jagadeeswaran `
  --var="google_chat_enabled=true" `
  --var="google_chat_webhook_secret_scope=cricinsights-alerts" `
  --var="google_chat_webhook_secret_key=google-chat-webhook-url"
```

The webhook URL contains secret key and token values, so do not put it directly in bundle YAML or source files.
