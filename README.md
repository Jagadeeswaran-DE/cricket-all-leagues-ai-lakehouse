# Cricket All-Leagues AI Lakehouse

Portfolio Databricks project for processing 22K+ Cricsheet-style cricket JSON
files from:

```text
/Volumes/cricket/cricket_all/cricket_all_raw/extracted/*.json
```

The project uses Unity Catalog, Delta tables, medallion architecture, and
serverless Databricks jobs.

## Tables

Bronze:

- `cricket.cricket_all.bronze_raw_matches`

Silver:

- `cricket.cricket_all.silver_matches`
- `cricket.cricket_all.silver_deliveries`
- `cricket.cricket_all.silver_wickets`

Gold:

- `cricket.cricket_all.gold_player_batting_stats`
- `cricket.cricket_all.gold_bowler_stats`
- `cricket.cricket_all.gold_team_match_summary`
- `cricket.cricket_all.gold_league_season_summary`

## Run

Validate the bundle:

```powershell
databricks bundle validate -t dev --profile jagadeeswaran
```

Deploy:

```powershell
databricks bundle deploy -t dev --profile jagadeeswaran
```

Run:

```powershell
databricks bundle run cricket_all_leagues_lakehouse_job -t dev --profile jagadeeswaran
```

## Incremental ZIP Pipeline

Drop new ZIP files here by default:

```text
/Volumes/cricket/cricket_all/cricket_all_raw/zips/*.zip
```

Run the production incremental pipeline:

```powershell
databricks bundle run cricket_incremental_zip_pipeline_job -t dev --profile jagadeeswaran
```

Pipeline behavior:

- Extracts only ZIP files not already recorded in `pipeline_zip_manifest`.
- Writes newly extracted JSON files to
  `/Volumes/cricket/cricket_all/cricket_all_raw/extracted`.
- Ingests only JSON files extracted in the current run into
  `cricket.cricket_all.bronze_raw_matches`.
- Appends only unseen match IDs into all-data silver tables.
- Refreshes only affected season partitions in all-data gold tables.
- Moves matching IPL, Big Bash/WBBL, and The Hundred men's/women's matches into
  `cricket.cricinsights_src2`.
- Publishes focus-league and other-league AI serving tables in
  `cricket.cricinsights_src2`.
- Writes a detailed run summary to
  `cricket.cricket_all.pipeline_run_summaries`.

Detailed notifications are supported through SMTP email or Google Chat incoming
webhooks. Email uses:

- `summary_email_enabled`
- `summary_email_to`
- `summary_email_from`
- `smtp_host`
- `smtp_port`
- `smtp_user`
- `smtp_password_secret_scope`
- `smtp_password_secret_key`

Keep the SMTP password in a Databricks secret, not in the bundle file.

Google Chat uses:

- `google_chat_enabled`
- `google_chat_webhook_secret_scope`
- `google_chat_webhook_secret_key`

Keep the Google Chat webhook URL in a Databricks secret, not in the bundle file.

## AI Showcase Ideas

- League comparison assistant over Gold tables.
- Player similarity search using career and venue features.
- Natural-language SQL questions over curated cricket facts.
- Match summary generation from Silver delivery facts.

## Focused AI Showcase

The bundle also includes a focused job for IPL, Big Bash/WBBL, and The Hundred
men's/women's competitions. It writes to `cricket.cricinsights_src2`:

```powershell
databricks bundle run cricket_showcase_ai_lakehouse_job -t dev --profile jagadeeswaran
```

It reuses `cricket.cricket_all.bronze_raw_matches` and writes filtered
silver/gold tables optimized for chatbot use, including:

- `cricket.cricinsights_src2.gold_ai_player_cards`
- `cricket.cricinsights_src2.gold_ai_team_season_cards`
- `cricket.cricinsights_src2.gold_ai_match_facts`

Full documentation is in `docs/notion_cricket_showcase_lakehouse.md`.
