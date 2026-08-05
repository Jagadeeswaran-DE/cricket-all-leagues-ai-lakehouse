# Cleanup And Migration Report

The repository contains one job definition: `cricket_incremental_zip_pipeline_job`.
Retired full-load, showcase-only, and smoke job definitions were removed from
the bundle. Existing Delta tables are preserved; this extension does not drop
production data automatically.

## Safe migration order

1. Deploy with `dry_run=true` and inspect source manifests.
2. Run `bootstrap` only for an empty environment or an approved full refresh.
3. Run incremental mode and inspect DQ/audit results.
4. Keep the production daily schedule enabled after the first successful run.
5. Retire obsolete tables only after dependency checks and sign-off.

## Commented cleanup SQL

These statements are intentionally comments. Execute them only after checking
downstream notebooks, dashboards, SQL alerts, and AI endpoints.

```sql
-- SHOW TABLES IN cricket.cricket_all;
-- SHOW TABLES IN cricket.cricinsights_src2;
-- DESCRIBE DETAIL cricket.cricket_all.<candidate_table>;
-- DROP TABLE IF EXISTS cricket.cricket_all.<candidate_table>;
-- DROP TABLE IF EXISTS cricket.cricinsights_src2.<candidate_table>;
```
