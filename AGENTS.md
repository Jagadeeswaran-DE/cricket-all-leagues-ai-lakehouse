# Project Instructions

This repository builds a Databricks medallion lakehouse for all-league cricket
ball-by-ball JSON files stored in Unity Catalog volumes.

Use production-style PySpark source files, not notebook-only logic.

Rules:

- Use Unity Catalog three-level table names.
- Keep Bronze close to the raw JSON source.
- Build Silver tables as clean, queryable facts and dimensions.
- Build Gold tables as portfolio-ready analytics outputs.
- Do not hard-code credentials or tokens.
- Do not delete Databricks objects without explicit approval.
- Keep transformations idempotent and rerunnable.
- Prefer Spark SQL functions over Python UDFs.
- Preserve source file and match lineage.
- Add focused tests for non-Spark helper logic.

Before finishing code changes, run:

```powershell
pytest
databricks bundle validate -t dev --profile jagadeeswaran
```

