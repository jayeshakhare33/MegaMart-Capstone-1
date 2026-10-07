# MegaMart Pipeline — Job Run Guide

This guide explains how to run, monitor, and troubleshoot the automated MegaMart pipeline job.

**Job name:** MegaMart Pipeline
**Job ID:** 200712033858379
**Trigger:** Manual (no schedule)
**Compute:** Serverless (all tasks)

## Prerequisites

Before running the job, ensure the following one-time setup is complete:

1. The 6 source CSVs are uploaded to `/Volumes/megamart_dev/bronze/raw_files/`:
   * `stores.csv`, `products.csv`, `suppliers.csv`, `sales_transactions.csv`, `inventory.csv`, `purchase_orders.csv`
2. You have permission to create the `megamart_dev` catalog (or it already exists).
3. The Git folder `MegaMart-Capstone-1` is cloned in your Databricks workspace.

> The CSV upload is **manual** and not part of the automated job. It only needs to be done once unless the source files change.

## Task Sequence

The job runs 11 tasks sequentially. If any task fails, all downstream tasks are skipped.

| # | Task name | Type | Source file | Depends on | What it does |
| --- | --- | --- | --- | --- | --- |
| 1 | `00_setup` | Python script | `scripts/00_setup/00_setup.py` | — | Creates catalog `megamart_dev`, schemas (bronze, silver, gold, audit), and volume `bronze.raw_files` |
| 2 | `01_ingestion` | Python script | `scripts/01_ingestion/01_ingest_raw.py` | `00_setup` | Reads CSVs from the volume, adds lineage metadata, writes 6 Bronze Delta tables |
| 3 | `02_quality` | Notebook | `scripts/02_quality/02_validate_clean` | `01_ingestion` | Validates types, duplicates, referential integrity; logs issues to `audit.data_quality_issues` |
| 4 | `03_build_silver` | Python script | `scripts/03_model/03_build_silver.py` | `02_quality` | Type-casts, standardizes, deduplicates, normalizes phones; writes 6 Silver tables |
| 5 | `03B_gold_views` | SQL file | `scripts/03B_generate_views/gold_layer_views.sql` | `03_build_silver` | Creates 10 Gold views from Silver (store_performance, enterprise_kpi, etc.) |
| 6 | `05_inventory` | Python script | `scripts/05_inventory/05_inventory_optimizer.py` | `03B_gold_views` | Calculates demand, safety stock, reorder points; writes `gold.reorder_recommendations` |
| 7 | `04_sales_report` | Python script | `scripts/04_sales/04_sales_report.py` | `05_inventory` | Exports CSVs and a self-contained HTML sales report to the `gold.report_artifacts` volume |
| 8 | `06_kpi_pipeline` | Python script | `scripts/06_kpi/06_kpi_pipeline.py` | `04_sales_report` | Publishes KPI CSV and PDF artifacts |
| 9 | `06_kpi_report` | Python script | `scripts/06_kpi/06_kpi_dashboard_report.py` | `06_kpi_pipeline` | Generates an HTML report mirroring all 15 dashboard widgets |
| 10 | `07_dashboard` | Python script | `scripts/07_dashboard/07_dashboard.py` | `06_kpi_report` | Prepares dashboard-ready data and a build guide |
| 11 | `verify` | Python script | `verify_pipeline.py` | `07_dashboard` | Runs 36 checks across all layers — prints PASS/FAIL summary |

## How to Run

1. Navigate to **Jobs** in the Databricks sidebar.
2. Find and open **MegaMart Pipeline**.
3. Click **Run now** (the play button) in the top-right corner.
4. A new run starts immediately. Click the run link to view live progress.

## Monitoring

* **Run view** — each task shows a status badge: Running, Succeeded, Failed, or Skipped.
* **Task logs** — click any task to see stdout/stderr and error traces.
* **Audit table** — query `megamart_dev.audit.pipeline_runs` to see per-stage execution records with run IDs and timestamps.
* **Verify task** — the final task prints a PASS/FAIL summary with 36 checks. If all pass, the pipeline is healthy.

## If a Task Fails

1. Open the failed task and read the error in the task logs.
2. The most common failure causes are:
   * **Missing CSVs** — the 6 source files haven't been uploaded to the volume. Upload them and re-run.
   * **Permission error** — your user lacks `CREATE CATALOG` or write access to `megamart_dev`.
   * **Import error** — the project root can't be resolved. Ensure the Git folder `MegaMart-Capstone-1` is cloned in the workspace.
3. After fixing the issue, click **Run now** again. The job is idempotent — all tables use `overwrite` or `CREATE OR REPLACE`, so re-runs are safe.

## Post-Run Outputs

After a successful run, the following artifacts are available:

| Artifact | Location |
| --- | --- |
| Bronze tables (6) | `megamart_dev.bronze.*` |
| Silver tables (6) | `megamart_dev.silver.*` |
| Gold views (10) | `megamart_dev.gold.*` |
| Reorder recommendations | `megamart_dev.gold.reorder_recommendations` |
| Sales report (CSV + HTML) | `/Volumes/megamart_dev/gold/report_artifacts/sales/<run_id>/` |
| KPI report (CSV + PDF) | `/Volumes/megamart_dev/gold/report_artifacts/kpi/<run_id>/` |
| Dashboard data + build guide | `/Volumes/megamart_dev/gold/report_artifacts/dashboard/<run_id>/` |
| Audit logs | `megamart_dev.audit.pipeline_runs`, `megamart_dev.audit.data_quality_issues` |