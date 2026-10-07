# MegaMart Supermarket Chain — Data Engineering Capstone

End-to-end ETL pipeline on Databricks using a **Bronze → Silver → Gold** medallion architecture with Unity Catalog. The project ingests 6 CSV source files for a 5-store Indian supermarket chain (Jan–Dec 2024), validates data quality, transforms them into typed Silver tables, and builds Gold-layer analytical views, a reorder-recommendations table, report artifacts (CSV/HTML/PDF), and a Databricks AI/BI dashboard.

## Prerequisites

* **Databricks workspace** with Unity Catalog enabled
* **Catalog creation permission** — the pipeline creates catalog `megamart_dev` (ask your admin if you lack `CREATE CATALOG`)
* **Python-capable compute** — Serverless or a Standard cluster with DBR 13.3+ (SQL-only warehouses cannot run the Python stages)
* Pre-installed libraries only: `pyspark`, `pandas`, `matplotlib` — no custom installs needed

## Project Structure

```
MegaMart-Capstone-1/
├── config/project_config.py          Central configuration (catalog, tables, thresholds)
├── shared/common.py                  Reusable Python utilities (phone normalization, hashing, etc.)
├── data/
│   ├── data_generation/generate_sample_data.py   Generates 6 CSVs with intentional DQ issues
│   └── raw_files/                     6 source CSVs (stores, products, suppliers, sales, inventory, POs)
├── scripts/
│   ├── 00_setup/00_setup.py            Create catalog, schemas, volume
│   ├── 01_ingestion/01_ingest_raw.py   CSV → Bronze Delta tables
│   ├── 02_quality/02_validate_clean     DQ validation notebook (audit logging)
│   ├── 03_model/03_build_silver.py     Bronze → typed, cleaned Silver tables
│   ├── 03B_generate_views/gold_layer_views.sql   10 Gold views from Silver
│   ├── 04_sales/04_sales_report.py     Sales report → CSV + HTML
│   ├── 05_inventory/05_inventory_optimizer.py     Reorder recommendations → Gold table
│   ├── 06_kpi/06_kpi_pipeline.py       KPI artifacts → CSV + PDF
│   ├── 06_kpi/06_kpi_dashboard_report.py   Dashboard HTML report
│   └── 07_dashboard/07_dashboard.py   Dashboard data prep + build guide
├── resources/megamart_pipeline.yml    Declarative Automation Bundle job definition (11 tasks, serverless)
├── verify_pipeline.py                Post-pipeline verification (36 checks)
├── documentation/                    Problem-statement deliverables (PS01–PS05)
└── outputs/                          Exported dashboard PDFs
```

## Pipeline Execution Order

Run each stage **in order** from a Databricks notebook or job task. Scripts auto-resolve the project root via `_find_project_root()`, so they work from any Git-backed folder context.

| Step | Script | What it does |
| --- | --- | --- |
| 1 | `scripts/00_setup/00_setup.py` | Creates catalog `megamart_dev`, schemas (bronze, silver, gold, audit), and volume `bronze.raw_files` |
| 2 | — | **Manual**: Upload the 6 CSVs from `data/raw_files/` to `/Volumes/megamart_dev/bronze/raw_files/` (see notes below) |
| 3 | `scripts/01_ingestion/01_ingest_raw.py` | Reads CSVs, adds lineage metadata, writes 6 Bronze Delta tables |
| 4 | `scripts/02_quality/02_validate_clean` (notebook) | Validates types, duplicates, referential integrity; logs issues to `audit.data_quality_issues` |
| 5 | `scripts/03_model/03_build_silver.py` | Type-casts, standardizes, deduplicates, normalizes phones; writes 6 Silver tables |
| 6 | `scripts/03B_generate_views/gold_layer_views.sql` | Creates 10 Gold views (store_performance, enterprise_kpi, etc.) |
| 7 | `scripts/05_inventory/05_inventory_optimizer.py` | Calculates demand, safety stock, reorder points; writes `gold.reorder_recommendations` |
| 8 | `scripts/04_sales/04_sales_report.py` | Exports CSVs and a self-contained HTML sales report |
| 9 | `scripts/06_kpi/06_kpi_pipeline.py` | Publishes KPI CSV and PDF artifacts |
| 10 | `scripts/06_kpi/06_kpi_dashboard_report.py` | Generates an HTML report mirroring all 15 dashboard widgets |
| 11 | `scripts/07_dashboard/07_dashboard.py` | Prepares dashboard-ready data and a build guide |
| 12 | `verify_pipeline.py` | Runs 36 checks across all layers — PASS/FAIL summary |

## Configuration

All project settings are centralized in `config/project_config.py`. Every pipeline script imports from it — no hard-coded paths or table names in the stage scripts. Key values:

* `CATALOG_NAME = "megamart_dev"`, schemas: `bronze`, `silver`, `gold`, `audit`
* 6 source datasets: stores, products, suppliers, sales_transactions, inventory, purchase_orders
* 10 Gold views + 1 Gold table (`reorder_recommendations`)
* Data period: 2024-01-01 to 2024-12-31

## Dataset Overview

5 stores across Hyderabad, 14 products in 7 categories, 5 suppliers, ~2,000 sales transactions, 70 inventory snapshots, and 300 purchase orders — all for calendar year 2024.

## Automated Job

The pipeline is automated as a single Databricks Lakeflow Job with 11 sequential tasks (steps 1, 3–12 above). The job definition is in [`resources/megamart_pipeline.yml`](resources/megamart_pipeline.yml) (a Declarative Automation Bundle resource file). See [JOB_GUIDE.md](JOB_GUIDE.md) for detailed instructions on how to run, monitor, and troubleshoot the job.

### Notes on automation scope

* **CSV upload is manual** — the 6 source CSVs must be uploaded to `/Volumes/megamart_dev/bronze/raw_files/` before triggering the job. This is a one-time prerequisite, not part of the automated job.
* **Static dataset** — the pipeline performs a full refresh (overwrite) on every run. The source CSVs are generated once by `generate_sample_data.py` and do not change between runs.
* **Report artifacts** — the `gold.report_artifacts` volume is created automatically by `04_sales_report.py` if it does not already exist. No manual setup is needed.
* **Error handling** — the job uses sequential task dependencies; if any stage fails, downstream tasks are skipped and the failure is surfaced in the job run UI. No retry logic or complex alerting is configured.

## Verification

After the full pipeline completes, `verify_pipeline.py` (the final job task) confirms:
* Catalog, schemas, and volume exist
* 6 Bronze tables (MANAGED) with data
* 6 Silver tables (MANAGED) with correct column types
* 10 Gold views (VIEW) and 1 Gold table with data
* 2 Audit tables with data
* No leftover SDP artifacts, no duplicate transaction IDs