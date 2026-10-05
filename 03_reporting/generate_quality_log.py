# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 03 - Quality Log Export

Purpose
-------
Generate a reusable quality-issue log for the current pipeline run.
This script reads the data quality findings written by Stage 02 and exports
 them to a CSV file and a summary table for reporting and auditing.

Usage
-----
Run this notebook after Stage 02 data quality validation completes.
"""

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

from config.project_config import CATALOG_NAME, AUDIT_TABLES


QUALITY_ISSUES_TABLE = AUDIT_TABLES["data_quality_issues"]
PIPELINE_RUNS_TABLE = AUDIT_TABLES["pipeline_runs"]

# Use a fixed Volume location for exported logs
EXPORT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/audit"
LOG_PATH = f"{EXPORT_BASE_PATH}/quality_logs"

print(f"Quality issues table: {QUALITY_ISSUES_TABLE}")
print(f"Export base path    : {EXPORT_BASE_PATH}")
print(f"Log export path     : {LOG_PATH}")

# COMMAND ----------

# =========================
# 1. Read the quality findings
# =========================

quality_df = spark.table(QUALITY_ISSUES_TABLE)

print(f"Total rows in {QUALITY_ISSUES_TABLE}: {quality_df.count():,}")

# COMMAND ----------

# =========================
# 2. Build a summary table
# =========================

severity_summary = (
    quality_df
    .groupBy("severity")
    .count()
    .orderBy("severity")
)

issue_type_summary = (
    quality_df
    .groupBy("issue_type", "severity")
    .count()
    .orderBy(F.desc("count"))
)

print("=== Severity summary ===")
severity_summary.show(truncate=False)

print("=== Issue type summary ===")
issue_type_summary.show(100, truncate=False)

# COMMAND ----------

# =========================
# 3. Export all issues to CSV log file
# =========================

current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_log_dir = f"{LOG_PATH}/quality_issues_{current_time}"

(
    quality_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(csv_log_dir)
)

print(f"✓ CSV quality log exported to: {csv_log_dir}")

# COMMAND ----------

# =========================
# 4. Export a human-readable summary CSV
# =========================

summary_csv_path = f"{LOG_PATH}/quality_summary_{current_time}"

(
    quality_df
    .groupBy("dataset", "severity")
    .count()
    .orderBy("dataset", "severity")
    .withColumnRenamed("count", "issue_count")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(summary_csv_path)
)

print(f"✓ Summary export written to: {summary_csv_path}")

# COMMAND ----------

# =========================
# 5. Create a persistent reporting table
# =========================

report_table = f"{CATALOG_NAME}.audit.quality_issue_report"

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {report_table}
    (
        run_id STRING,
        dataset STRING,
        record_identifier STRING,
        issue_type STRING,
        severity STRING,
        description STRING,
        action_taken STRING,
        raw_record STRING,
        detected_at TIMESTAMP,
        exported_at TIMESTAMP
    )
    USING DELTA
    """
)

(
    quality_df
    .withColumn("exported_at", F.current_timestamp())
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(report_table)
)

print(f"✓ Quality issue report table created/updated: {report_table}")

# COMMAND ----------

# =========================
# 6. Print a concise final summary
# =========================

error_count = quality_df.filter(F.col("severity") == "ERROR").count()
warning_count = quality_df.filter(F.col("severity") == "WARNING").count()
info_count = quality_df.filter(F.col("severity") == "INFO").count()

print("\n========================================")
print("Quality issue log generation completed.")
print("========================================")
print(f"Total issues: {quality_df.count():,}")
print(f"ERROR count : {error_count:,}")
print(f"WARNING count: {warning_count:,}")
print(f"INFO count   : {info_count:,}")
print(f"CSV log path : {csv_log_dir}")
print(f"Summary path : {summary_csv_path}")
print(f"Report table : {report_table}")

# COMMAND ----------

# =========================
# 7. Optional: display the most important issues
# =========================

print("\n=== Important issues (ERROR and WARNING) ===")
(
    quality_df
    .filter(F.col("severity").isin(["ERROR", "WARNING"]))
    .select(
        "dataset",
        "record_identifier",
        "issue_type",
        "severity",
        "description",
        "action_taken",
        "detected_at",
    )
    .orderBy("dataset", "record_identifier", "issue_type")
    .show(100, truncate=False)
)

# COMMAND ----------
