# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 03 - Silver Data Modeling

Purpose
-------
Transform Bronze source tables into typed, standardized Silver tables.

Responsibilities
----------------
1. Convert Bronze string fields into appropriate data types.
2. Standardize textual fields.
3. Normalize supplier phone numbers.
4. Standardize missing manager names.
5. Handle invalid product prices.
6. Remove duplicate sales business records.
7. Classify purchase orders as PENDING or DELIVERED.
8. Preserve useful source lineage metadata.
9. Write normalized Silver Delta tables.
10. Record the stage execution in the audit layer.

Important
---------
Bronze tables are never modified.

The actual data-generation script is the source of truth for
the source schema and source-data behavior.
"""

# COMMAND ----------

# =========================
# 1. Imports
# =========================

import sys
from pathlib import Path

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# 1a. Resolve project root
# ------------------------------------------------------------------
# Scripts live under scripts/<stage>/ while config/ and shared/ are
# at the project root.  Walk upward from this file's location to find
# the directory containing both config/project_config.py and
# shared/common.py, then add it to sys.path so the imports below work.
# ------------------------------------------------------------------

def _find_project_root() -> Path:
    here = (
        Path(__file__).resolve().parent
        if "__file__" in globals()
        else Path.cwd().resolve()
    )
    cwd = Path.cwd().resolve()
    candidates = [here, *here.parents, cwd, *cwd.parents]
    seen: set = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / "config" / "project_config.py").exists() and (
            candidate / "shared" / "common.py"
        ).exists():
            return candidate
    raise RuntimeError(
        "Could not locate project root containing "
        "config/project_config.py and shared/common.py. "
        "Run this file from the MegaMart-Capstone-1 Git folder."
    )


PROJECT_ROOT = _find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.project_config import (  # noqa: E402
    BRONZE_TABLES,
    SILVER_TABLES,
    AUDIT_TABLES,
)

from shared.common import (  # noqa: E402
    create_run_id,
)


# COMMAND ----------

# =========================
# 2. Pipeline Configuration
# =========================

PIPELINE_NAME = "megamart_pipeline"
STAGE_NAME = "03_model"

RUN_ID = create_run_id(STAGE_NAME)

START_TIME = datetime.now(timezone.utc)

QUALITY_ISSUES_TABLE = AUDIT_TABLES[
    "data_quality_issues"
]

PIPELINE_RUNS_TABLE = AUDIT_TABLES[
    "pipeline_runs"
]

print(f"Pipeline : {PIPELINE_NAME}")
print(f"Stage    : {STAGE_NAME}")
print(f"Run ID   : {RUN_ID}")


# COMMAND ----------

# =========================
# 3. Load Bronze Tables
# =========================

print("\n=== Loading Bronze Tables ===")

bronze = {}

for dataset_name, table_name in BRONZE_TABLES.items():

    bronze[dataset_name] = spark.table(
        table_name
    )

    count = bronze[dataset_name].count()

    print(
        f"✓ {dataset_name:22s} "
        f"{count:>6,} records"
    )


# COMMAND ----------

# =========================
# 4. Stores -> Silver
# =========================

print("\n=== Building silver.stores ===")

stores_raw = bronze["stores"]

stores_silver = (
    stores_raw
    .select(
        F.col("store_id")
        .cast("int")
        .alias("store_id"),

        F.trim(
            F.col("store_name")
        ).alias("store_name"),

        F.trim(
            F.col("location")
        ).alias("location"),

        F.when(
            F.col("manager_name").isNull()
            | (
                F.trim(
                    F.col("manager_name")
                ) == ""
            ),
            F.lit("UNKNOWN"),
        )
        .otherwise(
            F.trim(
                F.col("manager_name")
            )
        )
        .alias("manager_name"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
    .dropDuplicates(["store_id"])
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    stores_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["stores"]
    )
)

print(
    f"✓ Created {SILVER_TABLES['stores']} "
    f"with {stores_silver.count():,} records"
)


# COMMAND ----------

# =========================
# 5. Suppliers -> Silver
# =========================

print("\n=== Building silver.suppliers ===")

suppliers_raw = bronze["suppliers"]


# Normalize the supplier phone number using
# the shared utility from shared/common.py.
#
# The generator intentionally produces both:
#     +91-XXXXXXXXXX
#     91XXXXXXXXXX
#     XXXXXXXXXX
#
# The standardized Silver representation is 10 digits.

suppliers_silver = (
    suppliers_raw
    .select(
        F.col("supplier_id")
        .cast("int")
        .alias("supplier_id"),

        F.trim(
            F.col("supplier_name")
        ).alias("supplier_name"),

        F.when(
            F.col("contact_person").isNull()
            | (
                F.trim(
                    F.col("contact_person")
                ) == ""
            ),
            F.lit("UNKNOWN"),
        )
        .otherwise(
            F.trim(
                F.col("contact_person")
            )
        )
        .alias("contact_person"),

        F.regexp_replace(
            F.trim(F.col("phone")),
            r"\D",
            "",
        ).alias("phone"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
    .dropDuplicates(["supplier_id"])
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    suppliers_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["suppliers"]
    )
)

print(
    f"✓ Created {SILVER_TABLES['suppliers']} "
    f"with {suppliers_silver.count():,} records"
)


# COMMAND ----------

# =========================
# 6. Products -> Silver
# =========================

print("\n=== Building silver.products ===")

products_raw = bronze["products"]


products_silver = (
    products_raw
    .select(
        F.col("product_id")
        .cast("int")
        .alias("product_id"),

        F.trim(
            F.col("product_name")
        ).alias("product_name"),

        F.trim(
            F.col("category")
        ).alias("category"),

        F.when(
            F.col("unit_price").cast("double") > 0,
            F.col("unit_price")
            .cast("decimal(12,2)"),
        )
        .otherwise(
            F.lit(None).cast("decimal(12,2)")
        )
        .alias("unit_price"),

        F.col("supplier_id")
        .cast("int")
        .alias("supplier_id"),

        F.when(
            F.col("unit_price").cast("double") > 0,
            F.lit("VALID"),
        )
        .otherwise(
            F.lit("PRICE_QUARANTINED")
        )
        .alias("data_quality_status"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
    .dropDuplicates(["product_id"])
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    products_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["products"]
    )
)

print(
    f"✓ Created {SILVER_TABLES['products']} "
    f"with {products_silver.count():,} records"
)


# COMMAND ----------

# =========================
# 7. Sales Transactions -> Silver
# =========================

print("\n=== Building silver.sales_transactions ===")

sales_raw = bronze["sales_transactions"]


# Convert the source fields to their appropriate types.
sales_typed = (
    sales_raw
    .select(
        F.col("transaction_id")
        .cast("long")
        .alias("transaction_id"),

        F.col("store_id")
        .cast("int")
        .alias("store_id"),

        F.col("product_id")
        .cast("int")
        .alias("product_id"),

        F.col("quantity_sold")
        .cast("int")
        .alias("quantity_sold"),

        F.col("unit_price")
        .cast("decimal(12,2)")
        .alias("unit_price"),

        F.col("total_amount")
        .cast("decimal(14,2)")
        .alias("total_amount"),

        F.to_date(
            F.col("sale_date"),
            "yyyy-MM-dd",
        ).alias("sale_date"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
)


# ---------------------------------------------------------------------------
# Deduplicate sales using the same business-key logic used by Stage 02.
#
# transaction_id is deliberately NOT part of the duplicate definition.
#
# This handles cases where two records have:
#   same store
#   same product
#   same quantity
#   same unit price
#   same total amount
#   same sale date
#
# but different transaction IDs.
# ---------------------------------------------------------------------------

sales_duplicate_window = (
    Window
    .partitionBy(
        "store_id",
        "product_id",
        "quantity_sold",
        "unit_price",
        "total_amount",
        "sale_date",
    )
    .orderBy(
        F.col("transaction_id")
    )
)


sales_silver = (
    sales_typed
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(
            sales_duplicate_window
        ),
    )
    .filter(
        F.col("_duplicate_rank") == 1
    )
    .drop("_duplicate_rank")
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    sales_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["sales_transactions"]
    )
)

sales_silver_count = sales_silver.count()

print(
    f"✓ Created {SILVER_TABLES['sales_transactions']} "
    f"with {sales_silver_count:,} records"
)


# COMMAND ----------

# =========================
# 8. Inventory -> Silver
# =========================

print("\n=== Building silver.inventory ===")

inventory_raw = bronze["inventory"]


inventory_silver = (
    inventory_raw
    .select(
        F.col("inventory_id")
        .cast("long")
        .alias("inventory_id"),

        F.col("store_id")
        .cast("int")
        .alias("store_id"),

        F.col("product_id")
        .cast("int")
        .alias("product_id"),

        F.col("quantity_on_hand")
        .cast("int")
        .alias("quantity_on_hand"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
    .dropDuplicates(
        ["store_id", "product_id"]
    )
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    inventory_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["inventory"]
    )
)

print(
    f"✓ Created {SILVER_TABLES['inventory']} "
    f"with {inventory_silver.count():,} records"
)


# COMMAND ----------

# =========================
# 9. Purchase Orders -> Silver
# =========================

print("\n=== Building silver.purchase_orders ===")

purchase_orders_raw = bronze["purchase_orders"]


purchase_orders_silver = (
    purchase_orders_raw
    .select(
        F.col("po_id")
        .cast("long")
        .alias("po_id"),

        F.col("supplier_id")
        .cast("int")
        .alias("supplier_id"),

        F.to_date(
            F.col("order_date"),
            "yyyy-MM-dd",
        ).alias("order_date"),

        F.to_date(
            F.col("delivery_date"),
            "yyyy-MM-dd",
        ).alias("delivery_date"),

        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
    .withColumn(
        "po_status",
        F.when(
            F.col("delivery_date").isNull(),
            F.lit("PENDING"),
        )
        .otherwise(
            F.lit("DELIVERED")
        ),
    )
    .withColumn(
        "lead_time_days",
        F.when(
            F.col("delivery_date").isNotNull(),
            F.datediff(
                F.col("delivery_date"),
                F.col("order_date"),
            ),
        )
        .otherwise(
            F.lit(None).cast("int")
        ),
    )
    .dropDuplicates(["po_id"])
    .withColumn(
        "_silver_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )
)


(
    purchase_orders_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        SILVER_TABLES["purchase_orders"]
    )
)

print(
    f"✓ Created {SILVER_TABLES['purchase_orders']} "
    f"with {purchase_orders_silver.count():,} records"
)


# COMMAND ----------

# =========================
# 10. Silver Table Summary
# =========================

print("\n=== SILVER TABLE SUMMARY ===")

silver_counts = []

for dataset_name, table_name in SILVER_TABLES.items():

    count = spark.table(
        table_name
    ).count()

    silver_counts.append(
        (
            dataset_name,
            table_name,
            count,
        )
    )

for dataset_name, table_name, count in silver_counts:

    print(
        f"{dataset_name:22s} | "
        f"{count:>6,} | "
        f"{table_name}"
    )


# COMMAND ----------

# =========================
# 11. Expected Cleaning Impact
# =========================

bronze_sales_count = bronze[
    "sales_transactions"
].count()

print("\n=== CLEANING IMPACT ===")

print(
    f"Bronze sales records : "
    f"{bronze_sales_count:,}"
)

print(
    f"Silver sales records : "
    f"{sales_silver_count:,}"
)

print(
    f"Duplicate records removed: "
    f"{bronze_sales_count - sales_silver_count:,}"
)


# COMMAND ----------

# =========================
# 12. Product Quality Check
# =========================

print("\n=== PRODUCT QUALITY STATUS ===")

(
    spark.table(
        SILVER_TABLES["products"]
    )
    .groupBy(
        "data_quality_status"
    )
    .count()
    .orderBy(
        "data_quality_status"
    )
    .show(truncate=False)
)


# COMMAND ----------

# =========================
# 13. Purchase Order Status
# =========================

print("\n=== PURCHASE ORDER STATUS ===")

(
    spark.table(
        SILVER_TABLES["purchase_orders"]
    )
    .groupBy(
        "po_status"
    )
    .count()
    .orderBy(
        "po_status"
    )
    .show(truncate=False)
)


# COMMAND ----------

# =========================
# 14. Sample Silver Records
# =========================

print("\n=== SAMPLE SILVER RECORDS ===")

for dataset_name, table_name in SILVER_TABLES.items():

    print(
        f"\n--- {table_name} ---"
    )

    (
        spark.table(table_name)
        .limit(5)
        .show(truncate=False)
    )


# COMMAND ----------

# =========================
# 15. Audit Pipeline Run
# =========================

END_TIME = datetime.now(timezone.utc)

total_bronze_records = sum(
    bronze[dataset_name].count()
    for dataset_name in bronze
)

total_silver_records = sum(
    spark.table(table_name).count()
    for table_name in SILVER_TABLES.values()
)

records_removed = (
    total_bronze_records
    - total_silver_records
)

pipeline_run = (
    spark.range(1)
    .select(
        F.lit(RUN_ID)
        .cast("string")
        .alias("run_id"),

        F.lit(PIPELINE_NAME)
        .cast("string")
        .alias("pipeline_name"),

        F.lit(STAGE_NAME)
        .cast("string")
        .alias("stage_name"),

        F.lit("SUCCESS")
        .cast("string")
        .alias("status"),

        F.lit(START_TIME)
        .cast("timestamp")
        .alias("start_time"),

        F.lit(END_TIME)
        .cast("timestamp")
        .alias("end_time"),

        F.lit(total_bronze_records)
        .cast("long")
        .alias("records_processed"),

        F.lit(records_removed)
        .cast("long")
        .alias("records_failed"),

        F.lit(None)
        .cast("string")
        .alias("error_message"),
    )
)


(
    pipeline_run
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        PIPELINE_RUNS_TABLE
    )
)


# COMMAND ----------

# =========================
# 16. Final Status
# =========================

print("\n========================================")
print("Silver data modeling completed successfully.")
print("========================================")

print(f"Run ID              : {RUN_ID}")
print(f"Bronze records read  : {total_bronze_records:,}")
print(f"Silver records       : {total_silver_records:,}")
print(f"Records removed      : {records_removed:,}")

print("\nSilver tables:")

for table_name in SILVER_TABLES.values():
    print(f"✓ {table_name}")

print("\nBronze tables were not modified.")