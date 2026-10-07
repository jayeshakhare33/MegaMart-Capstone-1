# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 05 - Inventory Optimization

Purpose
-------
Calculate store/product reorder recommendations using cleaned Silver sales,
inventory, product, supplier, and supplier-performance data.

Responsibilities
----------------
1. Calculate daily demand at store/product level.
2. Calculate average daily demand and demand variability.
3. Estimate safety stock.
4. Calculate a demand-based reorder point.
5. Calculate recommended order quantity.
6. Assign replenishment priority.
7. Write the recommendations to the Gold layer.
8. Record the stage execution in the audit table.

Source-data constraints
-----------------------
The uploaded data-generation script is the source of truth.

The generated data provides:
- inventory by store/product
- sales quantity by store/product/date
- product -> supplier mapping
- supplier purchase-order delivery dates

The generated purchase orders do NOT contain:
- product_id
- quantity_ordered
- promised_delivery_date
- supplier SLA

Therefore this stage does not invent procurement quantities or an SLA-based
delivery calculation.

Reorder-point assumption
------------------------

This stage uses the following explicit planning assumption:

    Safety Stock =
        Z * Daily Demand Std Dev * sqrt(Lead Time)

    Reorder Point =
        Average Daily Demand * Average Lead Time
        + Safety Stock

    Recommended Order Quantity =
        max(Reorder Point - Current Stock, 0)

Z = 1.65 is used as a transparent planning assumption corresponding to an
approximately 95% one-sided service level.

This is a data-driven reorder recommendation. It is not a claim of a
mathematically optimal order quantity such as EOQ.
"""

# COMMAND ----------

# =========================
# 1. Imports
# =========================

import sys
from pathlib import Path

from datetime import datetime, timezone

from pyspark.sql import functions as F

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
    SILVER_TABLES,
    GOLD_TABLES,
    GOLD_VIEWS,
    AUDIT_TABLES,
)

from shared.common import create_run_id  # noqa: E402


# COMMAND ----------

# =========================
# 2. Configuration
# =========================

PIPELINE_NAME = "megamart_pipeline"
STAGE_NAME = "05_inventory_optimizer"

RUN_ID = create_run_id(STAGE_NAME)

START_TIME = datetime.now(timezone.utc)

PIPELINE_RUNS_TABLE = AUDIT_TABLES["pipeline_runs"]

# Explicit business/planning assumption.
SERVICE_LEVEL_Z = 1.65

print(f"Pipeline        : {PIPELINE_NAME}")
print(f"Stage           : {STAGE_NAME}")
print(f"Run ID          : {RUN_ID}")
print(f"Service-level Z : {SERVICE_LEVEL_Z}")


# COMMAND ----------

# =========================
# 3. Load Source Tables
# =========================

print("\n=== Loading Source Tables ===")

sales = spark.table(
    SILVER_TABLES["sales_transactions"]
)

inventory = spark.table(
    SILVER_TABLES["inventory"]
)

products = spark.table(
    SILVER_TABLES["products"]
)

stores = spark.table(
    SILVER_TABLES["stores"]
)

suppliers = spark.table(
    SILVER_TABLES["suppliers"]
)

supplier_scorecard = spark.table(
    GOLD_VIEWS["supplier_scorecard"]
)

print(
    f"✓ Sales records          : {sales.count():,}"
)

print(
    f"✓ Inventory records      : {inventory.count():,}"
)

print(
    f"✓ Product records        : {products.count():,}"
)

print(
    f"✓ Store records          : {stores.count():,}"
)

print(
    f"✓ Supplier records       : {suppliers.count():,}"
)

print(
    f"✓ Supplier scorecard     : {supplier_scorecard.count():,}"
)


# COMMAND ----------

# =========================
# 4. Determine Actual Sales Period
# =========================
#
# We derive the demand-calculation period from the actual Silver sales data
# rather than blindly assuming the PRD calendar dates.
#
# This respects the generator as the source of truth.
# =========================

print("\n=== Determining Actual Sales Period ===")

sales_period = (
    sales
    .agg(
        F.min("sale_date").alias(
            "sales_start_date"
        ),
        F.max("sale_date").alias(
            "sales_end_date"
        ),
    )
    .first()
)

sales_start_date = sales_period[
    "sales_start_date"
]

sales_end_date = sales_period[
    "sales_end_date"
]

if sales_start_date is None or sales_end_date is None:
    raise ValueError(
        "Unable to determine sales period from Silver sales data."
    )

project_days = (
    sales_end_date - sales_start_date
).days + 1

print(
    f"Sales period : {sales_start_date} -> {sales_end_date}"
)

print(
    f"Calendar days: {project_days}"
)


# COMMAND ----------

# =========================
# 5. Build Daily Demand
# =========================

print("\n=== Building Daily Demand ===")

daily_sales = (
    sales
    .groupBy(
        "store_id",
        "product_id",
        "sale_date",
    )
    .agg(
        F.sum(
            "quantity_sold"
        )
        .cast("double")
        .alias(
            "daily_quantity_sold"
        )
    )
)

print(
    f"✓ Store/product/day combinations: "
    f"{daily_sales.count():,}"
)


# COMMAND ----------

# =========================
# 6. Create Complete Daily Calendar
# =========================
#
# Inventory contains the full store/product universe.
# We create every calendar day in the actual generated sales period.
#
# A missing sales record on a day is treated as zero demand.
# This prevents average demand from being biased toward only days with sales.
# =========================

date_range = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(sales_start_date).cast("date"),
                F.lit(sales_end_date).cast("date"),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("sale_date")
    )
)

store_product = (
    inventory
    .select(
        "store_id",
        "product_id",
    )
    .dropDuplicates(
        [
            "store_id",
            "product_id",
        ]
    )
)

store_product_days = (
    store_product
    .crossJoin(date_range)
)

daily_demand = (
    store_product_days
    .join(
        daily_sales,
        on=[
            "store_id",
            "product_id",
            "sale_date",
        ],
        how="left",
    )
    .withColumn(
        "daily_quantity_sold",
        F.coalesce(
            F.col("daily_quantity_sold"),
            F.lit(0.0),
        ),
    )
)

print(
    f"✓ Daily store/product calendar rows: "
    f"{daily_demand.count():,}"
)


# COMMAND ----------

# =========================
# 7. Calculate Demand Metrics
# =========================

print("\n=== Calculating Demand Metrics ===")

demand_metrics = (
    daily_demand
    .groupBy(
        "store_id",
        "product_id",
    )
    .agg(
        F.avg(
            "daily_quantity_sold"
        ).alias(
            "average_daily_demand"
        ),

        F.stddev_samp(
            "daily_quantity_sold"
        ).alias(
            "daily_demand_stddev"
        ),

        F.sum(
            "daily_quantity_sold"
        ).alias(
            "period_units_sold"
        ),

        F.count(
            F.when(
                F.col("daily_quantity_sold") > 0,
                True,
            )
        ).alias(
            "days_with_sales"
        ),
    )
    .withColumn(
        "daily_demand_stddev",
        F.coalesce(
            F.col("daily_demand_stddev"),
            F.lit(0.0),
        ),
    )
)

print(
    f"✓ Demand metrics created for "
    f"{demand_metrics.count():,} store/product combinations"
)


# COMMAND ----------

# =========================
# 8. Determine Overall Lead-Time Fallback
# =========================
#
# Normally each product inherits the average lead time of its supplier.
#
# If a supplier has no delivered orders, its supplier-level average lead time
# is NULL. In that unusual case we use the overall average delivered
# supplier lead time as a transparent fallback.
# =========================

overall_average_lead_time = (
    supplier_scorecard
    .filter(
        F.col("average_lead_time_days").isNotNull()
    )
    .agg(
        F.avg(
            "average_lead_time_days"
        ).alias(
            "overall_average_lead_time"
        )
    )
    .first()["overall_average_lead_time"]
)

if overall_average_lead_time is None:
    overall_average_lead_time = 0.0

print(
    f"Overall fallback lead time: "
    f"{float(overall_average_lead_time):.2f} days"
)


# COMMAND ----------

# =========================
# 9. Join Inventory + Demand + Product + Supplier
# =========================

print("\n=== Combining Optimization Inputs ===")

optimization_base = (
    inventory
    .select(
        "inventory_id",
        "store_id",
        "product_id",
        "quantity_on_hand",
    )
    .join(
        demand_metrics,
        on=[
            "store_id",
            "product_id",
        ],
        how="left",
    )
    .join(
        products
        .select(
            "product_id",
            "product_name",
            "category",
            "supplier_id",
        ),
        on="product_id",
        how="left",
    )
    .join(
        stores
        .select(
            "store_id",
            "store_name",
            "location",
        ),
        on="store_id",
        how="left",
    )
    .join(
        suppliers
        .select(
            "supplier_id",
            "supplier_name",
        ),
        on="supplier_id",
        how="left",
    )
    .join(
        supplier_scorecard
        .select(
            "supplier_id",
            "average_lead_time_days",
            "delivery_reliability_pct",
        ),
        on="supplier_id",
        how="left",
    )
    .withColumn(
        "average_daily_demand",
        F.coalesce(
            F.col("average_daily_demand"),
            F.lit(0.0),
        ),
    )
    .withColumn(
        "daily_demand_stddev",
        F.coalesce(
            F.col("daily_demand_stddev"),
            F.lit(0.0),
        ),
    )
    .withColumn(
        "period_units_sold",
        F.coalesce(
            F.col("period_units_sold"),
            F.lit(0.0),
        ),
    )
    .withColumn(
        "days_with_sales",
        F.coalesce(
            F.col("days_with_sales"),
            F.lit(0),
        ),
    )
    .withColumn(
        "planning_lead_time_days",
        F.coalesce(
            F.col("average_lead_time_days"),
            F.lit(float(overall_average_lead_time)),
        ),
    )
)


# COMMAND ----------

# =========================
# 10. Calculate Safety Stock
# =========================

print("\n=== Calculating Safety Stock ===")

optimization_base = (
    optimization_base
    .withColumn(
        "safety_stock",
        F.ceil(
            F.lit(SERVICE_LEVEL_Z)
            * F.col("daily_demand_stddev")
            * F.sqrt(
                F.greatest(
                    F.col("planning_lead_time_days"),
                    F.lit(0.0),
                )
            )
        ).cast("int")
    )
)


# COMMAND ----------

# =========================
# 11. Calculate Reorder Point
# =========================

print("\n=== Calculating Reorder Point ===")

optimization_base = (
    optimization_base
    .withColumn(
        "reorder_point",
        F.ceil(
            (
                F.col("average_daily_demand")
                * F.col("planning_lead_time_days")
            )
            + F.col("safety_stock")
        )
    )
    .withColumn(
        "reorder_point",
        F.col("reorder_point").cast("long")
    )
)


# COMMAND ----------

# =========================
# 12. Calculate Recommended Order Quantity
# =========================

optimization_base = (
    optimization_base
    .withColumn(
        "recommended_order_quantity",
        F.greatest(
            F.col("reorder_point")
            - F.col("quantity_on_hand"),
            F.lit(0),
        ).cast("long"),
    )
)


# COMMAND ----------

# =========================
# 13. Assign Replenishment Priority
# =========================

optimization_base = (
    optimization_base
    .withColumn(
        "replenishment_priority",
        F.when(
            F.col("quantity_on_hand") <= 0,
            F.lit("CRITICAL"),
        )
        .when(
            F.col("quantity_on_hand")
            <= F.col("reorder_point"),
            F.lit("HIGH"),
        )
        .when(
            F.col("quantity_on_hand")
            <= (
                F.col("reorder_point") * 1.25
            ),
            F.lit("MEDIUM"),
        )
        .otherwise(
            F.lit("LOW"),
        ),
    )
)


# COMMAND ----------

# =========================
# 14. Create Recommendation Status
# =========================

optimization_base = (
    optimization_base
    .withColumn(
        "recommendation_status",
        F.when(
            F.col("recommended_order_quantity") > 0,
            F.lit("REORDER"),
        )
        .otherwise(
            F.lit("NO_REORDER"),
        ),
    )
)


# COMMAND ----------

# =========================
# 15. Build Final Recommendation Table
# =========================

print("\n=== Building Reorder Recommendations ===")

GOLD_REORDER_TABLE = GOLD_TABLES[
    "reorder_recommendations"
]

reorder_recommendations = (
    optimization_base
    .select(
        "inventory_id",

        "store_id",
        "store_name",
        "location",

        "product_id",
        "product_name",
        "category",

        "supplier_id",
        "supplier_name",

        "quantity_on_hand",

        "period_units_sold",

        F.round(
            "average_daily_demand",
            4,
        ).alias(
            "average_daily_demand"
        ),

        "days_with_sales",

        F.round(
            "daily_demand_stddev",
            4,
        ).alias(
            "daily_demand_stddev"
        ),

        F.round(
            "planning_lead_time_days",
            2,
        ).alias(
            "planning_lead_time_days"
        ),

        "safety_stock",

        "reorder_point",

        "recommended_order_quantity",

        F.round(
            "delivery_reliability_pct",
            2,
        ).alias(
            "supplier_delivery_reliability_pct"
        ),

        "replenishment_priority",

        "recommendation_status",

        F.lit(
            SERVICE_LEVEL_Z
        ).alias(
            "service_level_z"
        ),

        F.lit(
            "DEMAND_BASED_REORDER_POINT"
        ).alias(
            "reorder_method"
        ),

        F.lit(
            "Average daily demand x supplier lead time + safety stock"
        ).alias(
            "reorder_point_definition"
        ),

        F.lit(
            RUN_ID
        ).alias(
            "_optimizer_run_id"
        ),

        F.current_timestamp().alias(
            "_optimizer_processed_at"
        ),
    )
)


# COMMAND ----------

# =========================
# 16. Write Gold Recommendations
# =========================

(
    reorder_recommendations
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true",
    )
    .saveAsTable(
        GOLD_REORDER_TABLE
    )
)

print(
    f"✓ Created {GOLD_REORDER_TABLE}"
)


# COMMAND ----------

# =========================
# 17. Recommendation Summary
# =========================

print("\n=== RECOMMENDATION SUMMARY ===")

(
    reorder_recommendations
    .groupBy(
        "recommendation_status"
    )
    .count()
    .orderBy(
        "recommendation_status"
    )
    .show(truncate=False)
)


# COMMAND ----------

# =========================
# 18. Priority Summary
# =========================

print("\n=== PRIORITY SUMMARY ===")

(
    reorder_recommendations
    .groupBy(
        "replenishment_priority"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(truncate=False)
)


# COMMAND ----------

# =========================
# 19. Reorder Recommendations
# =========================

print(
    "\n=== REORDER RECOMMENDATIONS ==="
)

(
    reorder_recommendations
    .filter(
        F.col("recommendation_status") == "REORDER"
    )
    .select(
        "store_id",
        "store_name",
        "product_id",
        "product_name",
        "quantity_on_hand",
        "average_daily_demand",
        "planning_lead_time_days",
        "safety_stock",
        "reorder_point",
        "recommended_order_quantity",
        "supplier_delivery_reliability_pct",
        "replenishment_priority",
    )
    .orderBy(
        F.when(
            F.col("replenishment_priority")
            == "CRITICAL",
            1,
        )
        .when(
            F.col("replenishment_priority")
            == "HIGH",
            2,
        )
        .when(
            F.col("replenishment_priority")
            == "MEDIUM",
            3,
        )
        .otherwise(4),
        F.desc(
            "recommended_order_quantity"
        ),
        "store_id",
        "product_id",
    )
    .show(
        100,
        truncate=False,
    )
)


# COMMAND ----------

# =========================
# 20. Fastest-Moving Items
# =========================

print(
    "\n=== FASTEST-MOVING STORE/PRODUCT COMBINATIONS ==="
)

(
    reorder_recommendations
    .select(
        "store_id",
        "store_name",
        "product_id",
        "product_name",
        "period_units_sold",
        "average_daily_demand",
        "quantity_on_hand",
        "reorder_point",
        "recommendation_status",
    )
    .orderBy(
        F.desc("period_units_sold"),
        "store_id",
        "product_id",
    )
    .show(
        20,
        truncate=False,
    )
)


# COMMAND ----------

# =========================
# 21. Output Verification
# =========================

print("\n=== OUTPUT VERIFICATION ===")

final_count = spark.table(
    GOLD_REORDER_TABLE
).count()

print(
    f"Reorder recommendation rows: "
    f"{final_count:,}"
)

print(
    "Expected inventory universe: "
    "5 stores x 14 products = 70 combinations"
)


# COMMAND ----------

# =========================
# 22. Audit Pipeline Run
# =========================

END_TIME = datetime.now(timezone.utc)

records_processed = inventory.count()

records_recommended = (
    reorder_recommendations
    .filter(
        F.col("recommendation_status") == "REORDER"
    )
    .count()
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

        F.lit(records_processed)
        .cast("long")
        .alias("records_processed"),

        F.lit(0)
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
# 23. Final Status
# =========================

print("\n========================================")
print("Inventory optimization completed successfully.")
print("========================================")

print(f"Run ID                  : {RUN_ID}")
print(f"Inventory combinations  : {records_processed:,}")
print(f"Reorder recommendations : {records_recommended:,}")
print(f"Output table             : {GOLD_REORDER_TABLE}")

print(
    "\nBronze and Silver tables were not modified."
)
