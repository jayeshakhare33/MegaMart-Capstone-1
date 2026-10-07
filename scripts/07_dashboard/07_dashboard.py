# Databricks source file
# MegaMart Capstone - Stage 07: Dashboard Data Preparation
#
# Purpose:
#   Prepare a stable, dashboard-ready semantic layer from the Gold KPI tables
#   and the Silver inventory snapshot.
#
# Architecture:
#   Silver/Gold -> Dashboard Gold tables -> Databricks AI/BI Dashboard
#
# This stage does NOT create the AI/BI Dashboard itself. The dashboard is a
# Databricks UI object built after these dashboard-ready tables are validated.
# This file also does NOT create infrastructure.

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession, types as T


# -----------------------------------------------------------------------------
# 1. Resolve project root for Git-backed Databricks execution
# -----------------------------------------------------------------------------

def _find_project_root() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    cwd = Path.cwd().resolve()

    candidates = [here, *here.parents, cwd, *cwd.parents]
    seen = set()

    for candidate in candidates:
        if candidate in seen:
            continue

        seen.add(candidate)

        if (
            (candidate / "config" / "project_config.py").exists()
            and (candidate / "shared" / "common.py").exists()
        ):
            return candidate

    raise RuntimeError(
        "Could not locate the MegaMart project root containing "
        "config/project_config.py and shared/common.py."
    )


PROJECT_ROOT = _find_project_root()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.project_config import (  # noqa: E402
    AUDIT_TABLES,
    CATALOG_NAME,
    GOLD_VIEWS,
    PROJECT_END_DATE,
    PROJECT_START_DATE,
)
from shared.common import create_run_id, get_utc_timestamp  # noqa: E402


spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


# -----------------------------------------------------------------------------
# 2. Stage configuration
# -----------------------------------------------------------------------------

STAGE_NAME = "07_dashboard"
PIPELINE_NAME = "megamart_pipeline"

RUN_ID = create_run_id(STAGE_NAME)
RUN_TIMESTAMP = get_utc_timestamp()

GOLD_SCHEMA = f"{CATALOG_NAME}.gold"
AUDIT_TABLE = AUDIT_TABLES["pipeline_runs"]

REPORT_BASE = f"/Volumes/{CATALOG_NAME}/gold/report_artifacts/dashboard"
RUN_OUTPUT_PATH = f"{REPORT_BASE}/{RUN_ID}"


# The gold layer is now views. The consolidated views already include
# all dashboard columns (inventory_health_pct, revenue_share_pct,
# month_label, dashboard_period) so no separate dashboard tables
# are needed.
SOURCE_VIEWS = {
    "store_kpi": GOLD_VIEWS["store_performance"],
    "category_kpi": GOLD_VIEWS["category_kpis"],
    "monthly_kpi": GOLD_VIEWS["monthly_kpis"],
    "enterprise_kpi": GOLD_VIEWS["enterprise_kpi"],
    "store_category_top3": GOLD_VIEWS["store_category_top3"],
}


# -----------------------------------------------------------------------------
# 3. Helpers
# -----------------------------------------------------------------------------


def write_single_csv(df: DataFrame, file_name: str) -> None:
    output_path = Path(f"{RUN_OUTPUT_PATH}/{file_name}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(output_path, index=False)


def write_text_artifact(file_name: str, content: str) -> None:
    output_path = Path(f"{RUN_OUTPUT_PATH}/{file_name}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")


def write_pipeline_audit(
    status: str,
    records_processed: int,
    message: str = "",
) -> None:
    """
    Append to the existing audit table without assuming a fixed schema.
    """

    existing_schema = spark.table(AUDIT_TABLE).schema
    now = get_utc_timestamp()

    values = {}

    for field in existing_schema.fields:
        name = field.name.lower()

        if name in {"run_id", "pipeline_run_id"}:
            value = RUN_ID

        elif name in {"pipeline_name", "job_name"}:
            value = PIPELINE_NAME

        elif name in {"stage_name", "stage", "step_name"}:
            value = STAGE_NAME

        elif name in {"status", "run_status"}:
            value = status

        elif name in {"started_at", "start_time", "run_started_at"}:
            value = RUN_TIMESTAMP

        elif name in {
            "ended_at",
            "end_time",
            "completed_at",
            "run_ended_at",
        }:
            value = now

        elif name in {
            "records_processed",
            "record_count",
            "row_count",
            "rows_processed",
        }:
            value = int(records_processed)

        elif name in {"message", "error_message", "details"}:
            value = message

        else:
            data_type = field.dataType

            if isinstance(data_type, T.StringType):
                value = ""

            elif isinstance(
                data_type,
                (
                    T.IntegerType,
                    T.LongType,
                    T.ShortType,
                    T.ByteType,
                    T.DoubleType,
                    T.FloatType,
                    T.DecimalType,
                ),
            ):
                value = 0

            elif isinstance(data_type, T.BooleanType):
                value = False

            else:
                value = None

        values[field.name] = value

    audit_df = spark.createDataFrame(
        [values],
        schema=existing_schema,
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )
    

# -----------------------------------------------------------------------------
# 7. Dashboard build guide
# -----------------------------------------------------------------------------

def build_dashboard_guide(store_df: DataFrame) -> str:
    return f"""# MegaMart Dashboard Build Guide

Run ID: {RUN_ID}
Project period: {PROJECT_START_DATE} to {PROJECT_END_DATE}

## Dashboard data sources

Use these Gold tables as the data sources for the Databricks AI/BI Dashboard:

1. {SOURCE_VIEWS["enterprise_kpi"]}
2. {SOURCE_VIEWS["store_kpi"]}
3. {SOURCE_VIEWS["category_kpi"]}
4. {SOURCE_VIEWS["monthly_kpi"]}
5. {SOURCE_VIEWS["store_category_top3"]}

## Suggested dashboard layout

### KPI cards
Use the enterprise summary for:

- Total Revenue
- Transaction Count
- Average Transaction Value
- Unique Products Sold
- Current Inventory Units
- Out-of-Stock SKUs
- Low-Stock SKUs

### Store comparison

Use {SOURCE_VIEWS["store_kpi"]}.

Recommended fields:

- store_name
- total_revenue
- transaction_count
- avg_transaction_value
- unique_products_sold
- store_revenue_share_pct
- current_inventory_units
- out_of_stock_skus
- low_stock_skus
- healthy_stock_skus
- inventory_health_pct

### Monthly trend

Use {SOURCE_VIEWS["monthly_kpi"]}.

Recommended fields:

- month_start
- month_label
- total_revenue
- transaction_count
- avg_transaction_value
- units_sold
- mom_revenue_change_pct

### Category performance

Use {SOURCE_VIEWS["category_kpi"]}.

Recommended fields:

- category
- total_revenue
- revenue_share_pct
- units_sold
- transaction_count
- unique_products_sold
- category_rank
- top_3_category_flag

## Important data limitations

**Inventory turnover ratio** is calculated as a simplified units-based
proxy (total units sold / current inventory on hand) because the source
data contains a single inventory snapshot and no cost-of-goods-sold or
average-inventory history. See the `inventory_turnover_status` column in
the store_performance and enterprise_kpi views for the caveat flag.

**Market share** is calculated as internal chain share — each store's
revenue as a percentage of total MegaMart chain revenue. The dataset
contains MegaMart sales only and no external market-total or competitor
data, so true external market share is not available. See the
`market_share_status` column in the store_performance and enterprise_kpi
views.

The dashboard should present these limitations rather than filling the gaps
with estimates.
"""


# -----------------------------------------------------------------------------
# 8. Main
# -----------------------------------------------------------------------------

def main() -> None:
    print("=" * 88)
    print("MEGAMART 07_DASHBOARD START")
    print(f"Run ID: {RUN_ID}")
    print(
        f"Project period: {PROJECT_START_DATE} -> {PROJECT_END_DATE}"
    )
    print(f"Output path: {RUN_OUTPUT_PATH}")
    print("=" * 88)

    # Read from consolidated gold views — no table creation needed.
    store_dashboard = spark.table(SOURCE_VIEWS["store_kpi"])
    category_dashboard = spark.table(SOURCE_VIEWS["category_kpi"])
    monthly_dashboard = spark.table(SOURCE_VIEWS["monthly_kpi"])
    enterprise_dashboard = spark.table(SOURCE_VIEWS["enterprise_kpi"])
    store_cat_dashboard = spark.table(SOURCE_VIEWS["store_category_top3"])

    print("\nSource Gold views:")
    for key in ["store_kpi", "category_kpi", "monthly_kpi", "enterprise_kpi", "store_category_top3"]:
        print(f"  {SOURCE_VIEWS[key]}: {spark.table(SOURCE_VIEWS[key]).count():,} rows")

    Path(RUN_OUTPUT_PATH).mkdir(
        parents=True,
        exist_ok=True,
    )

    write_single_csv(
        store_dashboard,
        "dashboard_store_performance.csv",
    )

    write_single_csv(
        category_dashboard,
        "dashboard_category_performance.csv",
    )

    write_single_csv(
        monthly_dashboard,
        "dashboard_monthly_trend.csv",
    )

    write_single_csv(
        enterprise_dashboard,
        "dashboard_enterprise_summary.csv",
    )

    write_single_csv(
        store_cat_dashboard,
        "dashboard_store_category_top3.csv",
    )

    guide = build_dashboard_guide(store_dashboard)

    write_text_artifact(
        "dashboard_build_guide.md",
        guide,
    )

    counts = {
        "store": store_dashboard.count(),
        "category": category_dashboard.count(),
        "monthly": monthly_dashboard.count(),
        "enterprise": enterprise_dashboard.count(),
        "store_cat_top3": store_cat_dashboard.count(),
    }

    if counts["store"] != 5:
        raise RuntimeError(
            f"Dashboard store view expected 5 rows, found {counts['store']}."
        )

    if counts["category"] != 7:
        raise RuntimeError(
            f"Dashboard category view expected 7 rows, found {counts['category']}."
        )

    if counts["store_cat_top3"] != 15:
        raise RuntimeError(
            f"Dashboard store_category_top3 view expected 15 rows, "
            f"found {counts['store_cat_top3']}."
        )

    if counts["monthly"] != 12:
        raise RuntimeError(
            f"Dashboard monthly table expected 12 rows, "
            f"found {counts['monthly']}."
        )

    if counts["enterprise"] != 1:
        raise RuntimeError(
            "Dashboard enterprise table must contain exactly "
            "one row."
        )

    records_processed = (
        counts["store"]
        + counts["category"]
        + counts["monthly"]
        + counts["enterprise"]
        + counts["store_cat_top3"]
    )

    write_pipeline_audit(
        status="SUCCESS",
        records_processed=records_processed,
        message=(
            "Dashboard CSV artifacts generated from Gold views. "
            "The final Databricks AI/BI Dashboard reads these views directly."
        ),
    )

    print("\nDashboard Gold views used:")

    for key, count in [
        ("store_kpi", counts["store"]),
        ("category_kpi", counts["category"]),
        ("monthly_kpi", counts["monthly"]),
        ("enterprise_kpi", counts["enterprise"]),
        ("store_category_top3", counts["store_cat_top3"]),
    ]:
        print(
            f"  {SOURCE_VIEWS[key]}: {count:,} rows"
        )

    print("\nDashboard artifacts:")
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_store_performance.csv"
    )
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_category_performance.csv"
    )
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_monthly_trend.csv"
    )
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_enterprise_summary.csv"
    )
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_store_category_top3.csv"
    )
    print(
        f"  {RUN_OUTPUT_PATH}/dashboard_build_guide.md"
    )

    print("\nNext Databricks UI step:")
    print(
        "  Create an AI/BI Dashboard and use the five "
        "gold views as its data sources."
    )

    print("\nSTAGE 07_DASHBOARD DATA PREPARATION COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        try:
            write_pipeline_audit(
                status="FAILED",
                records_processed=0,
                message=str(exc)[:4000],
            )
        except Exception as audit_exc:
            print(
                "Audit write failed after stage failure: "
                f"{audit_exc}"
            )

        print(
            f"STAGE 07_DASHBOARD FAILED: {exc}"
        )
        raise
