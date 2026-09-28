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
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


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


SOURCE_TABLES = {
    "store_kpi": f"{GOLD_SCHEMA}.store_kpi_summary",
    "category_kpi": f"{GOLD_SCHEMA}.category_kpi_summary",
    "monthly_kpi": f"{GOLD_SCHEMA}.monthly_kpi_summary",
    "enterprise_kpi": f"{GOLD_SCHEMA}.enterprise_kpi_summary",
    "inventory": f"{CATALOG_NAME}.silver.inventory",
    "stores": f"{CATALOG_NAME}.silver.stores",
}


TARGET_TABLES = {
    "store_dashboard": f"{GOLD_SCHEMA}.dashboard_store_performance",
    "category_dashboard": f"{GOLD_SCHEMA}.dashboard_category_performance",
    "monthly_dashboard": f"{GOLD_SCHEMA}.dashboard_monthly_trend",
    "enterprise_dashboard": f"{GOLD_SCHEMA}.dashboard_enterprise_summary",
}


# -----------------------------------------------------------------------------
# 3. Helpers
# -----------------------------------------------------------------------------

def table_exists(full_table_name: str) -> bool:
    return spark.catalog.tableExists(full_table_name)


def ensure_columns(df: DataFrame, required: List[str], table_name: str) -> None:
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {missing}"
        )


def write_gold(df: DataFrame, full_table_name: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )


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
# 4. Load and validate source tables
# -----------------------------------------------------------------------------

def load_sources() -> Dict[str, DataFrame]:
    missing_tables = [
        name
        for name, table in SOURCE_TABLES.items()
        if not table_exists(table)
    ]

    if missing_tables:
        raise ValueError(
            "Required source tables are missing: "
            + ", ".join(missing_tables)
        )

    sources = {
        key: spark.table(table)
        for key, table in SOURCE_TABLES.items()
    }

    ensure_columns(
        sources["store_kpi"],
        [
            "store_id",
            "store_name",
            "total_revenue",
            "transaction_count",
            "avg_transaction_value",
            "unique_products_sold",
            "store_revenue_share_pct",
        ],
        SOURCE_TABLES["store_kpi"],
    )

    ensure_columns(
        sources["category_kpi"],
        [
            "category",
            "total_revenue",
            "units_sold",
            "transaction_count",
            "unique_products_sold",
            "category_rank",
            "top_3_category_flag",
        ],
        SOURCE_TABLES["category_kpi"],
    )

    ensure_columns(
        sources["monthly_kpi"],
        [
            "month_start",
            "total_revenue",
            "transaction_count",
            "avg_transaction_value",
            "unique_products_sold",
            "units_sold",
            "categories_sold",
            "mom_revenue_change_pct",
        ],
        SOURCE_TABLES["monthly_kpi"],
    )

    ensure_columns(
        sources["enterprise_kpi"],
        [
            "total_revenue",
            "transaction_count",
            "avg_transaction_value",
            "unique_products_sold",
            "current_inventory_units",
            "out_of_stock_skus",
            "low_stock_skus",
        ],
        SOURCE_TABLES["enterprise_kpi"],
    )

    ensure_columns(
        sources["inventory"],
        [
            "store_id",
            "product_id",
            "quantity_on_hand",
        ],
        SOURCE_TABLES["inventory"],
    )

    ensure_columns(
        sources["stores"],
        [
            "store_id",
            "store_name",
        ],
        SOURCE_TABLES["stores"],
    )

    return sources


# -----------------------------------------------------------------------------
# 5. Build dashboard-ready Gold tables
# -----------------------------------------------------------------------------

def build_store_dashboard(
    store_kpi: DataFrame,
    inventory: DataFrame,
) -> DataFrame:

    inventory_summary = (
        inventory.groupBy("store_id")
        .agg(
            F.sum("quantity_on_hand")
            .cast("long")
            .alias("current_inventory_units"),

            F.sum(
                F.when(
                    F.col("quantity_on_hand") == 0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("out_of_stock_skus"),

            F.sum(
                F.when(
                    (F.col("quantity_on_hand") > 0)
                    & (F.col("quantity_on_hand") < 50),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("low_stock_skus"),

            F.sum(
                F.when(
                    F.col("quantity_on_hand") >= 50,
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("healthy_stock_skus"),
        )
    )

    return (
        store_kpi
        .join(inventory_summary, "store_id", "left")
        .fillna(
            0,
            subset=[
                "current_inventory_units",
                "out_of_stock_skus",
                "low_stock_skus",
                "healthy_stock_skus",
            ],
        )
        .withColumn(
            "inventory_health_pct",
            F.when(
                F.lit(14) > 0,
                F.round(
                    F.col("healthy_stock_skus") / F.lit(14) * 100,
                    2,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("_dashboard_run_id", F.lit(RUN_ID))
        .withColumn("_dashboard_processed_at", F.current_timestamp())
        .orderBy("store_id")
    )


def build_monthly_dashboard(monthly_kpi: DataFrame) -> DataFrame:
    return (
        monthly_kpi
        .withColumn(
            "month_label",
            F.date_format("month_start", "MMM yyyy"),
        )
        .withColumn("_dashboard_run_id", F.lit(RUN_ID))
        .withColumn("_dashboard_processed_at", F.current_timestamp())
        .orderBy("month_start")
    )


def build_enterprise_dashboard(enterprise_kpi: DataFrame) -> DataFrame:
    return (
        enterprise_kpi
        .withColumn(
            "dashboard_period",
            F.lit(
                f"{PROJECT_START_DATE} to {PROJECT_END_DATE}"
            ),
        )
        .withColumn(
            "dashboard_generated_at",
            F.current_timestamp(),
        )
        .withColumn("_dashboard_run_id", F.lit(RUN_ID))
        .withColumn("_dashboard_processed_at", F.current_timestamp())
    )


# -----------------------------------------------------------------------------
# 6. Category revenue-share construction
# -----------------------------------------------------------------------------

def build_category_dashboard_safe(category_kpi: DataFrame) -> DataFrame:
    total_revenue_row = (
        category_kpi
        .agg(F.sum("total_revenue").alias("total_revenue"))
        .first()
    )

    total_revenue = float(
        total_revenue_row["total_revenue"]
        if total_revenue_row["total_revenue"] is not None
        else 0.0
    )

    return (
        category_kpi
        .withColumn(
            "revenue_share_pct",
            F.when(
                F.lit(total_revenue) > 0,
                F.round(
                    F.col("total_revenue")
                    / F.lit(total_revenue)
                    * 100,
                    2,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("_dashboard_run_id", F.lit(RUN_ID))
        .withColumn("_dashboard_processed_at", F.current_timestamp())
        .orderBy("category_rank")
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

1. {TARGET_TABLES["enterprise_dashboard"]}
2. {TARGET_TABLES["store_dashboard"]}
3. {TARGET_TABLES["category_dashboard"]}
4. {TARGET_TABLES["monthly_dashboard"]}

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

Use {TARGET_TABLES["store_dashboard"]}.

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

Use {TARGET_TABLES["monthly_dashboard"]}.

Recommended fields:

- month_start
- month_label
- total_revenue
- transaction_count
- avg_transaction_value
- units_sold
- mom_revenue_change_pct

### Category performance

Use {TARGET_TABLES["category_dashboard"]}.

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

Inventory turnover is not calculated because the generated dataset contains
one inventory snapshot rather than beginning/ending inventory history and does
not provide COGS.

External market share is not calculated because the generated dataset contains
MegaMart sales only and has no external market-total or competitor-sales data.

Store revenue share is an internal MegaMart chain share, not external market
share.

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

    sources = load_sources()

    print("\nSource Gold tables:")
    for key in [
        "store_kpi",
        "category_kpi",
        "monthly_kpi",
        "enterprise_kpi",
    ]:
        print(
            f"  {SOURCE_TABLES[key]}: "
            f"{sources[key].count():,} rows"
        )

    print(
        f"  {SOURCE_TABLES['inventory']}: "
        f"{sources['inventory'].count():,} rows"
    )

    store_dashboard = build_store_dashboard(
        sources["store_kpi"],
        sources["inventory"],
    )

    # Use the safe version for category revenue share.
    category_dashboard = build_category_dashboard_safe(
        sources["category_kpi"]
    )

    monthly_dashboard = build_monthly_dashboard(
        sources["monthly_kpi"]
    )

    enterprise_dashboard = build_enterprise_dashboard(
        sources["enterprise_kpi"]
    )

    write_gold(
        store_dashboard,
        TARGET_TABLES["store_dashboard"],
    )

    write_gold(
        category_dashboard,
        TARGET_TABLES["category_dashboard"],
    )

    write_gold(
        monthly_dashboard,
        TARGET_TABLES["monthly_dashboard"],
    )

    write_gold(
        enterprise_dashboard,
        TARGET_TABLES["enterprise_dashboard"],
    )

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
    }

    expected_store_rows = (
        sources["stores"]
        .select("store_id")
        .distinct()
        .count()
    )

    expected_category_rows = sources["category_kpi"].count()

    if counts["store"] != expected_store_rows:
        raise RuntimeError(
            "Dashboard store table row count does not match "
            "the store master."
        )

    if counts["category"] != expected_category_rows:
        raise RuntimeError(
            "Dashboard category table row count does not match "
            "the Stage 06 category KPI table."
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
    )

    write_pipeline_audit(
        status="SUCCESS",
        records_processed=records_processed,
        message=(
            "Dashboard-ready Gold tables and CSV artifacts created. "
            "The final Databricks AI/BI Dashboard is built from these "
            "stable Gold dashboard tables."
        ),
    )

    print("\nDashboard Gold tables:")

    for key, count in [
        ("store_dashboard", counts["store"]),
        ("category_dashboard", counts["category"]),
        ("monthly_dashboard", counts["monthly"]),
        ("enterprise_dashboard", counts["enterprise"]),
    ]:
        print(
            f"  {TARGET_TABLES[key]}: {count:,} rows"
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
        f"  {RUN_OUTPUT_PATH}/dashboard_build_guide.md"
    )

    print("\nNext Databricks UI step:")
    print(
        "  Create an AI/BI Dashboard and use the four "
        "gold.dashboard_* tables as its data sources."
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
