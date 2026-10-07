# Databricks source file
# MegaMart Capstone - Stage 06: KPI Pipeline
#
# Purpose:
#   Build Gold KPI tables from Silver data and publish CSV/HTML/PDF KPI
#   artifacts to the Unity Catalog Volume created during setup.
#
# This file intentionally does NOT create infrastructure. It only creates
# run-specific output folders/tables.

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T


# -----------------------------------------------------------------------------
# 1. Resolve project root so config/shared imports work from a Git-backed
#    Databricks folder regardless of the notebook/script working directory.
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
        if (candidate / "config" / "project_config.py").exists() and (
            candidate / "shared" / "common.py"
        ).exists():
            return candidate
    raise RuntimeError(
        "Could not locate project root containing config/project_config.py "
        "and shared/common.py. Run this file from the MegaMart-Capstone-1 Git folder."
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

STAGE_NAME = "06_kpi"
PIPELINE_NAME = "megamart_pipeline"
RUN_ID = create_run_id(STAGE_NAME)
RUN_TIMESTAMP = get_utc_timestamp()

GOLD_SCHEMA = f"{CATALOG_NAME}.gold"
AUDIT_SCHEMA = f"{CATALOG_NAME}.audit"

# report_artifacts volume is created by 00_setup/admin work.
REPORT_BASE = f"/Volumes/{CATALOG_NAME}/gold/report_artifacts/kpi"
RUN_OUTPUT_PATH = f"{REPORT_BASE}/{RUN_ID}"


# -----------------------------------------------------------------------------
# 2. Helpers
# -----------------------------------------------------------------------------

# def table_name(table_key: str) -> str:
#     return SILVER_TABLES[table_key]


# def gold_name(table_key: str, fallback: str) -> str:
#     return GOLD_TABLES.get(table_key, fallback)


def ensure_columns(df: DataFrame, required: List[str], table: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{table} is missing required columns: {missing}")


# def write_gold(df: DataFrame, full_table_name: str) -> None:
#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(full_table_name)
#     )


def save_csv(df: DataFrame, file_name: str) -> None:
    # Spark CSV output is a directory with part files. Pandas gives the
    # requested single CSV artifact and the data sizes here are intentionally
    # small (5/12/70/1 rows).
    pdf = df.toPandas()
    output = Path(f"{RUN_OUTPUT_PATH}/{file_name}")
    output.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(output, index=False)


def html_table(df: DataFrame, max_rows: int = 25) -> str:
    pdf = df.limit(max_rows).toPandas()
    if pdf.empty:
        return "<p>No rows.</p>"
    return pdf.to_html(index=False, border=0, classes="data-table")


def build_pdf(pdf_path: str, sections: List[tuple[str, DataFrame]]) -> None:
    # Matplotlib is used only for the PDF artifact. All source data comes from
    # Spark and the PDF is written directly to the Unity Catalog Volume path.
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    with PdfPages(pdf_path) as pdf:
        for title, df in sections:
            local = df.toPandas()
            fig = plt.figure(figsize=(11, 8.5))
            fig.text(0.05, 0.95, title, fontsize=16, weight="bold", va="top")

            if local.empty:
                fig.text(0.05, 0.85, "No rows available.", fontsize=11)
            else:
                # Keep PDF readable and compact for the small capstone outputs.
                display_df = local.head(30).copy()
                ax = fig.add_axes([0.03, 0.08, 0.94, 0.80])
                ax.axis("off")
                table = ax.table(
                    cellText=display_df.astype(str).values,
                    colLabels=display_df.columns.tolist(),
                    cellLoc="left",
                    loc="upper left",
                )
                table.auto_set_font_size(False)
                table.set_fontsize(7)
                table.scale(1, 1.25)

            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


# -----------------------------------------------------------------------------
# 5. Audit helpers
# -----------------------------------------------------------------------------

def write_pipeline_audit(status: str, records_processed: int, message: str = "") -> None:
    """Append to the existing audit table without assuming its exact schema."""
    audit_table = AUDIT_TABLES["pipeline_runs"]
    existing_schema = spark.table(audit_table).schema
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
        elif name in {"ended_at", "end_time", "completed_at", "run_ended_at"}:
            value = now
        elif name in {"records_processed", "record_count", "row_count", "rows_processed"}:
            value = int(records_processed)
        elif name in {"message", "error_message", "details"}:
            value = message
        else:
            # Preserve the table's declared type for fields that belong to
            # earlier stages but are not needed by this KPI stage.
            if isinstance(field.dataType, T.StringType):
                value = ""
            elif isinstance(field.dataType, (T.IntegerType, T.LongType, T.ShortType, T.ByteType, T.DoubleType, T.FloatType, T.DecimalType)):
                value = 0
            elif isinstance(field.dataType, T.BooleanType):
                value = False
            else:
                value = None
        values[field.name] = value

    audit_df = spark.createDataFrame([values], schema=existing_schema)
    audit_df.write.mode("append").format("delta").saveAsTable(audit_table)


# -----------------------------------------------------------------------------
# 6. Main execution
# -----------------------------------------------------------------------------

def main() -> None:
    print("=" * 80)
    print(f"MEGAMART {STAGE_NAME.upper()} START")
    print(f"Run ID: {RUN_ID}")
    print(f"Project period: {PROJECT_START_DATE} -> {PROJECT_END_DATE}")
    print(f"Output path: {RUN_OUTPUT_PATH}")
    print("=" * 80)

    # Gold layer is now views defined in gold_layer_views.sql.
    # This stage reads from those views and generates artifacts only —
    # no table creation, no DataFrame building.
    store_kpis = spark.table(GOLD_VIEWS["store_performance"])
    category_kpis = spark.table(GOLD_VIEWS["category_kpis"])
    monthly_kpis = spark.table(GOLD_VIEWS["monthly_kpis"])
    enterprise_kpis = spark.table(GOLD_VIEWS["enterprise_kpi"])

    # Validate view row counts before publishing artifacts.
    counts = {
        "store_kpi_summary": store_kpis.count(),
        "category_kpi_summary": category_kpis.count(),
        "monthly_kpi_summary": monthly_kpis.count(),
        "enterprise_kpi_summary": enterprise_kpis.count(),
    }

    if counts["store_kpi_summary"] != 5:
        raise RuntimeError(f"Store performance view expected 5 rows, found {counts['store_kpi_summary']}.")
    if counts["monthly_kpi_summary"] != 12:
        raise RuntimeError(f"Monthly KPI view expected 12 project months, found {counts['monthly_kpi_summary']}.")
    if counts["enterprise_kpi_summary"] != 1:
        raise RuntimeError("Enterprise KPI view must contain exactly one row.")

    Path(RUN_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    save_csv(store_kpis, "store_kpi_summary.csv")
    save_csv(category_kpis, "category_kpi_summary.csv")
    save_csv(monthly_kpis, "monthly_kpi_summary.csv")
    save_csv(enterprise_kpis, "enterprise_kpi_summary.csv")

    enterprise_row = enterprise_kpis.first().asDict()
    top3_categories = [
        r["category"]
        for r in category_kpis.filter(F.col("category_rank") <= 3)
        .orderBy("category_rank")
        .select("category")
        .collect()
    ]

    latest_month = (
        monthly_kpis.orderBy(F.col("month_start").desc()).limit(1).toPandas().to_dict("records")
    )
    latest_month_html = ""
    if latest_month:
        latest_month_html = f"<pre>{latest_month[0]}</pre>"

    html_path = Path(f"{RUN_OUTPUT_PATH}/kpi_summary.html")
    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>MegaMart KPI Summary</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 32px; }}
h1, h2 {{ margin-bottom: 8px; }}
.meta {{ color: #555; margin-bottom: 20px; }}
.card {{ display: inline-block; vertical-align: top; min-width: 180px; margin: 0 12px 12px 0; padding: 14px; border: 1px solid #ddd; border-radius: 6px; }}
.card .value {{ font-size: 24px; font-weight: 700; }}
.card .label {{ color: #666; font-size: 13px; margin-top: 4px; }}
.data-table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
.data-table th, .data-table td {{ border: 1px solid #ddd; padding: 7px; text-align: left; font-size: 12px; }}
.data-table th {{ background: #f3f3f3; }}
.note {{ padding: 12px; border-left: 4px solid #999; background: #fafafa; margin: 16px 0; }}
</style>
</head>
<body>
<h1>MegaMart KPI Summary</h1>
<div class=\"meta\">Run ID: {RUN_ID} | Project period: {PROJECT_START_DATE} to {PROJECT_END_DATE}</div>

<div class=\"card\"><div class=\"value\">₹{enterprise_row['total_revenue']:,.2f}</div><div class=\"label\">Total Revenue</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['transaction_count']:,}</div><div class=\"label\">Transactions</div></div>
<div class=\"card\"><div class=\"value\">₹{enterprise_row['avg_transaction_value']:,.2f}</div><div class=\"label\">Average Transaction Value</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['unique_products_sold']}</div><div class=\"label\">Unique Products Sold</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['current_inventory_units']:,}</div><div class=\"label\">Current Inventory Units</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['out_of_stock_skus']}</div><div class=\"label\">Out-of-Stock SKUs</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['low_stock_skus']}</div><div class=\"label\">Low-Stock SKUs</div></div>

<h2>Top 3 Categories by Revenue</h2>
<p>{', '.join(top3_categories) if top3_categories else 'No category data'}</p>

<h2>Store KPI Summary</h2>
{html_table(store_kpis)}

<h2>Category KPI Summary</h2>
{html_table(category_kpis)}

<h2>Month-on-Month Trend</h2>
{html_table(monthly_kpis)}

<div class=\"note\"><strong>Inventory turnover ratio:</strong> calculated as a simplified units-based proxy (total units sold / current inventory on hand) because the source data contains a single inventory snapshot and no cost-of-goods-sold or average-inventory history. See the <code>inventory_turnover_status</code> column in the store and enterprise KPI views for the caveat flag.</div>
<div class=\"note\"><strong>Market share:</strong> calculated as internal chain share — each store's revenue as a percentage of total MegaMart chain revenue. The dataset contains MegaMart sales only and no external market-total or competitor data, so true external market share is not available. See the <code>market_share_status</code> column in the store and enterprise KPI views.</div>

<h2>Latest Month Record</h2>
{latest_month_html}
</body>
</html>"""
    html_path.write_text(html, encoding="utf-8")

    pdf_path = Path(f"{RUN_OUTPUT_PATH}/kpi_summary.pdf")
    build_pdf(
        str(pdf_path),
        [
            ("Enterprise KPI Summary", enterprise_kpis),
            ("Store KPI Summary", store_kpis),
            ("Top Categories", category_kpis.filter(F.col("category_rank") <= 3)),
            ("Month-on-Month Trend", monthly_kpis),
        ],
    )

    write_pipeline_audit(
        status="SUCCESS",
        records_processed=int(counts["monthly_kpi_summary"]),
        message=(
            "Gold KPI views read and CSV/HTML/PDF artifacts published. "
            "Inventory turnover is provided as a simplified units-based proxy "
            "(units sold / current stock). Market share is internal chain share "
            "(store revenue / total chain revenue). Both include status flags "
            "documenting their limitations."
        ),
    )

    print("\nGold KPI views:")
    print(f"  {GOLD_VIEWS['store_performance']}: {counts['store_kpi_summary']} rows")
    print(f"  {GOLD_VIEWS['category_kpis']}: {counts['category_kpi_summary']} rows")
    print(f"  {GOLD_VIEWS['monthly_kpis']}: {counts['monthly_kpi_summary']} rows")
    print(f"  {GOLD_VIEWS['enterprise_kpi']}: {counts['enterprise_kpi_summary']} rows")

    print("\nArtifacts:")
    print(f"  {RUN_OUTPUT_PATH}/store_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/category_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/monthly_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/enterprise_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/kpi_summary.html")
    print(f"  {RUN_OUTPUT_PATH}/kpi_summary.pdf")
    print("\nSTAGE 06_KPI COMPLETED SUCCESSFULLY")


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
            print(f"Audit write failed after stage failure: {audit_exc}")
        print(f"STAGE 06_KPI FAILED: {exc}")
        raise
