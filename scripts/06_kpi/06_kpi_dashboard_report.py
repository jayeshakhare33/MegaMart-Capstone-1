# Databricks source file
# MegaMart Capstone - Stage 06: KPI Dashboard HTML Report Generator
#
# Purpose:
#   Recreate all 15 widgets from the Databricks AI/BI Dashboard as a
#   standalone HTML report with embedded matplotlib charts (base64 PNG)
#   and HTML tables.  The report mirrors the dashboard's 3 pages:
#     1. Store Performance Overview
#     2. Monthly Trends
#     3. Category Performance
#
# This script reads directly from the Gold views — no dashboard export
# or screenshot needed.

from __future__ import annotations

import base64
import io
import sys
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# -----------------------------------------------------------------------------
# 1. Resolve project root
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
        "and shared/common.py."
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

REPORT_BASE = f"/Volumes/{CATALOG_NAME}/gold/report_artifacts/kpi"
RUN_OUTPUT_PATH = f"{REPORT_BASE}/{RUN_ID}"


# -----------------------------------------------------------------------------
# 2. Helpers
# -----------------------------------------------------------------------------

STORE_COLOURS = {
    "HITEC City Store":     "#4e79a7",
    "Banjara Hills Store":  "#f28e2b",
    "Ameerpet Store":       "#e15759",
    "Kukatpally Store":     "#76b7b2",
    "Dilsukhnagar Store":   "#59a14f",
}

DEFAULT_COLOUR = "#4e79a7"

CATEGORY_PALETTE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
    "#59a14f", "#edc948", "#b07aa1",
]


def get_store_colour(name: str) -> str:
    return STORE_COLOURS.get(name, DEFAULT_COLOUR)


def fig_to_base64_png(fig) -> str:
    """Convert a matplotlib figure to a base64-encoded PNG string."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def df_to_pandas(df: DataFrame):
    """Convert Spark DataFrame to pandas, casting DECIMAL columns to float."""
    pdf = df.toPandas()
    for col in pdf.columns:
        if pdf[col].dtype == "object":
            try:
                pdf[col] = pdf[col].astype(float)
            except (TypeError, ValueError):
                pass
    return pdf


def html_data_table(pdf, max_rows: int = 100) -> str:
    """Generate an HTML table from a pandas DataFrame."""
    if pdf.empty:
        return "<p>No data available.</p>"
    display = pdf.head(max_rows).copy()
    for col in display.columns:
        if display[col].dtype == "float64":
            display[col] = display[col].round(2)
    return display.to_html(index=False, border=0, classes="widget-table", escape=False)


def kpi_card(value: str, label: str, colour: str = "#4e79a7") -> str:
    return (
        f'<div class="kpi-card" style="border-top: 4px solid {colour};">'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f'</div>'
    )


def chart_container(title: str, img_b64: str) -> str:
    return (
        f'<div class="chart-container">'
        f'<h3>{title}</h3>'
        f'<img src="data:image/png;base64,{img_b64}" '
        f'style="max-width:100%;height:auto;" />'
        f'</div>'
    )


def table_container(title: str, table_html: str) -> str:
    return (
        f'<div class="table-container">'
        f'<h3>{title}</h3>'
        f'{table_html}'
        f'</div>'
    )


# -----------------------------------------------------------------------------
# 3. Chart generators (one per dashboard widget type)
# -----------------------------------------------------------------------------

def chart_bar(
    labels: List[str],
    values: List[float],
    title: str,
    ylabel: str,
    colours: List[str] = None,
    fmt: str = "₹{:,.0f}",
) -> str:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))

    if colours is None:
        colours = [DEFAULT_COLOUR] * len(labels)

    bars = ax.bar(labels, values, color=colours, edgecolor="white", linewidth=0.5)
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=13, weight="bold", pad=12)
    ax.tick_params(axis="x", rotation=30)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(values) * 0.01,
            fmt.format(val),
            ha="center", va="bottom", fontsize=9,
        )

    fig.tight_layout()
    return fig_to_base64_png(fig)


def chart_line(
    labels: List[str],
    values: List[float],
    title: str,
    ylabel: str,
    colour: str = "#4e79a7",
) -> str:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(labels, values, color=colour, linewidth=2.5, marker="o", markersize=6)
    ax.fill_between(labels, values, alpha=0.15, color=colour)
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=13, weight="bold", pad=12)
    ax.tick_params(axis="x", rotation=45)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for i, val in enumerate(values):
        ax.annotate(
            f"₹{val:,.0f}",
            (i, val),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
        )

    fig.tight_layout()
    return fig_to_base64_png(fig)


def chart_pie(
    labels: List[str],
    values: List[float],
    title: str,
    colours: List[str] = None,
) -> str:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 6))

    if colours is None:
        colours = CATEGORY_PALETTE[: len(labels)]

    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colours,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.75,
        wedgeprops=dict(width=0.5, edgecolor="white", linewidth=1.5),
    )
    for t in autotexts:
        t.set_fontsize(9)
    for t in texts:
        t.set_fontsize(9)

    ax.set_title(title, fontsize=13, weight="bold", pad=12)
    fig.tight_layout()
    return fig_to_base64_png(fig)


# -----------------------------------------------------------------------------
# 4. Audit helper
# -----------------------------------------------------------------------------

def write_pipeline_audit(status: str, records_processed: int, message: str = "") -> None:
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
# 5. HTML template
# -----------------------------------------------------------------------------

HTML_CSS = """
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }
  .header { background: linear-gradient(135deg, #1a3a5c, #2d5f8a); color: white; padding: 24px 40px; }
  .header h1 { margin: 0; font-size: 24px; }
  .header .meta { margin-top: 6px; font-size: 13px; opacity: 0.85; }
  .page-section { background: white; margin: 24px 40px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }
  .page-title { background: #e8eef3; padding: 14px 24px; font-size: 18px; font-weight: 700; color: #1a3a5c; border-bottom: 2px solid #d0d8df; }
  .page-content { padding: 24px; }
  .kpi-row { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 24px; }
  .kpi-card { display: flex; flex-direction: column; min-width: 160px; flex: 1; background: #fafbfc; border: 1px solid #e0e4e8; border-radius: 8px; padding: 16px; text-align: center; }
  .kpi-value { font-size: 22px; font-weight: 700; color: #1a3a5c; }
  .kpi-label { font-size: 12px; color: #666; margin-top: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
  .chart-container { margin: 16px 0; }
  .chart-container h3 { font-size: 14px; color: #333; margin-bottom: 8px; }
  .table-container { margin: 16px 0; overflow-x: auto; }
  .table-container h3 { font-size: 14px; color: #333; margin-bottom: 8px; }
  .widget-table { border-collapse: collapse; width: 100%; font-size: 12px; }
  .widget-table th { background: #e8eef3; color: #1a3a5c; font-weight: 600; padding: 8px 10px; text-align: left; border-bottom: 2px solid #d0d8df; }
  .widget-table td { padding: 6px 10px; border-bottom: 1px solid #eef1f4; }
  .widget-table tr:hover { background: #f8f9fa; }
  .note { padding: 12px 16px; border-left: 4px solid #999; background: #fafafa; margin: 16px 0; font-size: 12px; color: #555; border-radius: 0 4px 4px 0; }
  .footer { text-align: center; padding: 16px; color: #999; font-size: 11px; }
</style>
"""


def build_html(
    page1_widgets: List[str],
    page2_widgets: List[str],
    page3_widgets: List[str],
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MegaMart Dashboard Report</title>
{HTML_CSS}
</head>
<body>

<div class="header">
  <h1>MegaMart Store Performance Dashboard Report</h1>
  <div class="meta">
    Run ID: {RUN_ID} &nbsp;|&nbsp;
    Project period: {PROJECT_START_DATE} to {PROJECT_END_DATE} &nbsp;|&nbsp;
    Generated: {RUN_TIMESTAMP}
  </div>
</div>

<div class="page-section">
  <div class="page-title">Page 1 &mdash; Store Performance Overview</div>
  <div class="page-content">
    {''.join(page1_widgets)}
  </div>
</div>

<div class="page-section">
  <div class="page-title">Page 2 &mdash; Monthly Trends</div>
  <div class="page-content">
    {''.join(page2_widgets)}
  </div>
</div>

<div class="page-section">
  <div class="page-title">Page 3 &mdash; Category Performance</div>
  <div class="page-content">
    {''.join(page3_widgets)}
  </div>
</div>

<div class="note">
  <strong>Inventory turnover ratio:</strong> Simplified units-based proxy
  (total units sold / current inventory on hand). The source data contains a
  single inventory snapshot with no COGS or average-inventory history.
  See the <code>inventory_turnover_status</code> column in the gold views.
</div>
<div class="note">
  <strong>Market share:</strong> Internal chain share &mdash; each store's
  revenue as a percentage of total MegaMart chain revenue. No external
  market or competitor data is available. See the
  <code>market_share_status</code> column in the gold views.
</div>

<div class="footer">
  MegaMart Data Engineering Capstone &mdash; Problem Statement 5<br>
  Generated from Gold layer views: store_performance, monthly_kpis,
  category_kpis, enterprise_kpi, store_category_top3
</div>

</body>
</html>"""


# -----------------------------------------------------------------------------
# 6. Main execution
# -----------------------------------------------------------------------------

def main() -> None:
    print("=" * 80)
    print("MEGAMART 06_KPI DASHBOARD REPORT START")
    print(f"Run ID: {RUN_ID}")
    print(f"Output path: {RUN_OUTPUT_PATH}")
    print("=" * 80)

    # ------------------------------------------------------------------
    # Read data from Gold views (same queries as the AI/BI Dashboard)
    # ------------------------------------------------------------------

    enterprise_df = spark.table(GOLD_VIEWS["enterprise_kpi"])
    store_df = spark.table(GOLD_VIEWS["store_performance"])
    monthly_df = spark.table(GOLD_VIEWS["monthly_kpis"])
    category_df = spark.table(GOLD_VIEWS["category_kpis"])
    store_cat_df = spark.table(GOLD_VIEWS["store_category_top3"])

    # Convert to pandas for charting (data is small: 1/5/12/7/15 rows)
    enterprise_pdf = df_to_pandas(enterprise_df)
    store_pdf = df_to_pandas(store_df)
    monthly_pdf = df_to_pandas(monthly_df)
    category_pdf = df_to_pandas(category_df)
    store_cat_pdf = df_to_pandas(store_cat_df)

    # Ensure correct sort order
    store_pdf = store_pdf.sort_values("revenue_rank")
    monthly_pdf = monthly_pdf.sort_values("month_start")
    category_pdf = category_pdf.sort_values("category_rank")
    store_cat_pdf = store_cat_pdf.sort_values(["store_id", "cat_rank"])

    ent = enterprise_pdf.iloc[0]

    # ------------------------------------------------------------------
    # Page 1: Store Performance Overview
    # ------------------------------------------------------------------
    page1: List[str] = []

    # 4 KPI counters
    page1.append(
        '<div class="kpi-row">'
        + kpi_card(f"₹{ent['total_revenue']:,.2f}", "Total Revenue", "#4e79a7")
        + kpi_card(f"{int(ent['transaction_count']):,}", "Transactions", "#f28e2b")
        + kpi_card(f"₹{ent['avg_transaction_value']:,.2f}", "Avg Transaction Value", "#e15759")
        + kpi_card(f"{int(ent['unique_products_sold'])}", "Unique Products Sold", "#59a14f")
        + "</div>"
    )

    # Bar: Total Revenue by Store
    page1.append(chart_container(
        "Total Revenue by Store",
        chart_bar(
            store_pdf["store_name"].tolist(),
            store_pdf["total_revenue"].astype(float).tolist(),
            "Total Revenue by Store",
            "Revenue (₹)",
            colours=[get_store_colour(s) for s in store_pdf["store_name"]],
            fmt="₹{:,.0f}",
        ),
    ))

    # Bar: Market Share %
    page1.append(chart_container(
        "Market Share % (Internal Chain Share)",
        chart_bar(
            store_pdf["store_name"].tolist(),
            store_pdf["market_share_pct"].astype(float).tolist(),
            "Market Share % (Internal Chain Share)",
            "Market Share (%)",
            colours=[get_store_colour(s) for s in store_pdf["store_name"]],
            fmt="{:,.1f}%",
        ),
    ))

    # Bar: Inventory Turnover Ratio
    page1.append(chart_container(
        "Inventory Turnover Ratio by Store",
        chart_bar(
            store_pdf["store_name"].tolist(),
            store_pdf["inventory_turnover_ratio"].astype(float).tolist(),
            "Inventory Turnover Ratio by Store",
            "Turnover Ratio",
            colours=[get_store_colour(s) for s in store_pdf["store_name"]],
            fmt="{:.2f}",
        ),
    ))

    # Table: Store Performance Comparison
    store_table_cols = [
        "store_name", "total_revenue", "transaction_count",
        "average_transaction_value", "unique_products_sold",
        "store_revenue_share_pct", "inventory_turnover_ratio",
        "current_inventory_units", "out_of_stock_skus",
        "inventory_health_pct",
    ]
    page1.append(table_container(
        "Store Performance Comparison",
        html_data_table(store_pdf[store_table_cols]),
    ))

    # ------------------------------------------------------------------
    # Page 2: Monthly Trends
    # ------------------------------------------------------------------
    page2: List[str] = []

    # Line: Monthly Revenue Trend
    page2.append(chart_container(
        "Monthly Revenue Trend",
        chart_line(
            monthly_pdf["month_label"].tolist(),
            monthly_pdf["total_revenue"].astype(float).tolist(),
            "Monthly Revenue Trend",
            "Total Revenue (₹)",
            colour="#4e79a7",
        ),
    ))

    # Bar: MoM Revenue Change %
    mom_values = monthly_pdf["mom_revenue_change_pct"].astype(float).tolist()
    mom_colours = ["#59a14f" if v >= 0 else "#e15759" for v in mom_values]
    page2.append(chart_container(
        "Month-over-Month Revenue Change %",
        chart_bar(
            monthly_pdf["month_label"].tolist(),
            mom_values,
            "Month-over-Month Revenue Change %",
            "Change (%)",
            colours=mom_colours,
            fmt="{:+.1f}%",
        ),
    ))

    # Bar: Units Sold by Month
    page2.append(chart_container(
        "Units Sold by Month",
        chart_bar(
            monthly_pdf["month_label"].tolist(),
            monthly_pdf["units_sold"].astype(float).tolist(),
            "Units Sold by Month",
            "Units Sold",
            colours=["#76b7b2"] * len(monthly_pdf),
            fmt="{:,.0f}",
        ),
    ))

    # Table: Monthly KPI Summary
    monthly_table_cols = [
        "month_label", "total_revenue", "transaction_count",
        "avg_transaction_value", "units_sold", "mom_revenue_change_pct",
    ]
    page2.append(table_container(
        "Monthly KPI Summary",
        html_data_table(monthly_pdf[monthly_table_cols]),
    ))

    # ------------------------------------------------------------------
    # Page 3: Category Performance
    # ------------------------------------------------------------------
    page3: List[str] = []

    # Bar: Top 3 Categories by Revenue (filter to rank <= 3)
    top3_cat = category_pdf[category_pdf["category_rank"] <= 3].copy()
    page3.append(chart_container(
        "Top 3 Categories by Revenue",
        chart_bar(
            top3_cat["category"].tolist(),
            top3_cat["total_revenue"].astype(float).tolist(),
            "Top 3 Categories by Revenue",
            "Revenue (₹)",
            colours=CATEGORY_PALETTE[: len(top3_cat)],
            fmt="₹{:,.0f}",
        ),
    ))

    # Pie: Revenue Share by Category (all 7 categories)
    page3.append(chart_container(
        "Revenue Share by Category",
        chart_pie(
            category_pdf["category"].tolist(),
            category_pdf["revenue_share_pct"].astype(float).tolist(),
            "Revenue Share by Category",
            colours=CATEGORY_PALETTE[: len(category_pdf)],
        ),
    ))

    # Table: Top 3 Categories by Store
    store_cat_table_cols = [
        "store_name", "category", "category_revenue",
        "units_sold", "transaction_count", "cat_rank",
    ]
    page3.append(table_container(
        "Top 3 Categories by Store",
        html_data_table(store_cat_pdf[store_cat_table_cols]),
    ))

    # ------------------------------------------------------------------
    # Build and write the HTML report
    # ------------------------------------------------------------------

    html = build_html(page1, page2, page3)

    output_path = Path(f"{RUN_OUTPUT_PATH}/kpi_dashboard_report.html")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    # Audit
    total_widgets = len(page1) + len(page2) + len(page3)
    write_pipeline_audit(
        status="SUCCESS",
        records_processed=total_widgets,
        message=(
            "Dashboard HTML report generated with 15 widgets (4 counters, "
            "5 bar charts, 1 line chart, 1 pie chart, 3 tables) across 3 "
            "pages. Charts embedded as base64 PNG."
        ),
    )

    print(f"\nReport written to: {output_path}")
    print(f"Total widgets rendered: {total_widgets}")
    print("\nSTAGE 06_KPI DASHBOARD REPORT COMPLETED SUCCESSFULLY")


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
        print(f"STAGE 06_KPI DASHBOARD REPORT FAILED: {exc}")
        raise
