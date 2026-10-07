# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 04 - Sales Reporting

Purpose
-------
Generate business-facing outputs from the Gold sales-analysis tables.

Responsibilities
----------------
1. Read Gold sales-analysis tables.
2. Export analytical datasets to CSV files.
3. Generate a self-contained HTML sales report.
4. Generate charts used by the HTML report.
5. Record successful execution in audit.pipeline_runs.

This file does NOT:
- modify Bronze or Silver tables
- recalculate Gold sales metrics
- calculate true product profitability

Profitability limitation
------------------------
The generated source contains selling price/revenue fields but no unit cost.
Therefore this report uses sales/revenue performance and explicitly states
that true profitability is not calculable from the supplied dataset.
"""

# COMMAND ----------

# =========================
# 1. Imports
# =========================

import base64
import html
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from pyspark.sql import functions as F

# =========================
# Project Root / Module Path
# =========================

import os
import sys
from pathlib import Path


def add_project_root_to_python_path():
    """
    Find the Git/project root containing config/project_config.py
    and shared/common.py, then add it to Python's import path.
    """

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

        config_file = (
            candidate
            / "config"
            / "project_config.py"
        )

        shared_file = (
            candidate
            / "shared"
            / "common.py"
        )

        if config_file.exists() and shared_file.exists():

            project_root = str(candidate)

            if project_root not in sys.path:
                sys.path.insert(
                    0,
                    project_root,
                )

            print(
                f"✓ Project root added to Python path: "
                f"{project_root}"
            )

            return project_root

    raise ModuleNotFoundError(
        "Could not locate the MegaMart project root. "
        "Expected config/project_config.py and shared/common.py."
    )


PROJECT_ROOT = add_project_root_to_python_path()

from config.project_config import (
    CATALOG_NAME,
    GOLD_VIEWS,
    AUDIT_TABLES,
)

from shared.common import create_run_id


# COMMAND ----------

# =========================
# 2. Pipeline Configuration
# =========================

PIPELINE_NAME = "megamart_pipeline"
STAGE_NAME = "04_sales_report"

RUN_ID = create_run_id(STAGE_NAME)

START_TIME = datetime.now(timezone.utc)

PIPELINE_RUNS_TABLE = AUDIT_TABLES["pipeline_runs"]

REPORT_VOLUME_NAME = "report_artifacts"

REPORT_BASE_PATH = (
    f"/Volumes/{CATALOG_NAME}/gold/{REPORT_VOLUME_NAME}"
)

REPORT_OUTPUT_PATH = (
    f"{REPORT_BASE_PATH}/sales/{RUN_ID}"
)

print(f"Pipeline       : {PIPELINE_NAME}")
print(f"Stage          : {STAGE_NAME}")
print(f"Run ID         : {RUN_ID}")
print(f"Report path    : {REPORT_OUTPUT_PATH}")


# COMMAND ----------

# =========================
# 3. Prepare Output Locations
# =========================

print("\n=== Preparing Report Output Locations ===")

# Create the report volume if it does not yet exist,
# then create the stage-specific report directory.

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"`{CATALOG_NAME}`.gold.`{REPORT_VOLUME_NAME}`"
)

dbutils.fs.mkdirs(REPORT_OUTPUT_PATH)

print("✓ Unity Catalog output path ready:")
print(f"  {REPORT_OUTPUT_PATH}")


# COMMAND ----------

# =========================
# 4. Load Gold Tables
# =========================

print("\n=== Loading Gold Views ===")

gold_data = {}

# store_performance consolidates the former store_sales_summary,
# store_revenue_ranking, and store_product_diversity tables.
gold_views_to_load = [
    "store_performance",
    "top_products",
    "monthly_store_trends",
    "product_performance",
]

for view_name in gold_views_to_load:

    table_name = GOLD_VIEWS[view_name]

    df = spark.table(table_name)

    count = df.count()

    print(
        f"✓ {view_name:28s} "
        f"{count:>6,} rows"
    )

    gold_data[view_name] = df.toPandas()


# COMMAND ----------

# =========================
# 5. Export Gold Data to CSV
# =========================

print("\n=== Exporting CSV Reports ===")

csv_files = {}

for dataset_name, pandas_df in gold_data.items():

    volume_file = (
        f"{REPORT_OUTPUT_PATH}/{dataset_name}.csv"
    )

    pandas_df.to_csv(
        volume_file,
        index=False,
    )

    csv_files[dataset_name] = volume_file

    print(
        f"✓ {dataset_name}.csv"
    )


# COMMAND ----------

# =========================
# 6. Prepare Report Data
# =========================

# store_performance consolidates the former store_sales_summary,
# store_revenue_ranking, and store_product_diversity tables.
# The downstream code uses these as separate variables but they all
# read from the same view.
store_performance_df = gold_data[
    "store_performance"
].copy()

top_products = gold_data[
    "top_products"
].copy()

monthly_sales = gold_data[
    "monthly_store_trends"
].copy()

store_sales = store_performance_df
store_rankings = store_performance_df
store_diversity = store_performance_df


# COMMAND ----------

# =========================
# 7. Calculate Summary Metrics
# =========================

highest_revenue_store = (
    store_sales
    .sort_values(
        ["total_revenue", "store_id"],
        ascending=[False, True],
    )
    .iloc[0]
)

highest_transaction_store = (
    store_sales
    .sort_values(
        ["transaction_count", "store_id"],
        ascending=[False, True],
    )
    .iloc[0]
)

most_diverse_store = (
    store_diversity
    .sort_values(
        ["unique_products_sold", "store_id"],
        ascending=[False, True],
    )
    .iloc[0]
)

chain_total_revenue = float(
    store_sales["total_revenue"].sum()
)

chain_transaction_count = int(
    store_sales["transaction_count"].sum()
)

chain_quantity_sold = int(
    store_sales["total_quantity_sold"].sum()
)

chain_average_transaction_value = (
    chain_total_revenue / chain_transaction_count
    if chain_transaction_count > 0
    else 0.0
)

highest_revenue_store_name = str(
    highest_revenue_store["store_name"]
)

highest_revenue_store_revenue = float(
    highest_revenue_store["total_revenue"]
)

highest_transaction_store_name = str(
    highest_transaction_store["store_name"]
)

highest_transaction_count = int(
    highest_transaction_store["transaction_count"]
)

most_diverse_store_name = str(
    most_diverse_store["store_name"]
)

most_diverse_unique_products = int(
    most_diverse_store["unique_products_sold"]
)


# COMMAND ----------

# =========================
# 8. Create Summary CSV
# =========================

summary_rows = [
    {
        "metric": "chain_total_revenue",
        "value": chain_total_revenue,
    },
    {
        "metric": "chain_transaction_count",
        "value": chain_transaction_count,
    },
    {
        "metric": "chain_quantity_sold",
        "value": chain_quantity_sold,
    },
    {
        "metric": "chain_average_transaction_value",
        "value": round(
            chain_average_transaction_value,
            2,
        ),
    },
    {
        "metric": "highest_revenue_store",
        "value": highest_revenue_store_name,
    },
    {
        "metric": "highest_revenue_store_revenue",
        "value": highest_revenue_store_revenue,
    },
    {
        "metric": "highest_transaction_count_store",
        "value": highest_transaction_store_name,
    },
    {
        "metric": "highest_transaction_count",
        "value": highest_transaction_count,
    },
    {
        "metric": "most_diverse_store",
        "value": most_diverse_store_name,
    },
    {
        "metric": "most_diverse_store_unique_products",
        "value": most_diverse_unique_products,
    },
    {
        "metric": "profitability_status",
        "value": (
            "True profitability not calculable because "
            "unit cost is absent from source data."
        ),
    },
]

summary_df = pd.DataFrame(summary_rows)

summary_volume_file = (
    f"{REPORT_OUTPUT_PATH}/sales_summary.csv"
)

summary_df.to_csv(
    summary_volume_file,
    index=False,
)


csv_files["sales_summary"] = summary_volume_file

print("✓ sales_summary.csv")


# COMMAND ----------

# =========================
# 9. Chart Utility
# =========================

def figure_to_base64_png(figure) -> str:
    """
    Convert a matplotlib figure to a base64 PNG string.
    This keeps the HTML report self-contained.
    """
    buffer = BytesIO()

    figure.savefig(
        buffer,
        format="png",
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(figure)

    buffer.seek(0)

    return base64.b64encode(
        buffer.read()
    ).decode("utf-8")


# COMMAND ----------

# =========================
# 10. Revenue by Store Chart
# =========================

revenue_plot = (
    store_sales
    .sort_values(
        "total_revenue",
        ascending=True,
    )
)

revenue_chart = plt.figure(
    figsize=(9, 5)
)

plt.barh(
    revenue_plot["store_name"],
    revenue_plot["total_revenue"],
)

plt.xlabel("Revenue")
plt.ylabel("Store")
plt.title("Total Revenue by Store")

plt.tight_layout()

revenue_chart_base64 = figure_to_base64_png(
    revenue_chart
)


# COMMAND ----------

# =========================
# 11. Top Products Chart
# =========================

products_plot = (
    top_products
    .sort_values(
        "total_quantity_sold",
        ascending=True,
    )
)

products_chart = plt.figure(
    figsize=(9, 5)
)

plt.barh(
    products_plot["product_name"],
    products_plot["total_quantity_sold"],
)

plt.xlabel("Quantity Sold")
plt.ylabel("Product")
plt.title("Top 5 Products by Quantity Sold")

plt.tight_layout()

products_chart_base64 = figure_to_base64_png(
    products_chart
)


# COMMAND ----------

# =========================
# 12. Monthly Chain Revenue Chart
# =========================

monthly_chain = (
    monthly_sales
    .groupby(
        "month_start",
        as_index=False,
    )["monthly_revenue"]
    .sum()
)

monthly_chain["month_start"] = pd.to_datetime(
    monthly_chain["month_start"]
)

monthly_chain = monthly_chain.sort_values(
    "month_start"
)

monthly_chart = plt.figure(
    figsize=(10, 5)
)

plt.plot(
    monthly_chain["month_start"],
    monthly_chain["monthly_revenue"],
    marker="o",
)

plt.xlabel("Month")
plt.ylabel("Revenue")
plt.title("Monthly Chain Revenue Trend")

plt.xticks(
    rotation=45,
)

plt.tight_layout()

monthly_chart_base64 = figure_to_base64_png(
    monthly_chart
)


# COMMAND ----------

# =========================
# 13. HTML Helpers
# =========================

def format_currency(value) -> str:
    """Format a numeric value as INR."""
    return f"₹{float(value):,.2f}"


def safe_html(value) -> str:
    """Escape a value before embedding it into HTML."""
    return html.escape(str(value))


# COMMAND ----------

# =========================
# 14. Prepare HTML Tables
# =========================

store_sales_display = (
    store_sales[
        [
            "store_id",
            "store_name",
            "location",
            "transaction_count",
            "total_quantity_sold",
            "total_revenue",
            "average_transaction_value",
            "unique_products_sold",
        ]
    ]
    .copy()
)

store_sales_display["total_revenue"] = (
    store_sales_display["total_revenue"]
    .map(format_currency)
)

store_sales_display[
    "average_transaction_value"
] = (
    store_sales_display[
        "average_transaction_value"
    ]
    .map(format_currency)
)


top_products_display = (
    top_products[
        [
            "quantity_rank",
            "product_id",
            "product_name",
            "category",
            "total_quantity_sold",
            "total_revenue",
            "transaction_count",
        ]
    ]
    .copy()
)

top_products_display["total_revenue"] = (
    top_products_display["total_revenue"]
    .map(format_currency)
)


rankings_display = (
    store_rankings[
        [
            "store_id",
            "store_name",
            "total_revenue",
            "transaction_count",
            "revenue_rank",
            "transaction_count_rank",
        ]
    ]
    .copy()
)

rankings_display["total_revenue"] = (
    rankings_display["total_revenue"]
    .map(format_currency)
)


diversity_display = (
    store_diversity[
        [
            "store_id",
            "store_name",
            "location",
            "unique_products_sold",
            "unique_categories_sold",
        ]
    ]
    .copy()
)


monthly_display = monthly_chain[
    [
        "month_start",
        "monthly_revenue",
    ]
].copy()

monthly_display["month"] = (
    monthly_display["month_start"]
    .dt.strftime("%Y-%m")
)

monthly_display["revenue"] = (
    monthly_display["monthly_revenue"]
    .map(format_currency)
)

monthly_display = monthly_display[
    [
        "month",
        "revenue",
    ]
]


# COMMAND ----------

# =========================
# 15. Build HTML Report
# =========================

report_html = f"""
<!DOCTYPE html>
<html lang="en">

<head>
<meta charset="UTF-8">

<title>MegaMart Sales Summary Report</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 40px;
    line-height: 1.5;
}}

h1, h2 {{
    margin-top: 30px;
}}

.metric-grid {{
    display: grid;
    grid-template-columns:
        repeat(4, minmax(180px, 1fr));
    gap: 15px;
    margin: 20px 0;
}}

.metric {{
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 16px;
}}

.metric-label {{
    font-size: 13px;
    font-weight: bold;
}}

.metric-value {{
    font-size: 22px;
    margin-top: 8px;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    margin: 15px 0 30px 0;
}}

th, td {{
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
}}

th {{
    font-weight: bold;
}}

.chart {{
    width: 100%;
    max-width: 1000px;
    margin: 20px 0;
}}

.note {{
    border-left: 4px solid #777;
    padding: 10px 15px;
    margin: 20px 0;
}}

.footer {{
    margin-top: 40px;
    font-size: 12px;
}}

</style>

</head>

<body>

<h1>MegaMart Sales Summary Report</h1>

<p>
Pipeline: {safe_html(PIPELINE_NAME)}<br>
Stage: {safe_html(STAGE_NAME)}<br>
Run ID: {safe_html(RUN_ID)}<br>
Generated: {safe_html(START_TIME.isoformat())}
</p>


<div class="metric-grid">

<div class="metric">
<div class="metric-label">Chain Revenue</div>
<div class="metric-value">
{safe_html(format_currency(chain_total_revenue))}
</div>
</div>

<div class="metric">
<div class="metric-label">Transactions</div>
<div class="metric-value">
{safe_html(f"{chain_transaction_count:,}")}
</div>
</div>

<div class="metric">
<div class="metric-label">Units Sold</div>
<div class="metric-value">
{safe_html(f"{chain_quantity_sold:,}")}
</div>
</div>

<div class="metric">
<div class="metric-label">Average Transaction Value</div>
<div class="metric-value">
{safe_html(format_currency(chain_average_transaction_value))}
</div>
</div>

</div>


<h2>Key Observations</h2>

<ul>

<li>
Highest revenue store:
<strong>{safe_html(highest_revenue_store_name)}</strong>
with
<strong>{safe_html(format_currency(
    highest_revenue_store_revenue
))}</strong>.
</li>

<li>
Highest transaction count:
<strong>{safe_html(highest_transaction_store_name)}</strong>
with
<strong>{safe_html(f"{highest_transaction_count:,}")}</strong>
transactions.
</li>

<li>
Most diverse store by number of unique products sold:
<strong>{safe_html(most_diverse_store_name)}</strong>
with
<strong>{safe_html(most_diverse_unique_products)}</strong>
products.
</li>

</ul>


<div class="note">

<strong>Profitability limitation:</strong>

True product profitability is not calculated because the generated
source data contains selling price/revenue but does not contain
product unit cost. Revenue contribution is reported instead.

</div>


<h2>Revenue by Store</h2>

<img
class="chart"
src="data:image/png;base64,{revenue_chart_base64}"
alt="Total Revenue by Store"
/>


<h2>Top 5 Products by Quantity Sold</h2>

<img
class="chart"
src="data:image/png;base64,{products_chart_base64}"
alt="Top 5 Products by Quantity Sold"
/>


<h2>Monthly Chain Revenue Trend</h2>

<img
class="chart"
src="data:image/png;base64,{monthly_chart_base64}"
alt="Monthly Chain Revenue Trend"
/>


<h2>Store Sales Summary</h2>

{store_sales_display.to_html(
    index=False,
    border=0,
)}


<h2>Top 5 Products</h2>

{top_products_display.to_html(
    index=False,
    border=0,
)}


<h2>Store Rankings</h2>

{rankings_display.to_html(
    index=False,
    border=0,
)}


<h2>Store Product Diversity</h2>

{diversity_display.to_html(
    index=False,
    border=0,
)}


<h2>Monthly Chain Revenue</h2>

{monthly_display.to_html(
    index=False,
    border=0,
)}


<h2>Output Files</h2>

<table>

<tr>
<th>File</th>
<th>Purpose</th>
</tr>

{''.join(
    f"<tr>"
    f"<td>{safe_html(Path(path).name)}</td>"
    f"<td>Sales analysis output</td>"
    f"</tr>"
    for path in csv_files.values()
)}

<tr>
<td>sales_summary_report.html</td>
<td>Business-facing visual report</td>
</tr>

</table>


<div class="footer">
Generated by the MegaMart Databricks pipeline.
</div>

</body>

</html>
"""


# COMMAND ----------

# =========================
# 16. Write HTML Report
# =========================


html_volume_file = (
    f"{REPORT_OUTPUT_PATH}/sales_summary_report.html"
)

with open(
    html_volume_file,
    "w",
    encoding="utf-8",
) as html_file:

    html_file.write(report_html)

print(
    "✓ sales_summary_report.html"
)


# COMMAND ----------

# =========================
# 17. Report Output Summary
# =========================

print("\n=== REPORT OUTPUTS ===")

for item in dbutils.fs.ls(
    REPORT_OUTPUT_PATH
):
    print(
        f"✓ {item.name:40s} "
        f"{item.size:,} bytes"
    )


# COMMAND ----------

# =========================
# 18. Verify Volume Outputs
# =========================

print("\n=== UNITY CATALOG REPORT OUTPUTS ===")

volume_files = dbutils.fs.ls(
    REPORT_OUTPUT_PATH
)

for item in volume_files:

    print(
        f"✓ {item.name:40s} "
        f"{item.size:,} bytes"
    )


# COMMAND ----------

# =========================
# 19. Audit Pipeline Run
# =========================

END_TIME = datetime.now(timezone.utc)

records_processed = int(
    len(store_sales)
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
# 20. Final Status
# =========================

print("\n========================================")
print("Sales reporting completed successfully.")
print("========================================")

print(f"Run ID: {RUN_ID}")

print("\nUnity Catalog report location:")
print(REPORT_OUTPUT_PATH)

print("\nCSV outputs:")

for path in csv_files.values():
    print(f"✓ {path}")

print("\nHTML report:")
print(f"✓ {html_volume_file}")

print("\nAudit table updated:")
print(f"✓ {PIPELINE_RUNS_TABLE}")


# COMMAND ----------

# =========================
# 21. HTML Preview
# =========================

displayHTML(report_html)
