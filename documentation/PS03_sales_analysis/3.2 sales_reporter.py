# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Subproject: Sales Analysis
File: sales_reporter.py

Purpose
-------
Execute the six sales-analysis SQL queries, format the results,
export each result set to a CSV file, generate visualizations, and
produce a consolidated summary report with key insights.

Deliverables produced in the output directory:
    1_store_revenue_2024.csv
    2_top5_products_by_quantity.csv
    3_store_rankings.csv
    4_most_profitable_product_per_store.csv
    5_monthly_sales_trend_by_store.csv
    6_store_product_diversity.csv
    sales_summary_report.txt
    chart_*.png   (visualization images)

Run inside a Databricks notebook (PySpark) where `spark` is available.
"""

# COMMAND ----------

# =========================
# 1. Imports & Configuration
# =========================

import os
import textwrap
from datetime import datetime, timezone

import pandas as pd

# matplotlib is available in Databricks runtime.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# Output directory: a results folder next to this script.
SCRIPT_DIR = "/Workspace/Users/akharejayesh@gmail.com/MegaMart-Capstone-1/documentation/PS03_sales_analysis"
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CATALOG = "megamart_dev"

print(f"Output directory : {OUTPUT_DIR}")
print(f"Run timestamp    : {datetime.now(timezone.utc).isoformat()}")


# COMMAND ----------

# =========================
# 2. SQL Query Definitions
# =========================
# Each entry maps a short key to (display title, SQL text, output csv name).

QUERIES = {
    "q1_store_revenue": (
        "Q1. Store with the highest total revenue in 2024",
        f"""
        SELECT
            s.store_id,
            s.store_name,
            s.location,
            ROUND(SUM(st.total_amount), 2) AS total_revenue,
            COUNT(DISTINCT st.transaction_id) AS transaction_count,
            ROUND(AVG(st.total_amount), 2) AS avg_transaction_value,
            DENSE_RANK() OVER (ORDER BY SUM(st.total_amount) DESC) AS revenue_rank
        FROM {CATALOG}.silver.sales_transactions st
        INNER JOIN {CATALOG}.silver.stores s
            ON st.store_id = s.store_id
        WHERE YEAR(st.sale_date) = 2024
        GROUP BY s.store_id, s.store_name, s.location
        ORDER BY total_revenue DESC
        """,
        "1_store_revenue_2024.csv",
    ),
    "q2_top5_products": (
        "Q2. Top 5 best-selling products by quantity",
        f"""
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            SUM(st.quantity_sold) AS total_quantity_sold,
            ROUND(SUM(st.total_amount), 2) AS total_revenue,
            COUNT(DISTINCT st.transaction_id) AS transaction_count,
            ROW_NUMBER() OVER (ORDER BY SUM(st.quantity_sold) DESC, SUM(st.total_amount) DESC) AS quantity_rank
        FROM {CATALOG}.silver.sales_transactions st
        INNER JOIN {CATALOG}.silver.products p
            ON st.product_id = p.product_id
        WHERE YEAR(st.sale_date) = 2024
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_quantity_sold DESC
        LIMIT 5
        """,
        "2_top5_products_by_quantity.csv",
    ),
    "q3_store_rankings": (
        "Q3. Store ranking by revenue and transaction count",
        f"""
        SELECT
            s.store_id,
            s.store_name,
            s.location,
            ROUND(SUM(st.total_amount), 2) AS total_revenue,
            COUNT(DISTINCT st.transaction_id) AS transaction_count,
            ROUND(AVG(st.total_amount), 2) AS avg_transaction_value,
            DENSE_RANK() OVER (ORDER BY SUM(st.total_amount) DESC) AS revenue_rank,
            DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT st.transaction_id) DESC) AS transaction_count_rank
        FROM {CATALOG}.silver.sales_transactions st
        INNER JOIN {CATALOG}.silver.stores s
            ON st.store_id = s.store_id
        WHERE YEAR(st.sale_date) = 2024
        GROUP BY s.store_id, s.store_name, s.location
        ORDER BY revenue_rank, transaction_count_rank
        """,
        "3_store_rankings.csv",
    ),
    "q4_profitable_per_store": (
        "Q4. Most profitable (by revenue) products per store",
        f"""
        WITH store_product_sales AS (
            SELECT
                st.store_id,
                s.store_name,
                p.product_id,
                p.product_name,
                p.category,
                SUM(st.quantity_sold) AS total_quantity_sold,
                ROUND(SUM(st.total_amount), 2) AS total_revenue,
                COUNT(DISTINCT st.transaction_id) AS transaction_count,
                ROUND(AVG(st.unit_price), 2) AS avg_selling_price
            FROM {CATALOG}.silver.sales_transactions st
            INNER JOIN {CATALOG}.silver.stores s   ON st.store_id   = s.store_id
            INNER JOIN {CATALOG}.silver.products p ON st.product_id = p.product_id
            WHERE YEAR(st.sale_date) = 2024
            GROUP BY st.store_id, s.store_name, p.product_id, p.product_name, p.category
        ),
        store_totals AS (
            SELECT store_id, SUM(total_revenue) AS store_total_revenue
            FROM store_product_sales
            GROUP BY store_id
        )
        SELECT
            sps.store_id,
            sps.store_name,
            sps.product_id,
            sps.product_name,
            sps.category,
            sps.total_quantity_sold,
            sps.transaction_count,
            sps.avg_selling_price,
            sps.total_revenue,
            ROUND((sps.total_revenue / NULLIF(st.store_total_revenue, 0)) * 100, 2) AS revenue_share_pct,
            DENSE_RANK() OVER (
                PARTITION BY sps.store_id
                ORDER BY sps.total_revenue DESC, sps.product_id
            ) AS revenue_rank_within_store
        FROM store_product_sales sps
        INNER JOIN store_totals st
            ON sps.store_id = st.store_id
        ORDER BY sps.store_id, revenue_rank_within_store
        """,
        "4_most_profitable_product_per_store.csv",
    ),
    "q5_monthly_trend": (
        "Q5. Month-on-month sales trend for each store",
        f"""
        WITH monthly_sales AS (
            SELECT
                s.store_id,
                s.store_name,
                DATE_TRUNC('MONTH', st.sale_date) AS month_start,
                SUM(st.quantity_sold) AS monthly_quantity_sold,
                ROUND(SUM(st.total_amount), 2) AS monthly_revenue,
                COUNT(DISTINCT st.transaction_id) AS monthly_transaction_count
            FROM {CATALOG}.silver.sales_transactions st
            INNER JOIN {CATALOG}.silver.stores s
                ON st.store_id = s.store_id
            WHERE YEAR(st.sale_date) = 2024
            GROUP BY s.store_id, s.store_name, DATE_TRUNC('MONTH', st.sale_date)
        ),
        with_previous AS (
            SELECT
                *,
                LAG(monthly_revenue) OVER (
                    PARTITION BY store_id ORDER BY month_start
                ) AS previous_month_revenue
            FROM monthly_sales
        )
        SELECT
            store_id,
            store_name,
            DATE_FORMAT(month_start, 'yyyy-MM') AS year_month,
            monthly_quantity_sold,
            monthly_transaction_count,
            monthly_revenue,
            previous_month_revenue,
            ROUND(monthly_revenue - COALESCE(previous_month_revenue, 0), 2) AS mom_revenue_change,
            CASE
                WHEN previous_month_revenue IS NULL THEN NULL
                WHEN previous_month_revenue = 0 AND monthly_revenue > 0 THEN NULL
                ELSE ROUND(((monthly_revenue - previous_month_revenue) / previous_month_revenue) * 100, 2)
            END AS mom_revenue_growth_pct
        FROM with_previous
        ORDER BY store_id, month_start
        """,
        "5_monthly_sales_trend_by_store.csv",
    ),
    "q6_diversity": (
        "Q6. Store with the most diverse product sales",
        f"""
        SELECT
            s.store_id,
            s.store_name,
            s.location,
            COUNT(DISTINCT st.product_id) AS unique_products_sold,
            COUNT(DISTINCT p.category) AS unique_categories_sold,
            ROUND(SUM(st.quantity_sold), 0) AS total_quantity_sold,
            DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT st.product_id) DESC) AS diversity_rank
        FROM {CATALOG}.silver.stores s
        LEFT JOIN {CATALOG}.silver.sales_transactions st
            ON s.store_id = st.store_id
            AND YEAR(st.sale_date) = 2024
        LEFT JOIN {CATALOG}.silver.products p
            ON st.product_id = p.product_id
        GROUP BY s.store_id, s.store_name, s.location
        ORDER BY unique_products_sold DESC, unique_categories_sold DESC
        """,
        "6_store_product_diversity.csv",
    ),
}


# COMMAND ----------

# =========================
# 3. Execute Queries & Export CSVs
# =========================

results = {}   # key -> pandas DataFrame

for key, (title, sql, csv_name) in QUERIES.items():
    print(f"\n{'=' * 70}")
    print(title)
    print("=" * 70)

    df = spark.sql(sql).toPandas()
    results[key] = df

    # Export CSV
    csv_path = os.path.join(OUTPUT_DIR, csv_name)
    df.to_csv(csv_path, index=False)
    print(f"Rows: {len(df)}  |  CSV: {csv_path}")
    display(df)


# COMMAND ----------

# =========================
# 4. Visualizations
# =========================

print("\n\nGenerating visualizations...")


# --- Chart 1: Total revenue by store (bar) ---
df1 = results["q1_store_revenue"]
fig, ax = plt.subplots(figsize=(8, 5))
ax.barh(df1["store_name"], df1["total_revenue"], color="steelblue")
ax.set_xlabel("Total Revenue ($)")
ax.set_title("Q1. Total Revenue by Store (2024)")
ax.invert_yaxis()
fig.tight_layout()
chart1_path = os.path.join(OUTPUT_DIR, "chart_1_revenue_by_store.png")
fig.savefig(chart1_path, dpi=120)
plt.close(fig)
print(f"  saved {chart1_path}")


# --- Chart 2: Top 5 products by quantity (bar) ---
df2 = results["q2_top5_products"]
fig, ax = plt.subplots(figsize=(8, 5))
ax.barh(df2["product_name"], df2["total_quantity_sold"], color="seagreen")
ax.set_xlabel("Total Quantity Sold")
ax.set_title("Q2. Top 5 Best-Selling Products by Quantity (2024)")
ax.invert_yaxis()
fig.tight_layout()
chart2_path = os.path.join(OUTPUT_DIR, "chart_2_top5_products.png")
fig.savefig(chart2_path, dpi=120)
plt.close(fig)
print(f"  saved {chart2_path}")


# --- Chart 3: Store rankings - revenue vs transaction count ---
df3 = results["q3_store_rankings"]
fig, ax = plt.subplots(figsize=(9, 5))
x = range(len(df3))
width = 0.35
ax.bar([i - width / 2 for i in x], df3["total_revenue"], width, label="Total Revenue", color="steelblue")
ax2 = ax.twinx()
ax2.bar([i + width / 2 for i in x], df3["transaction_count"], width, label="Transaction Count", color="coral")
ax.set_xticks(list(x))
ax.set_xticklabels(df3["store_name"], rotation=30, ha="right")
ax.set_ylabel("Total Revenue ($)")
ax2.set_ylabel("Transaction Count")
ax.set_title("Q3. Store Rankings: Revenue vs Transaction Count (2024)")
# Combined legend across BOTH axes: revenue bars live on ax, transaction
# bars on the twin axis ax2 — neither axis alone knows about the other's bars.
handles1, labels1 = ax.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()
ax.legend(handles1 + handles2, labels1 + labels2, loc="upper left", fontsize=9)
fig.tight_layout()
chart3_path = os.path.join(OUTPUT_DIR, "chart_3_store_rankings.png")
fig.savefig(chart3_path, dpi=120)
plt.close(fig)
print(f"  saved {chart3_path}")


# --- Chart 4: Top 3 products per store by revenue (horizontal grouped bars) ---
# NOTE: Every store's #1 product is the same (Basmati Rice 5kg), so a
# top-1-only chart renders five near-identical bars and carries little
# information. Instead, plot the top 3 products per store — one bar per
# (store, product) — labelled with revenue and the store revenue share.
df4 = results["q4_profitable_per_store"]
top3_per_store = (
    df4[df4["revenue_rank_within_store"] <= 3]
    .sort_values(["store_id", "revenue_rank_within_store"])
    .reset_index(drop=True)
    .copy()
)
# Spark DECIMAL columns arrive as decimal.Decimal after toPandas();
# Decimal does not support arithmetic with float (e.g. max() * 0.01),
# so cast to float before using the values in the chart.
top3_per_store["total_revenue"] = top3_per_store["total_revenue"].astype(float)
top3_per_store["revenue_share_pct"] = top3_per_store["revenue_share_pct"].astype(float)
rank_colors = {1: "#6a51a3", 2: "#9e9ac8", 3: "#cbc9e2"}

fig, ax = plt.subplots(figsize=(10, 8))
y_pos = range(len(top3_per_store))
labels = [
    f"{r['store_name']}  -  #{int(r['revenue_rank_within_store'])} {r['product_name']}"
    for _, r in top3_per_store.iterrows()
]
colors = [rank_colors[int(r)] for r in top3_per_store["revenue_rank_within_store"]]
ax.barh(y_pos, top3_per_store["total_revenue"], color=colors)
ax.set_yticks(list(y_pos))
ax.set_yticklabels(labels, fontsize=8)
ax.invert_yaxis()  # rank #1 of the first store at the top
ax.set_xlabel("Product Revenue ($)")
ax.set_title("Q4. Top 3 Products per Store by Revenue (2024)")
# Annotate revenue and revenue share at the end of each bar
for i, r in top3_per_store.iterrows():
    ax.text(
        r["total_revenue"] + top3_per_store["total_revenue"].max() * 0.01,
        i,
        f"${r['total_revenue']:,.0f} ({r['revenue_share_pct']:.1f}%)",
        va="center",
        fontsize=7,
    )
ax.set_xlim(0, top3_per_store["total_revenue"].max() * 1.30)  # headroom for labels
fig.tight_layout()
chart4_path = os.path.join(OUTPUT_DIR, "chart_4_top_product_per_store.png")
fig.savefig(chart4_path, dpi=120)
plt.close(fig)
print(f"  saved {chart4_path}")


# --- Chart 4b: Product revenue share per store (5 pie charts, one image) ---
# One pie per store showing the store's product revenue mix (its top 5
# products + "Others" = the remaining products). Colors are keyed to product
# name (top 5 chain-wide by revenue) and stay consistent across all pies,
# so the store mixes can be compared visually.
df4b = results["q4_profitable_per_store"].copy()
df4b["total_revenue"] = df4b["total_revenue"].astype(float)

top5_names = (
    df4b.groupby("product_name")["total_revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
    .tolist()
)
color_map = dict(
    zip(top5_names, ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f"])
)
color_map["Others"] = "#b0b0b0"

stores = df4b[["store_id", "store_name"]].drop_duplicates().sort_values("store_id")

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
axes = axes.flatten()
for idx, (store_id, store_name) in enumerate(stores.itertuples(index=False)):
    ax = axes[idx]
    sdf = (
        df4b[df4b["store_id"] == store_id]
        .sort_values("total_revenue", ascending=False)
    )
    top5 = sdf.head(5)
    others_rev = sdf.iloc[5:]["total_revenue"].sum()
    labels = top5["product_name"].tolist()
    sizes = top5["total_revenue"].tolist()
    pie_colors = [color_map.get(n, "#b0b0b0") for n in labels]
    if others_rev > 0:
        labels.append("Others")
        sizes.append(others_rev)
        pie_colors.append(color_map["Others"])
    ax.pie(
        sizes,
        labels=None,
        colors=pie_colors,
        autopct="%1.1f%%",
        startangle=90,
        counterclock=False,
        pctdistance=0.75,
        textprops={"fontsize": 8},
    )
    ax.set_title(
        f"{store_name}\n(total ${sdf['total_revenue'].sum():,.0f})", fontsize=10
    )

# 6th panel: shared legend (5 pies fill 2x3 grid's first 5 panels)
legend_handles = [
    plt.Rectangle((0, 0), 1, 1, color=color_map[n]) for n in top5_names + ["Others"]
]
axes[5].legend(
    legend_handles,
    top5_names + ["Others"],
    loc="center",
    fontsize=9,
    title="Products (top 5 chain-wide by revenue)",
)
axes[5].axis("off")

fig.suptitle("Q4. Product Revenue Share per Store (2024)", fontsize=14)
fig.tight_layout(rect=[0, 0, 1, 0.96])
chart4b_path = os.path.join(OUTPUT_DIR, "chart_4b_product_share_by_store.png")
fig.savefig(chart4b_path, dpi=120)
plt.close(fig)
print(f"  saved {chart4b_path}")


# --- Chart 5: Monthly revenue trend by store (line) ---
df5 = results["q5_monthly_trend"]
fig, ax = plt.subplots(figsize=(10, 5))
for store_name, grp in df5.groupby("store_name"):
    ax.plot(grp["year_month"], grp["monthly_revenue"], marker="o", label=store_name)
ax.set_xlabel("Month")
ax.set_ylabel("Monthly Revenue ($)")
ax.set_title("Q5. Month-on-Month Sales Trend by Store (2024)")
plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
ax.legend(fontsize=7, loc="best")
fig.tight_layout()
chart5_path = os.path.join(OUTPUT_DIR, "chart_5_monthly_trend.png")
fig.savefig(chart5_path, dpi=120)
plt.close(fig)
print(f"  saved {chart5_path}")


# --- Chart 6: Store product diversity (pie) ---
# All 5 stores sell the same 14 products across 7 categories (diversity
# is tied), so a pie of "unique products" would be five identical slices.
# Instead, each slice is a store's share of chain-wide units sold, labelled
# with its assortment (unique products / categories), so both the volume
# mix and the diversity outcome are visible in one chart.
df6 = results["q6_diversity"].copy()

df6 = df6.sort_values("store_id")
store_colors = dict(
    zip(df6["store_name"], ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f"])
)

fig, ax = plt.subplots(figsize=(10, 7))
labels = [
    f"{r['store_name']}\n({int(r['unique_products_sold'])} products, "
    f"{int(r['unique_categories_sold'])} categories)"
    for _, r in df6.iterrows()
]
ax.pie(
    df6["total_quantity_sold"],
    labels=labels,
    colors=[store_colors[n] for n in df6["store_name"]],
    autopct="%1.1f%%",
    startangle=90,
    counterclock=False,
    textprops={"fontsize": 8},
)
ax.set_title(
    "Q6. Store Product Diversity (2024)\n"
    "Share of chain-wide units sold - all stores carry the identical assortment",
    fontsize=11,
)
fig.tight_layout()
chart6_path = os.path.join(OUTPUT_DIR, "chart_6_diversity.png")
fig.savefig(chart6_path, dpi=120)
plt.close(fig)
print(f"  saved {chart6_path}")


# COMMAND ----------

# =========================
# 5. Summary Report with Key Insights
# =========================

def _fmt_money(v):
    """Format a numeric value as currency."""
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _fmt_num(v):
    """Format an integer/float with thousands separator."""
    try:
        return f"{float(v):,.0f}"
    except (TypeError, ValueError):
        return str(v)


df1 = results["q1_store_revenue"]
df2 = results["q2_top5_products"]
df3 = results["q3_store_rankings"]
df4 = results["q4_profitable_per_store"]
df5 = results["q5_monthly_trend"]
df6 = results["q6_diversity"]

# Derive key insights
top_store_row = df1.iloc[0]
top_store_name = top_store_row["store_name"]
top_store_rev = top_store_row["total_revenue"]

total_revenue_all = df1["total_revenue"].sum()
total_tx_all = df1["transaction_count"].sum()

top_product_row = df2.iloc[0]
top_product_name = top_product_row["product_name"]
top_product_qty = top_product_row["total_quantity_sold"]

# Highest growth month overall
growth_df = df5.dropna(subset=["mom_revenue_growth_pct"])
if not growth_df.empty:
    best_growth = growth_df.loc[growth_df["mom_revenue_growth_pct"].idxmax()]
    worst_growth = growth_df.loc[growth_df["mom_revenue_growth_pct"].idxmin()]
else:
    best_growth = worst_growth = None

most_diverse_row = df6.iloc[0]
least_diverse_row = df6.iloc[-1]

# Top product per store summary
top_per_store = df4[df4["revenue_rank_within_store"] == 1]

report_lines = []
report_lines.append("=" * 72)
report_lines.append("MEGAMART SALES ANALYSIS - SUMMARY REPORT (2024)")
report_lines.append("=" * 72)
report_lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
report_lines.append(f"Data source: {CATALOG}.silver.* (sales_transactions, stores, products)")
report_lines.append(f"Output directory: {OUTPUT_DIR}")
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("OVERVIEW")
report_lines.append("-" * 72)
report_lines.append(f"Total revenue (all stores):    {_fmt_money(total_revenue_all)}")
report_lines.append(f"Total transactions (all stores): {_fmt_num(total_tx_all)}")
report_lines.append(f"Number of stores:             {len(df1)}")
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q1. HIGHEST REVENUE STORE")
report_lines.append("-" * 72)
report_lines.append(f"Top store: {top_store_name} ({top_store_row['location']})")
report_lines.append(f"  Revenue:            {_fmt_money(top_store_rev)}")
report_lines.append(f"  Transactions:       {_fmt_num(top_store_row['transaction_count'])}")
report_lines.append(f"  Avg transaction:    {_fmt_money(top_store_row['avg_transaction_value'])}")
report_lines.append("")
report_lines.append("Full store revenue ranking:")
for _, r in df1.iterrows():
    report_lines.append(
        f"  Rank {r['revenue_rank']:.0f}  {r['store_name']:20s}  "
        f"{_fmt_money(r['total_revenue']):>14s}  txns={_fmt_num(r['transaction_count'])}"
    )
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q2. TOP 5 BEST-SELLING PRODUCTS BY QUANTITY")
report_lines.append("-" * 72)
for _, r in df2.iterrows():
    report_lines.append(
        f"  #{r['quantity_rank']:.0f}  {r['product_name']:25s}  "
        f"qty={_fmt_num(r['total_quantity_sold']):>6s}  rev={_fmt_money(r['total_revenue'])}"
    )
report_lines.append(f"Best seller: {top_product_name} ({_fmt_num(top_product_qty)} units)")
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q3. STORE RANKINGS (revenue & transaction count)")
report_lines.append("-" * 72)
for _, r in df3.iterrows():
    report_lines.append(
        f"  {r['store_name']:20s}  rev_rank={r['revenue_rank']:.0f}  "
        f"txn_rank={r['transaction_count_rank']:.0f}  "
        f"rev={_fmt_money(r['total_revenue'])}"
    )
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q4. MOST PROFITABLE (HIGHEST-REVENUE) PRODUCT PER STORE")
report_lines.append("-" * 72)
report_lines.append("NOTE: True profit requires unit cost, which is not in the source data.")
report_lines.append("      Revenue contribution is used as the profitability proxy.")
for _, r in top_per_store.iterrows():
    report_lines.append(
        f"  {r['store_name']:20s} -> {r['product_name']:25s}  "
        f"rev={_fmt_money(r['total_revenue'])}  share={r['revenue_share_pct']:.2f}%"
    )
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q5. MONTH-ON-MONTH SALES TREND")
report_lines.append("-" * 72)
if best_growth is not None:
    report_lines.append(
        f"Largest MoM growth: {best_growth['store_name']} in {best_growth['year_month']} "
        f"({best_growth['mom_revenue_growth_pct']:+.2f}%)"
    )
if worst_growth is not None:
    report_lines.append(
        f"Largest MoM decline: {worst_growth['store_name']} in {worst_growth['year_month']} "
        f"({worst_growth['mom_revenue_growth_pct']:+.2f}%)"
    )
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("Q6. STORE PRODUCT DIVERSITY")
report_lines.append("-" * 72)
for _, r in df6.iterrows():
    report_lines.append(
        f"  Rank {r['diversity_rank']:.0f}  {r['store_name']:20s}  "
        f"products={_fmt_num(r['unique_products_sold']):>3s}  "
        f"categories={_fmt_num(r['unique_categories_sold'])}"
    )
report_lines.append(
    f"Most diverse:  {most_diverse_row['store_name']} "
    f"({_fmt_num(most_diverse_row['unique_products_sold'])} unique products)"
)
report_lines.append(
    f"Least diverse: {least_diverse_row['store_name']} "
    f"({_fmt_num(least_diverse_row['unique_products_sold'])} unique products)"
)
report_lines.append("")

report_lines.append("-" * 72)
report_lines.append("FILES PRODUCED")
report_lines.append("-" * 72)
for _, (_, _, csv_name) in QUERIES.items():
    report_lines.append(f"  {csv_name}")
for c in [
    "chart_1_revenue_by_store.png",
    "chart_2_top5_products.png",
    "chart_3_store_rankings.png",
    "chart_4_top_product_per_store.png",
    "chart_4b_product_share_by_store.png",
    "chart_5_monthly_trend.png",
    "chart_6_diversity.png",
]:
    report_lines.append(f"  {c}")
report_lines.append("  sales_summary_report.txt")
report_lines.append("=" * 72)

report_text = "\n".join(report_lines)
report_path = os.path.join(OUTPUT_DIR, "sales_summary_report.txt")
with open(report_path, "w") as f:
    f.write(report_text)

print("\n" + report_text)
print(f"\nSummary report saved to: {report_path}")
print("\nAll deliverables generated successfully.")
