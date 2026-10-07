# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Subproject: Inventory Optimization
File: inventory_exporter.py

Purpose
-------
Export inventory analysis results from Gold-layer tables/views to CSV files
and generate a consolidated supplier performance scorecard text report.

Deliverables produced in the results/ folder:
    1_reorder_recommendations.csv      — full reorder recommendations (70 rows)
    2_stock_alerts.csv                 — out-of-stock and low-stock products
    3_fast_moving_products.csv         — fastest-moving store/product combos
    4_overstock_candidates.csv         — slow-moving overstock candidates
    5_supplier_scorecard.csv           — supplier delivery performance
    6_store_inventory_summary.csv      — store-level inventory health
    supplier_scorecard_report.txt      — formatted text report

Run inside a Databricks notebook (PySpark) where `spark` is available.
Prerequisite: inventory_optimizer.py must have been run first to populate
the gold.reorder_recommendations table.
"""

# COMMAND ----------

# =========================
# 1. Imports & Configuration
# =========================

import os
from datetime import datetime, timezone

import pandas as pd

# Output directory: a results folder next to this script.
SCRIPT_DIR = "/Workspace/Users/akharejayesh@gmail.com/MegaMart-Capstone-1/PS04_inventory_analysis"
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CATALOG = "megamart_dev"

print(f"Output directory : {OUTPUT_DIR}")
print(f"Run timestamp    : {datetime.now(timezone.utc).isoformat()}")


# COMMAND ----------

# =========================
# 2. Query Definitions
# =========================
# Each entry maps a short key to (display title, SQL text, output CSV name).

QUERIES = {
    # --- Q27: Reorder recommendations (full table) ---
    "reorder_recommendations": (
        "Q27. Reorder recommendations (all store/product combinations)",
        f"""
        SELECT
            store_id,
            store_name,
            location,
            product_id,
            product_name,
            category,
            supplier_id,
            supplier_name,
            quantity_on_hand,
            period_units_sold,
            average_daily_demand,
            days_with_sales,
            daily_demand_stddev,
            planning_lead_time_days,
            safety_stock,
            reorder_point,
            recommended_order_quantity,
            supplier_delivery_reliability_pct,
            replenishment_priority,
            recommendation_status,
            service_level_z,
            reorder_method
        FROM {CATALOG}.gold.reorder_recommendations
        ORDER BY
            CASE recommendation_status
                WHEN 'REORDER'    THEN 1
                WHEN 'NO_REORDER' THEN 2
            END,
            CASE replenishment_priority
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH'     THEN 2
                WHEN 'MEDIUM'   THEN 3
                ELSE 4
            END,
            recommended_order_quantity DESC,
            store_id,
            product_id
        """,
        "1_reorder_recommendations.csv",
    ),

    # --- Q22: Stock alerts (out-of-stock and low-stock) ---
    "stock_alerts": (
        "Q22. Out-of-stock and low-stock products (< 50 units)",
        f"""
        SELECT
            store_id,
            store_name,
            location,
            product_id,
            product_name,
            category,
            quantity_on_hand,
            stock_status,
            supplier_id,
            supplier_name
        FROM {CATALOG}.gold.inventory_status
        WHERE stock_status IN ('OUT_OF_STOCK', 'LOW_STOCK')
        ORDER BY
            stock_priority DESC,
            quantity_on_hand ASC,
            store_id,
            product_id
        """,
        "2_stock_alerts.csv",
    ),

    # --- Q23: Fast-moving products ---
    "fast_moving": (
        "Q23. Fastest-moving products (highest sales velocity)",
        f"""
        SELECT
            overall_velocity_rank,
            store_id,
            store_name,
            product_id,
            product_name,
            category,
            total_quantity_sold,
            average_daily_sales,
            quantity_on_hand,
            estimated_days_of_inventory,
            velocity_class
        FROM {CATALOG}.gold.product_performance
        WHERE velocity_class = 'FAST_MOVING'
        ORDER BY overall_velocity_rank
        """,
        "3_fast_moving_products.csv",
    ),

    # --- Q24: Overstock candidates ---
    "overstock": (
        "Q24. Slow-moving overstock candidates",
        f"""
        SELECT
            store_id,
            store_name,
            product_id,
            product_name,
            category,
            total_quantity_sold,
            average_daily_sales,
            quantity_on_hand,
            estimated_days_of_inventory,
            velocity_class
        FROM {CATALOG}.gold.product_performance
        WHERE velocity_class = 'SLOW_MOVING'
          AND quantity_on_hand >= 50
        ORDER BY
            average_daily_sales ASC,
            quantity_on_hand DESC
        """,
        "4_overstock_candidates.csv",
    ),

    # --- Q25 & Q26: Supplier scorecard ---
    "supplier_scorecard": (
        "Q25 & Q26. Supplier performance scorecard",
        f"""
        SELECT
            supplier_id,
            supplier_name,
            total_purchase_orders,
            delivered_orders,
            failed_or_incomplete_orders,
            delivery_reliability_pct,
            average_lead_time_days,
            minimum_lead_time_days,
            maximum_lead_time_days,
            reliability_definition
        FROM {CATALOG}.gold.supplier_scorecard
        ORDER BY
            delivery_reliability_pct DESC,
            average_lead_time_days ASC,
            supplier_id
        """,
        "5_supplier_scorecard.csv",
    ),

    # --- Bonus: Store-level inventory summary ---
    "store_summary": (
        "Store-level inventory health summary",
        f"""
        SELECT
            store_id,
            store_name,
            COUNT(*) AS total_products,
            SUM(CASE WHEN stock_status = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS out_of_stock_products,
            SUM(CASE WHEN stock_status = 'LOW_STOCK'   THEN 1 ELSE 0 END) AS low_stock_products,
            SUM(CASE WHEN stock_status = 'HEALTHY'     THEN 1 ELSE 0 END) AS healthy_stock_products,
            SUM(quantity_on_hand) AS total_quantity_on_hand
        FROM {CATALOG}.gold.inventory_status
        GROUP BY store_id, store_name
        ORDER BY store_id
        """,
        "6_store_inventory_summary.csv",
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

    csv_path = os.path.join(OUTPUT_DIR, csv_name)
    df.to_csv(csv_path, index=False)
    print(f"Rows: {len(df)}  |  CSV: {csv_path}")
    display(df)


# COMMAND ----------

# =========================
# 4. Inventory Optimization Report (covers Q22-Q27)
# =========================
# Generate a comprehensive formatted text report covering all six business
# questions: stock alerts (Q22), fast-moving products (Q23), overstock
# candidates (Q24), supplier lead time (Q25), supplier reliability (Q26),
# reorder recommendations (Q27), store-level summary, and procurement actions.
# =========================

print("\n\nGenerating inventory optimization report...")

report_path = os.path.join(OUTPUT_DIR, "supplier_scorecard_report.txt")

# Pull all result DataFrames
stock_df = results["stock_alerts"].copy()
fast_df = results["fast_moving"].copy()
overstock_df = results["overstock"].copy()
sup_df = results["supplier_scorecard"].copy()
reorder_df = results["reorder_recommendations"].copy()
store_df = results["store_summary"].copy()

# Ensure numeric types (Spark DECIMAL -> Decimal -> needs cast for formatting)
for col in ["delivery_reliability_pct", "average_lead_time_days"]:
    if col in sup_df.columns:
        sup_df[col] = sup_df[col].astype(float)
for col in ["average_daily_sales", "estimated_days_of_inventory"]:
    if col in fast_df.columns:
        fast_df[col] = fast_df[col].astype(float)
    if col in overstock_df.columns:
        overstock_df[col] = overstock_df[col].astype(float)
for col in ["average_daily_demand", "planning_lead_time_days"]:
    if col in reorder_df.columns:
        reorder_df[col] = reorder_df[col].astype(float)

# Separate reorder vs no-reorder
reorder_only = reorder_df[reorder_df["recommendation_status"] == "REORDER"].copy()

# Aggregate reorder recommendations by supplier
reorder_by_supplier = (
    reorder_only
    .groupby("supplier_name")
    .agg(
        reorder_count=("product_name", "count"),
        total_recommended_qty=("recommended_order_quantity", "sum"),
    )
    .reset_index()
    .sort_values("total_recommended_qty", ascending=False)
)

out_of_stock = stock_df[stock_df["stock_status"] == "OUT_OF_STOCK"]
low_stock = stock_df[stock_df["stock_status"] == "LOW_STOCK"]

total_combos = len(reorder_df)
reorder_count = len(reorder_only)
critical_count = len(reorder_only[reorder_only["replenishment_priority"] == "CRITICAL"])
high_count = len(reorder_only[reorder_only["replenishment_priority"] == "HIGH"])

lines = []
lines.append("=" * 76)
lines.append("MEGAMART — INVENTORY OPTIMIZATION REPORT")
lines.append("Problem Statement 4: Inventory Optimization")
lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
lines.append("Data Period: 2024-01-01 to 2024-12-31")
lines.append(f"Scope: 5 stores × 14 products = {total_combos} store/product combinations")
lines.append("=" * 76)
lines.append("")
lines.append("TABLE OF CONTENTS")
lines.append("  1. Q22 — Out-of-Stock and Low-Stock Products (< 50 units)")
lines.append("  2. Q23 — Fastest-Moving Products (Highest Sales Velocity)")
lines.append("  3. Q24 — Slow-Moving Overstock Candidates")
lines.append("  4. Q25 — Average Days to Deliver by Supplier")
lines.append("  5. Q26 — Supplier Reliability Rankings (On-Time Delivery Rate)")
lines.append("  6. Q27 — Reorder Point Recommendations")
lines.append("  7. Store-Level Inventory Health Summary")
lines.append("  8. Procurement Action Recommendations")
lines.append("")

# --- Section 1: Q22 ---
lines.append("=" * 76)
lines.append("1. Q22 — OUT-OF-STOCK AND LOW-STOCK PRODUCTS (< 50 UNITS)")
lines.append("=" * 76)
lines.append("")
lines.append("Stock status thresholds:")
lines.append("  OUT_OF_STOCK : quantity_on_hand = 0")
lines.append("  LOW_STOCK    : quantity_on_hand < 50")
lines.append("  HEALTHY      : quantity_on_hand >= 50")
lines.append("")
lines.append("Summary:")
lines.append(f"  Out-of-stock products : {len(out_of_stock)}")
lines.append(f"  Low-stock products    : {len(low_stock)}")
lines.append(f"  Total stock alerts    : {len(stock_df)}")
lines.append(f"  Healthy products      : {total_combos - len(stock_df)}")
lines.append("")
if len(out_of_stock) > 0:
    lines.append(f"OUT-OF-STOCK ITEMS ({len(out_of_stock)}):")
    lines.append("-" * 75)
    lines.append(f"  {'Store':<28} {'Product':<25} {'Qty':>4}   {'Supplier'}")
    lines.append("-" * 75)
    for _, row in out_of_stock.iterrows():
        lines.append(
            f"  {str(row['store_name']):<28} {str(row['product_name']):<25} "
            f"{int(row['quantity_on_hand']):>4}   {row['supplier_name']}"
        )
    lines.append("-" * 75)
lines.append("")
if len(low_stock) > 0:
    lines.append(f"LOW-STOCK ITEMS ({len(low_stock)}):")
    lines.append("-" * 75)
    lines.append(f"  {'Store':<28} {'Product':<25} {'Qty':>4}   {'Supplier'}")
    lines.append("-" * 75)
    for _, row in low_stock.iterrows():
        lines.append(
            f"  {str(row['store_name']):<28} {str(row['product_name']):<25} "
            f"{int(row['quantity_on_hand']):>4}   {row['supplier_name']}"
        )
    lines.append("-" * 75)
lines.append("")

# --- Section 2: Q23 ---
lines.append("=" * 76)
lines.append("2. Q23 — FASTEST-MOVING PRODUCTS (HIGHEST SALES VELOCITY)")
lines.append("=" * 76)
lines.append("")
lines.append("Velocity classification:")
lines.append("  FAST_MOVING : top 20th percentile by total quantity sold")
lines.append("  NORMAL_MOVING : middle 60th percentile")
lines.append("  SLOW_MOVING : bottom 20th percentile")
lines.append("  average_daily_sales = total_quantity_sold / 366 (2024 is a leap year)")
lines.append("")
lines.append(f"FAST-MOVING STORE/PRODUCT COMBINATIONS ({len(fast_df)}):")
lines.append("-" * 75)
lines.append(
    f" {'Rank':>4}  {'Store':<22} {'Product':<22} {'Qty Sold':>8} "
    f"{'Avg Daily':>9} {'On Hand':>7} {'Days Inv':>7}"
)
lines.append("-" * 75)
for _, row in fast_df.iterrows():
    lines.append(
        f" {int(row['overall_velocity_rank']):>4}  {str(row['store_name']):<22} "
        f"{str(row['product_name']):<22} {int(row['total_quantity_sold']):>8} "
        f"{float(row['average_daily_sales']):>9.3f} {int(row['quantity_on_hand']):>7} "
        f"{float(row['estimated_days_of_inventory']):>7.0f}"
    )
lines.append("-" * 75)
lines.append("")

# --- Section 3: Q24 ---
lines.append("=" * 76)
lines.append("3. Q24 — SLOW-MOVING OVERSTOCK CANDIDATES")
lines.append("=" * 76)
lines.append("")
lines.append("Criteria: velocity_class = 'SLOW_MOVING' AND quantity_on_hand >= 50")
lines.append("These products have ample stock but very low sales velocity, tying up")
lines.append("warehouse space and capital.")
lines.append("")
lines.append(f"OVERSTOCK CANDIDATES ({len(overstock_df)}):")
lines.append("-" * 75)
lines.append(
    f"  {'Store':<22} {'Product':<22} {'Qty Sold':>8} "
    f"{'Avg Daily':>9} {'On Hand':>7} {'Days Inv':>7}"
)
lines.append("-" * 75)
for _, row in overstock_df.iterrows():
    lines.append(
        f"  {str(row['store_name']):<22} {str(row['product_name']):<22} "
        f"{int(row['total_quantity_sold']):>8} {float(row['average_daily_sales']):>9.3f} "
        f"{int(row['quantity_on_hand']):>7} {float(row['estimated_days_of_inventory']):>7.0f}"
    )
lines.append("-" * 75)
lines.append("")

# --- Section 4: Q25 ---
lines.append("=" * 76)
lines.append("4. Q25 — AVERAGE DAYS TO DELIVER BY SUPPLIER")
lines.append("=" * 76)
lines.append("")
lines.append("Average lead time is computed from delivered purchase orders only (those")
lines.append("with a non-null delivery_date). lead_time_days = delivery_date - order_date.")
lines.append("")
lines.append("-" * 75)
lines.append(
    f"  {'Supplier':<30} {'Avg Days':>8} {'Min':>5} {'Max':>5} {'Delivered':>10} {'Total POs':>10}"
)
lines.append("-" * 75)
for _, row in sup_df.sort_values("average_lead_time_days").iterrows():
    lines.append(
        f"  {str(row['supplier_name']):<30} "
        f"{row['average_lead_time_days']:>7.2f}d "
        f"{int(row['minimum_lead_time_days']):>4}d "
        f"{int(row['maximum_lead_time_days']):>4}d "
        f"{int(row['delivered_orders']):>10} "
        f"{int(row['total_purchase_orders']):>10}"
    )
lines.append("-" * 75)
lines.append("")

# --- Section 5: Q26 ---
lines.append("=" * 76)
lines.append("5. Q26 — SUPPLIER RELIABILITY RANKINGS (ON-TIME DELIVERY RATE)")
lines.append("=" * 76)
lines.append("")
lines.append("delivery_reliability_pct = delivered_orders / total_purchase_orders * 100")
lines.append("")
lines.append("Note: The source data does not define an SLA threshold or promised delivery")
lines.append("date. Reliability is therefore measured as the percentage of POs that have")
lines.append("been delivered (delivery_date IS NOT NULL), not as on-time-vs-SLA.")
lines.append(f"Definition: {sup_df['reliability_definition'].iloc[0]}")
lines.append("")
lines.append("-" * 75)
lines.append(
    f" {'Rank':>4}  {'Supplier':<30} {'Reliability':>12} {'Avg Lead':>10} "
    f"{'Delivered':>10} {'Failed':>7}"
)
lines.append("-" * 75)
for i, (_, row) in enumerate(sup_df.iterrows(), 1):
    lines.append(
        f" {i:>4}  {str(row['supplier_name']):<30} "
        f"{row['delivery_reliability_pct']:>11.2f}% "
        f"{row['average_lead_time_days']:>9.2f}d "
        f"{int(row['delivered_orders']):>10} "
        f"{int(row['failed_or_incomplete_orders']):>7}"
    )
lines.append("-" * 75)
lines.append("")

# --- Section 6: Q27 ---
lines.append("=" * 76)
lines.append("6. Q27 — REORDER POINT RECOMMENDATIONS")
lines.append("=" * 76)
lines.append("")
lines.append("Reorder Point Formula:")
lines.append("  Safety Stock     = Z × DailyDemandStdDev × √LeadTime")
lines.append("  Reorder Point    = AverageDailyDemand × LeadTime + SafetyStock")
lines.append("  Recommended Qty  = max(ReorderPoint - QuantityOnHand, 0)")
lines.append("  Z = 1.65 (approx. 95% one-sided service level)")
lines.append("")
lines.append("Method: DEMAND_BASED_REORDER_POINT")
lines.append("Lead time source: supplier_scorecard average_lead_time_days (fallback to")
lines.append("chain-wide average if supplier has no delivered orders)")
lines.append("")
lines.append("Summary:")
lines.append(f"  Total store/product combinations : {total_combos}")
lines.append(f"  REORDER recommended             : {reorder_count}")
lines.append(f"  NO_REORDER                      : {total_combos - reorder_count}")
lines.append("")
lines.append("Priority breakdown:")
lines.append(f"  CRITICAL (0 units on hand, at or below reorder point) : {critical_count}")
lines.append(f"  HIGH    (at or below reorder point)                    : {high_count}")
lines.append("")
if len(reorder_only) > 0:
    lines.append(f"REORDER RECOMMENDATIONS ({len(reorder_only)}):")
    lines.append("-" * 75)
    lines.append(
        f" {'Priority':<10} {'Store':<16} {'Product':<20} {'OnHand':>6} "
        f"{'AvgDaily':>8} {'Lead':>6} {'Safety':>6} {'ROP':>5} {'OrderQty':>8}  {'Supplier'}"
    )
    lines.append("-" * 75)
    for _, row in reorder_only.iterrows():
        lines.append(
            f" {row['replenishment_priority']:<10} {str(row['store_name']):<16} "
            f"{str(row['product_name']):<20} {int(row['quantity_on_hand']):>6} "
            f"{float(row['average_daily_demand']):>8.3f} {float(row['planning_lead_time_days']):>5.1f}d "
            f"{int(row['safety_stock']):>6} {int(row['reorder_point']):>5} "
            f"{int(row['recommended_order_quantity']):>8}  {row['supplier_name']}"
        )
    lines.append("-" * 75)
lines.append("")
lines.append("Supplier reorder workload:")
if len(reorder_by_supplier) > 0:
    for _, row in reorder_by_supplier.iterrows():
        lines.append(
            f"  {str(row['supplier_name']):<30} : {int(row['reorder_count'])} items, "
            f"{int(row['total_recommended_qty'])} units total"
        )
else:
    lines.append("  No reorders required at this time.")
lines.append("")

# --- Section 7: Store Summary ---
lines.append("=" * 76)
lines.append("7. STORE-LEVEL INVENTORY HEALTH SUMMARY")
lines.append("=" * 76)
lines.append("")
lines.append("-" * 75)
lines.append(
    f"  {'Store':<24} {'Total':>6} {'OOS':>5} {'Low':>5} {'Healthy':>8} {'Total On Hand':>14}"
)
lines.append("-" * 75)
for _, row in store_df.iterrows():
    lines.append(
        f"  {str(row['store_name']):<24} {int(row['total_products']):>6} "
        f"{int(row['out_of_stock_products']):>5} {int(row['low_stock_products']):>5} "
        f"{int(row['healthy_stock_products']):>8} {int(row['total_quantity_on_hand']):>14,}"
    )
lines.append("-" * 75)
lines.append("")

# --- Section 8: Procurement Recommendations ---
lines.append("=" * 76)
lines.append("8. PROCUREMENT ACTION RECOMMENDATIONS")
lines.append("=" * 76)
lines.append("")
lines.append("IMMEDIATE ACTIONS (CRITICAL — place POs today):")
crit = reorder_only[reorder_only["replenishment_priority"] == "CRITICAL"]
for i, (_, row) in enumerate(crit.iterrows(), 1):
    lines.append(
        f"  {i}. {row['supplier_name']}: Order {row['product_name']} for "
        f"{row['store_name']} ({int(row['recommended_order_quantity'])} units)."
    )
lines.append("")
high_items = reorder_only[reorder_only["replenishment_priority"] == "HIGH"]
if len(high_items) > 0:
    lines.append("SHORT-TERM ACTIONS (HIGH — next order cycle):")
    for i, (_, row) in enumerate(high_items.iterrows(), len(crit) + 1):
        lines.append(
            f"  {i}. {row['supplier_name']}: Order {row['product_name']} for "
            f"{row['store_name']} ({int(row['recommended_order_quantity'])} units) "
            f"— small gap, bundle with next regular order."
        )
lines.append("")
lines.append("OVERSTOCK REDISTRIBUTION:")
for _, row in overstock_df.head(3).iterrows():
    lines.append(
        f"  {row['store_name']} {row['product_name']} ({int(row['quantity_on_hand'])} units, "
        f"{float(row['estimated_days_of_inventory']):.0f} days inventory) — consider "
        f"redistribution or promotional clearance."
    )
lines.append("")
lines.append("SUPPLIER MANAGEMENT:")
lines.append(
    f"  • {sup_df.iloc[0]['supplier_name']}: Most reliable supplier "
    f"({sup_df.iloc[0]['delivery_reliability_pct']:.2f}%) — preferential "
    f"for future sourcing where product range overlaps."
)
lines.append(
    f"  • {sup_df.iloc[-1]['supplier_name']}: Lowest reliability "
    f"({sup_df.iloc[-1]['delivery_reliability_pct']:.2f}%) with "
    f"{int(sup_df.iloc[-1]['failed_or_incomplete_orders'])} failed orders — "
    f"consider diversifying to alternative suppliers."
)
lines.append("")

lines.append("=" * 76)
lines.append("End of Inventory Optimization Report")
lines.append("=" * 76)

report_text = "\n".join(lines)

with open(report_path, "w") as f:
    f.write(report_text)

print(f"  saved {report_path}")
print()
print(report_text)


# COMMAND ----------

# =========================
# 5. Summary
# =========================

print("\n" + "=" * 70)
print("INVENTORY EXPORT COMPLETE")
print("=" * 70)

print(f"\nFiles generated in {OUTPUT_DIR}:")
for key, (title, sql, csv_name) in QUERIES.items():
    print(f"  {csv_name:<40} {len(results[key]):>4} rows")
print(f"  {'supplier_scorecard_report.txt':<40}    text report")

print(f"\nTotal CSV files: {len(QUERIES)}")
print(f"Total rows exported: {sum(len(df) for df in results.values())}")
