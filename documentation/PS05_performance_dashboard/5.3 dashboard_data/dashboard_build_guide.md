# MegaMart Dashboard Build Guide

Run ID: 07_dashboard_20261007T083342_672562
Project period: 2024-01-01 to 2024-12-31

## Dashboard data sources

Use these Gold tables as the data sources for the Databricks AI/BI Dashboard:

1. megamart_dev.gold.enterprise_kpi
2. megamart_dev.gold.store_performance
3. megamart_dev.gold.category_kpis
4. megamart_dev.gold.monthly_kpis
5. megamart_dev.gold.store_category_top3

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

Use megamart_dev.gold.store_performance.

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

Use megamart_dev.gold.monthly_kpis.

Recommended fields:

- month_start
- month_label
- total_revenue
- transaction_count
- avg_transaction_value
- units_sold
- mom_revenue_change_pct

### Category performance

Use megamart_dev.gold.category_kpis.

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
