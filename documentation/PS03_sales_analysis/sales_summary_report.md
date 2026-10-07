# MEGAMART SALES ANALYSIS — SUMMARY REPORT (2024)

> **Generated:** 2026-10-06T20:30:12.884830+00:00  
> **Data source:** `megamart_dev.silver.*` (sales_transactions, stores, products)  
> **Output directory:** `/Workspace/Users/akharejayesh@gmail.com/MegaMart-Capstone-1/PS03_sales_analysis/results`

---

## Overview

| Metric | Value |
| --- | --- |
| Total revenue (all stores) | $1,119,510.00 |
| Total transactions (all stores) | 1,990 |
| Number of stores | 5 |

---

## Q1. Highest Revenue Store

**Top store:** Banjara Hills Store (Banjara Hills)

| Metric | Value |
| --- | --- |
| Revenue | $233,260.00 |
| Transactions | 398 |
| Avg transaction | $586.08 |

### Full Store Revenue Ranking

| Rank | Store | Revenue | Transactions |
| ---: | --- | --- | ---: |
| 1 | Banjara Hills Store | $233,260.00 | 398 |
| 2 | Ameerpet Store | $223,845.00 | 403 |
| 3 | HITEC City Store | $223,385.00 | 364 |
| 4 | Dilsukhnagar Store | $220,140.00 | 411 |
| 5 | Kukatpally Store | $218,880.00 | 414 |

---

## Q2. Top 5 Best-Selling Products by Quantity

| Rank | Product | Quantity | Revenue |
| ---: | --- | ---: | --- |
| 1 | Mineral Water 1L | 862 | $21,550.00 |
| 2 | Shampoo 200ml | 847 | $101,640.00 |
| 3 | Turmeric Powder 100g | 830 | $37,350.00 |
| 4 | Biscuits Mix 200g | 823 | $32,920.00 |
| 5 | Wheat Flour 1kg | 805 | $28,175.00 |

**Best seller:** Mineral Water 1L (862 units)

---

## Q3. Store Rankings (Revenue & Transaction Count)

| Store | Revenue Rank | Transaction Rank | Revenue |
| --- | ---: | ---: | --- |
| Banjara Hills Store | 1 | 4 | $233,260.00 |
| Ameerpet Store | 2 | 3 | $223,845.00 |
| HITEC City Store | 3 | 5 | $223,385.00 |
| Dilsukhnagar Store | 4 | 2 | $220,140.00 |
| Kukatpally Store | 5 | 1 | $218,880.00 |

---

## Q4. Most Profitable (Highest-Revenue) Product per Store

> **NOTE:** True profit requires unit cost, which is not in the source data.  
> Revenue contribution is used as the profitability proxy.

| Store | Top Product | Revenue | Share |
| --- | --- | --- | ---: |
| HITEC City Store | Basmati Rice 5kg | $68,850.00 | 30.82% |
| Banjara Hills Store | Basmati Rice 5kg | $68,850.00 | 29.52% |
| Ameerpet Store | Basmati Rice 5kg | $66,150.00 | 29.55% |
| Kukatpally Store | Basmati Rice 5kg | $45,000.00 | 20.56% |
| Dilsukhnagar Store | Basmati Rice 5kg | $63,900.00 | 29.03% |

---

## Q5. Month-on-Month Sales Trend

- **Largest MoM growth:** HITEC City Store in 2024-11 (+193.50%)
- **Largest MoM decline:** Dilsukhnagar Store in 2024-07 (−67.34%)

---

## Q6. Store Product Diversity

| Rank | Store | Products | Categories |
| ---: | --- | ---: | ---: |
| 1 | Dilsukhnagar Store | 14 | 7 |
| 1 | Kukatpally Store | 14 | 7 |
| 1 | Banjara Hills Store | 14 | 7 |
| 1 | Ameerpet Store | 14 | 7 |
| 1 | HITEC City Store | 14 | 7 |

**Most diverse:** Dilsukhnagar Store (14 unique products)  
**Least diverse:** HITEC City Store (14 unique products)

---

## Files Produced

| # | File |
| ---: | --- |
| 1 | `1_store_revenue_2024.csv` |
| 2 | `2_top5_products_by_quantity.csv` |
| 3 | `3_store_rankings.csv` |
| 4 | `4_most_profitable_product_per_store.csv` |
| 5 | `5_monthly_sales_trend_by_store.csv` |
| 6 | `6_store_product_diversity.csv` |
| 7 | `chart_1_revenue_by_store.png` |
| 8 | `chart_2_top5_products.png` |
| 9 | `chart_3_store_rankings.png` |
| 10 | `chart_4_top_product_per_store.png` |
| 11 | `chart_4b_product_share_by_store.png` |
| 12 | `chart_5_monthly_trend.png` |
| 13 | `chart_6_diversity.png` |
| 14 | `sales_summary_report.txt` |