-- ============================================================================
-- MegaMart Databricks Capstone
-- Subproject: Sales Analysis
-- File: sales_analysis.sql
--
-- Purpose:
--   Answer six business questions about MegaMart 2024 sales using the
--   cleaned Silver-layer tables. Each query uses JOINs to combine data
--   from multiple tables and GROUP BY with aggregate functions
--   (SUM, COUNT, AVG) to summarize results.
--
-- Source tables (Unity Catalog: megamart_dev.silver):
--   megamart_dev.silver.stores            (store_id, store_name, location, manager_name)
--   megamart_dev.silver.products          (product_id, product_name, category, unit_price, supplier_id)
--   megamart_dev.silver.sales_transactions(transaction_id, store_id, product_id,
--                             quantity_sold, unit_price, total_amount, sale_date)
--
-- Data period: 2024-01-01 to 2024-12-31
--
-- Note on profitability:
--   The source generator does NOT provide a product unit cost, so true
--   profit cannot be computed. "Most profitable" is therefore measured
--   by revenue contribution, the best available proxy. This is flagged
--   explicitly in the result columns.
-- ============================================================================


USE CATALOG megamart_dev;


-- ============================================================================
-- QUESTION 1: Which store has the highest total revenue in 2024?
--
-- Tables joined: sales_transactions + stores
-- Aggregates   : SUM(total_amount), COUNT(DISTINCT transaction_id), AVG(total_amount)
-- ============================================================================

SELECT
    s.store_id,
    s.store_name,
    s.location,

    ROUND(SUM(st.total_amount), 2)                       AS total_revenue,
    COUNT(DISTINCT st.transaction_id)                    AS transaction_count,
    ROUND(AVG(st.total_amount), 2)                       AS avg_transaction_value,
    DENSE_RANK() OVER (ORDER BY SUM(st.total_amount) DESC) AS revenue_rank
FROM megamart_dev.silver.sales_transactions st
INNER JOIN megamart_dev.silver.stores s
    ON st.store_id = s.store_id
WHERE YEAR(st.sale_date) = 2024
GROUP BY s.store_id, s.store_name, s.location
ORDER BY total_revenue DESC;


-- ============================================================================
-- QUESTION 2: What are the top 5 best-selling products by quantity?
--
-- Tables joined: sales_transactions + products
-- Aggregates   : SUM(quantity_sold), SUM(total_amount), COUNT(DISTINCT transaction_id)
-- ============================================================================

SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(st.quantity_sold)                                AS total_quantity_sold,
    ROUND(SUM(st.total_amount), 2)                       AS total_revenue,
    COUNT(DISTINCT st.transaction_id)                     AS transaction_count,
    ROW_NUMBER() OVER (ORDER BY SUM(st.quantity_sold) DESC, SUM(st.total_amount) DESC) AS quantity_rank
FROM megamart_dev.silver.sales_transactions st
INNER JOIN megamart_dev.silver.products p
    ON st.product_id = p.product_id
WHERE YEAR(st.sale_date) = 2024
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_quantity_sold DESC
LIMIT 5;


-- ============================================================================
-- QUESTION 3: How does each store rank by revenue and transaction count?
--
-- Tables joined: sales_transactions + stores
-- Aggregates   : SUM(total_amount), COUNT(DISTINCT transaction_id), AVG(total_amount)
-- ============================================================================

SELECT
    s.store_id,
    s.store_name,
    s.location,
    ROUND(SUM(st.total_amount), 2)                            AS total_revenue,
    COUNT(DISTINCT st.transaction_id)                         AS transaction_count,
    ROUND(AVG(st.total_amount), 2)                            AS avg_transaction_value,
    DENSE_RANK() OVER (ORDER BY SUM(st.total_amount) DESC)        AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT st.transaction_id) DESC) AS transaction_count_rank
FROM megamart_dev.silver.sales_transactions st
INNER JOIN megamart_dev.silver.stores s
    ON st.store_id = s.store_id
WHERE YEAR(st.sale_date) = 2024
GROUP BY s.store_id, s.store_name, s.location
ORDER BY revenue_rank, transaction_count_rank;


-- ============================================================================
-- QUESTION 4: Which products are most profitable per store?
--
-- Tables joined: sales_transactions + stores + products
-- Aggregates   : SUM(total_amount), SUM(quantity_sold), COUNT(DISTINCT transaction_id), AVG(unit_price)
--
-- Note:
--   True profitability requires unit cost, which is not in the source data.
--   Revenue contribution is used as the profitability proxy and the
--   per-store revenue share percentage is reported.
-- ============================================================================

WITH store_product_sales AS (
    SELECT
        st.store_id,
        s.store_name,
        p.product_id,
        p.product_name,
        p.category,
        SUM(st.quantity_sold)        AS total_quantity_sold,
        ROUND(SUM(st.total_amount), 2) AS total_revenue,
        COUNT(DISTINCT st.transaction_id) AS transaction_count,
        ROUND(AVG(st.unit_price), 2)    AS avg_selling_price
    FROM megamart_dev.silver.sales_transactions st
    INNER JOIN megamart_dev.silver.stores s   ON st.store_id   = s.store_id
    INNER JOIN megamart_dev.silver.products p ON st.product_id = p.product_id
    WHERE YEAR(st.sale_date) = 2024
    GROUP BY st.store_id, s.store_name, p.product_id, p.product_name, p.category
),
store_totals AS (
    SELECT
        store_id,
        SUM(total_revenue) AS store_total_revenue
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
    ) AS revenue_rank_within_store,
    'REVENUE_PROXY_NO_UNIT_COST' AS profitability_status
FROM store_product_sales sps
INNER JOIN store_totals st
    ON sps.store_id = st.store_id
ORDER BY sps.store_id, revenue_rank_within_store;


-- ============================================================================
-- QUESTION 5: What is the month-on-month sales trend for each store?
--
-- Tables joined: sales_transactions + stores
-- Aggregates   : SUM(total_amount), SUM(quantity_sold), COUNT(DISTINCT transaction_id)
-- Window       : LAG() for previous-month revenue and MoM growth %
-- ============================================================================

WITH monthly_sales AS (
    SELECT
        s.store_id,
        s.store_name,
        DATE_TRUNC('MONTH', st.sale_date) AS month_start,
        SUM(st.quantity_sold)             AS monthly_quantity_sold,
        ROUND(SUM(st.total_amount), 2)    AS monthly_revenue,
        COUNT(DISTINCT st.transaction_id)  AS monthly_transaction_count
    FROM megamart_dev.silver.sales_transactions st
    INNER JOIN megamart_dev.silver.stores s
        ON st.store_id = s.store_id
    WHERE YEAR(st.sale_date) = 2024
    GROUP BY s.store_id, s.store_name, DATE_TRUNC('MONTH', st.sale_date)
),
with_previous AS (
    SELECT
        *,
        LAG(monthly_revenue) OVER (
            PARTITION BY store_id
            ORDER BY month_start
        ) AS previous_month_revenue
    FROM monthly_sales
)
SELECT
    store_id,
    store_name,
    DATE_FORMAT(month_start, 'yyyy-MM')  AS year_month,
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
ORDER BY store_id, month_start;


-- ============================================================================
-- QUESTION 6: Which store has the most diverse product sales?
--
-- Tables joined: stores + sales_transactions + products
-- Aggregates   : COUNT(DISTINCT product_id), COUNT(DISTINCT category)
-- ============================================================================

SELECT
    s.store_id,
    s.store_name,
    s.location,
    COUNT(DISTINCT st.product_id) AS unique_products_sold,
    COUNT(DISTINCT p.category)     AS unique_categories_sold,
    ROUND(SUM(st.quantity_sold), 0) AS total_quantity_sold,
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT st.product_id) DESC) AS diversity_rank
FROM megamart_dev.silver.stores s
LEFT JOIN megamart_dev.silver.sales_transactions st
    ON s.store_id = st.store_id
    AND YEAR(st.sale_date) = 2024
LEFT JOIN megamart_dev.silver.products p
    ON st.product_id = p.product_id
GROUP BY s.store_id, s.store_name, s.location
ORDER BY unique_products_sold DESC, unique_categories_sold DESC;


-- ============================================================================
-- End of sales_analysis.sql
-- ============================================================================
