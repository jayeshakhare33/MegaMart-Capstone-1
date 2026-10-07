-- ============================================================================
-- MegaMart Gold Layer — Consolidated Views
-- ============================================================================
-- Replaces 18 physical gold tables with 10 views + 1 table.
-- All views query the Silver layer directly and compute on demand.
--
-- The only remaining gold table is reorder_recommendations, created by
-- 05_inventory_optimizer.py (complex multi-step Python logic).
--
-- View inventory:
--   1. store_performance      — store-level sales, rankings, diversity, KPIs,
--                               inventory health (5 rows)
--   2. product_performance    — store×product sales, velocity, rankings (70 rows)
--   3. top_products           — top 5 products by quantity sold (5 rows)
--   4. monthly_store_trends   — per-store monthly sales with MoM (60 rows)
--   5. monthly_kpis           — chain-wide monthly KPIs with MoM (12 rows)
--   6. category_kpis          — category-level KPIs with revenue share (7 rows)
--   7. enterprise_kpi         — single-row chain-wide summary (1 row)
--   8. inventory_status       — stock status by store×product (70 rows)
--   9. supplier_scorecard     — supplier delivery performance (12 rows)
--  10. store_category_top3   — top 3 categories per store (15 rows)
--
-- Replaces these former tables:
--   store_sales_summary, store_revenue_ranking, store_product_diversity,
--   store_kpi_summary, dashboard_store_performance,
--   product_store_sales, product_velocity, top_products,
--   monthly_store_sales, monthly_kpi_summary, dashboard_monthly_trend,
--   category_kpi_summary, dashboard_category_performance,
--   enterprise_kpi_summary, dashboard_enterprise_summary,
--   inventory_status, supplier_scorecard
-- ============================================================================

USE CATALOG megamart_dev;


-- ============================================================================
-- View 1: store_performance
--
-- Combines: store_sales_summary, store_revenue_ranking,
--           store_product_diversity, store_kpi_summary,
--           dashboard_store_performance
--
-- Granularity: one row per store (5 rows).
-- ============================================================================

CREATE OR REPLACE VIEW gold.store_performance AS
WITH sales_agg AS (
    SELECT
        s.store_id,
        s.store_name,
        s.location,
        COUNT(DISTINCT st.transaction_id)  AS transaction_count,
        SUM(st.quantity_sold)                AS total_quantity_sold,
        SUM(st.total_amount)                 AS total_revenue,
        ROUND(
            SUM(st.total_amount)
            / NULLIF(COUNT(DISTINCT st.transaction_id), 0),
            2
        )                                    AS average_transaction_value,
        COUNT(DISTINCT st.product_id)        AS unique_products_sold
    FROM silver.sales_transactions st
    INNER JOIN silver.stores s
        ON st.store_id = s.store_id
    GROUP BY
        s.store_id,
        s.store_name,
        s.location
),

diversity AS (
    SELECT
        s.store_id,
        COUNT(DISTINCT p.category) AS unique_categories_sold
    FROM silver.stores s
    LEFT JOIN silver.sales_transactions st
        ON s.store_id = st.store_id
    LEFT JOIN silver.products p
        ON st.product_id = p.product_id
    GROUP BY s.store_id
),

inventory_summary AS (
    SELECT
        store_id,
        SUM(quantity_on_hand)                                                AS current_inventory_units,
        SUM(CASE WHEN quantity_on_hand = 0  THEN 1 ELSE 0 END)               AS out_of_stock_skus,
        SUM(CASE WHEN quantity_on_hand > 0
                  AND quantity_on_hand < 50 THEN 1 ELSE 0 END)               AS low_stock_skus,
        SUM(CASE WHEN quantity_on_hand >= 50 THEN 1 ELSE 0 END)              AS healthy_stock_skus
    FROM silver.inventory
    GROUP BY store_id
),

chain_total AS (
    SELECT SUM(total_amount) AS grand_total
    FROM silver.sales_transactions
)

SELECT
    sa.store_id,
    sa.store_name,
    sa.location,
    sa.transaction_count,
    sa.total_quantity_sold,
    sa.total_revenue,
    sa.average_transaction_value,
    sa.unique_products_sold,
    d.unique_categories_sold,

    -- Rankings (from store_revenue_ranking)
    DENSE_RANK() OVER (ORDER BY sa.total_revenue DESC)       AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY sa.transaction_count DESC)    AS transaction_count_rank,

    -- Revenue share (from store_kpi_summary)
    ROUND(
        sa.total_revenue / NULLIF(ct.grand_total, 0) * 100,
        2
    ) AS store_revenue_share_pct,

    -- Inventory turnover — simplified units-based (units sold / current stock)
    ROUND(
        sa.total_quantity_sold / NULLIF(inv.current_inventory_units, 0),
        4
    )                                                         AS inventory_turnover_ratio,
    'APPROXIMATE_UNITS_BASED_SINGLE_SNAPSHOT'                  AS inventory_turnover_status,

    -- Market share — internal chain share (% of total chain revenue)
    ROUND(
        sa.total_revenue / NULLIF(ct.grand_total, 0) * 100,
        2
    )                                                         AS market_share_pct,
    'INTERNAL_CHAIN_SHARE'                                    AS market_share_status,

    -- Inventory health (from dashboard_store_performance)
    COALESCE(inv.current_inventory_units, 0)                 AS current_inventory_units,
    COALESCE(inv.out_of_stock_skus, 0)                        AS out_of_stock_skus,
    COALESCE(inv.low_stock_skus, 0)                           AS low_stock_skus,
    COALESCE(inv.healthy_stock_skus, 0)                      AS healthy_stock_skus,
    ROUND(
        COALESCE(inv.healthy_stock_skus, 0)
        / NULLIF((SELECT COUNT(DISTINCT product_id) FROM silver.products), 0)
        * 100,
        2
    )                                                         AS inventory_health_pct

FROM sales_agg sa
LEFT JOIN diversity d
    ON sa.store_id = d.store_id
LEFT JOIN inventory_summary inv
    ON sa.store_id = inv.store_id
CROSS JOIN chain_total ct;


-- ============================================================================
-- View 2: product_performance
--
-- Combines: product_store_sales, product_velocity
--
-- Granularity: one row per store×product (70 rows).
-- ============================================================================

CREATE OR REPLACE VIEW gold.product_performance AS
WITH sales_by_store_product AS (
    SELECT
        st.store_id,
        st.product_id,
        SUM(st.quantity_sold)                    AS total_quantity_sold,
        COUNT(DISTINCT st.transaction_id)        AS transaction_count,
        SUM(st.total_amount)                     AS total_revenue,
        AVG(st.unit_price)                       AS average_selling_price
    FROM silver.sales_transactions st
    GROUP BY
        st.store_id,
        st.product_id
),

store_totals AS (
    SELECT
        store_id,
        SUM(total_revenue) AS store_total_revenue
    FROM sales_by_store_product
    GROUP BY store_id
)

SELECT
    i.inventory_id,

    s.store_id,
    s.store_name,
    s.location,

    p.product_id,
    p.product_name,
    p.category,

    p.supplier_id,
    sup.supplier_name,

    i.quantity_on_hand,

    -- Sales metrics (from product_store_sales)
    COALESCE(sbsp.total_quantity_sold, 0)                       AS total_quantity_sold,
    COALESCE(sbsp.transaction_count, 0)                         AS transaction_count,
    ROUND(COALESCE(sbsp.total_revenue, 0), 2)                   AS total_revenue,
    ROUND(COALESCE(sbsp.average_selling_price, 0), 2)           AS average_selling_price,

    ROUND(
        COALESCE(sbsp.total_revenue, 0)
        / NULLIF(st_total.store_total_revenue, 0)
        * 100,
        2
    )                                                           AS store_revenue_share_pct,

    DENSE_RANK() OVER (
        PARTITION BY s.store_id
        ORDER BY COALESCE(sbsp.total_revenue, 0) DESC, p.product_id
    )                                                           AS revenue_rank_within_store,

    DENSE_RANK() OVER (
        PARTITION BY s.store_id
        ORDER BY COALESCE(sbsp.total_quantity_sold, 0) DESC, p.product_id
    )                                                           AS quantity_rank_within_store,

    -- Velocity metrics (from product_velocity)
    ROUND(COALESCE(sbsp.total_quantity_sold, 0) / 366.0, 4)     AS average_daily_sales,

    ROUND(
        CASE
            WHEN i.quantity_on_hand > 0
            THEN i.quantity_on_hand
                 / NULLIF(COALESCE(sbsp.total_quantity_sold, 0) / 366.0, 0)
            ELSE 0
        END,
        2
    )                                                           AS estimated_days_of_inventory,

    DENSE_RANK() OVER (
        ORDER BY COALESCE(sbsp.total_quantity_sold, 0) DESC,
                 p.product_id,
                 s.store_id
    )                                                           AS overall_velocity_rank,

    DENSE_RANK() OVER (
        PARTITION BY s.store_id
        ORDER BY COALESCE(sbsp.total_quantity_sold, 0) DESC,
                 p.product_id
    )                                                           AS store_velocity_rank,

    CASE
        WHEN COALESCE(sbsp.total_quantity_sold, 0) = 0
            THEN 'NO_SALES'
        WHEN PERCENT_RANK() OVER (
            ORDER BY COALESCE(sbsp.total_quantity_sold, 0)
        ) >= 0.80
            THEN 'FAST_MOVING'
        WHEN PERCENT_RANK() OVER (
            ORDER BY COALESCE(sbsp.total_quantity_sold, 0)
        ) <= 0.20
            THEN 'SLOW_MOVING'
        ELSE 'NORMAL_MOVING'
    END                                                         AS velocity_class,

    CASE
        WHEN i.quantity_on_hand = 0
            THEN 'OUT_OF_STOCK'
        WHEN i.quantity_on_hand < 50
            THEN 'LOW_STOCK'
        ELSE 'HEALTHY'
    END                                                         AS stock_status,

    'TRUE_PROFITABILITY_NOT_CALCULABLE_NO_UNIT_COST'
        AS profitability_status

FROM silver.inventory i
INNER JOIN silver.stores s
    ON i.store_id = s.store_id
INNER JOIN silver.products p
    ON i.product_id = p.product_id
LEFT JOIN silver.suppliers sup
    ON p.supplier_id = sup.supplier_id
LEFT JOIN sales_by_store_product sbsp
    ON i.store_id = sbsp.store_id
    AND i.product_id = sbsp.product_id
LEFT JOIN store_totals st_total
    ON sbsp.store_id = st_total.store_id;


-- ============================================================================
-- View 3: top_products
--
-- Replaces: top_products
--
-- Granularity: one row per product, filtered to top 5 by quantity sold.
-- Built on top of product_performance (aggregated to product level).
-- ============================================================================

CREATE OR REPLACE VIEW gold.top_products AS
WITH product_sales AS (
    SELECT
        pp.product_id,
        pp.product_name,
        pp.category,
        SUM(pp.total_quantity_sold)  AS total_quantity_sold,
        SUM(pp.total_revenue)        AS total_revenue,
        SUM(pp.transaction_count)   AS transaction_count
    FROM gold.product_performance pp
    GROUP BY
        pp.product_id,
        pp.product_name,
        pp.category
),
ranked_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY
                total_quantity_sold DESC,
                total_revenue DESC,
                product_id
        ) AS quantity_rank
    FROM product_sales
)
SELECT
    quantity_rank,
    product_id,
    product_name,
    category,
    total_quantity_sold,
    total_revenue,
    transaction_count
FROM ranked_products
WHERE quantity_rank <= 5;


-- ============================================================================
-- View 4: monthly_store_trends
--
-- Replaces: monthly_store_sales
--
-- Granularity: one row per store×month (5 stores × 12 months = 60 rows).
-- Creates a complete Jan–Dec 2024 monthly grid with MoM metrics.
-- ============================================================================

CREATE OR REPLACE VIEW gold.monthly_store_trends AS
WITH months AS (
    SELECT
        ADD_MONTHS(
            DATE '2024-01-01',
            month_number
        ) AS month_start
    FROM (
        SELECT EXPLODE(
            SEQUENCE(0, 11)
        ) AS month_number
    )
),

store_months AS (
    SELECT
        s.store_id,
        s.store_name,
        s.location,
        m.month_start
    FROM silver.stores s
    CROSS JOIN months m
),

monthly_sales AS (
    SELECT
        store_id,
        DATE_TRUNC(
            'MONTH',
            sale_date
        ) AS month_start,
        SUM(quantity_sold)             AS monthly_quantity_sold,
        SUM(total_amount)              AS monthly_revenue,
        COUNT(DISTINCT transaction_id) AS monthly_transaction_count
    FROM silver.sales_transactions
    GROUP BY
        store_id,
        DATE_TRUNC(
            'MONTH',
            sale_date
        )
),

combined AS (
    SELECT
        sm.store_id,
        sm.store_name,
        sm.location,
        sm.month_start,
        COALESCE(ms.monthly_quantity_sold, 0)        AS monthly_quantity_sold,
        COALESCE(ms.monthly_revenue, 0)              AS monthly_revenue,
        COALESCE(ms.monthly_transaction_count, 0)    AS monthly_transaction_count
    FROM store_months sm
    LEFT JOIN monthly_sales ms
        ON sm.store_id = ms.store_id
        AND sm.month_start = ms.month_start
),

with_previous_month AS (
    SELECT
        *,
        LAG(monthly_revenue) OVER (
            PARTITION BY store_id
            ORDER BY month_start
        ) AS previous_month_revenue
    FROM combined
)

SELECT
    store_id,
    store_name,
    location,
    month_start,
    MONTH(month_start) AS month_number,
    DATE_FORMAT(
        month_start,
        'yyyy-MM'
    ) AS year_month,
    monthly_quantity_sold,
    monthly_transaction_count,
    monthly_revenue,
    previous_month_revenue,
    ROUND(
        monthly_revenue
        - COALESCE(previous_month_revenue, 0),
        2
    ) AS mom_revenue_change,
    CASE
        WHEN previous_month_revenue IS NULL THEN NULL
        WHEN previous_month_revenue = 0
            AND monthly_revenue > 0
            THEN NULL
        ELSE ROUND(
            (
                monthly_revenue
                - previous_month_revenue
            )
            / previous_month_revenue
            * 100,
            2
        )
    END AS mom_revenue_growth_pct
FROM with_previous_month;


-- ============================================================================
-- View 5: monthly_kpis
--
-- Combines: monthly_kpi_summary, dashboard_monthly_trend
--
-- Granularity: one row per month (12 rows, chain-wide).
-- Includes MoM change percentages and a human-readable month label.
-- ============================================================================

CREATE OR REPLACE VIEW gold.monthly_kpis AS
WITH month_calendar AS (
    SELECT EXPLODE(
        SEQUENCE(
            TO_DATE('2024-01-01'),
            TO_DATE('2024-12-31'),
            INTERVAL 1 MONTH
        )
    ) AS month_start
),

monthly_agg AS (
    SELECT
        DATE_TRUNC('MONTH', sale_date)  AS month_start,
        ROUND(SUM(total_amount), 2)     AS total_revenue,
        COUNT(DISTINCT transaction_id)   AS transaction_count,
        ROUND(AVG(total_amount), 2)      AS avg_transaction_value,
        COUNT(DISTINCT st.product_id)    AS unique_products_sold,
        SUM(st.quantity_sold)             AS units_sold,
        COUNT(DISTINCT p.category)       AS categories_sold
    FROM silver.sales_transactions st
    JOIN silver.products p
        ON st.product_id = p.product_id
    GROUP BY DATE_TRUNC('MONTH', sale_date)
)

SELECT
    mc.month_start,
    DATE_FORMAT(mc.month_start, 'MMM yyyy')  AS month_label,
    COALESCE(ma.total_revenue, 0)            AS total_revenue,
    COALESCE(ma.transaction_count, 0)       AS transaction_count,
    COALESCE(ma.avg_transaction_value, 0)    AS avg_transaction_value,
    COALESCE(ma.unique_products_sold, 0)     AS unique_products_sold,
    COALESCE(ma.units_sold, 0)              AS units_sold,
    COALESCE(ma.categories_sold, 0)         AS categories_sold,

    -- MoM revenue change %
    CASE
        WHEN LAG(ma.total_revenue) OVER (ORDER BY mc.month_start) > 0
        THEN ROUND(
            (ma.total_revenue - LAG(ma.total_revenue) OVER (ORDER BY mc.month_start))
            / LAG(ma.total_revenue) OVER (ORDER BY mc.month_start) * 100,
            2
        )
        ELSE NULL
    END AS mom_revenue_change_pct,

    -- MoM transaction change %
    CASE
        WHEN LAG(ma.transaction_count) OVER (ORDER BY mc.month_start) > 0
        THEN ROUND(
            (ma.transaction_count - LAG(ma.transaction_count) OVER (ORDER BY mc.month_start))
            / LAG(ma.transaction_count) OVER (ORDER BY mc.month_start) * 100,
            2
        )
        ELSE NULL
    END AS mom_transaction_change_pct,

    -- MoM units change %
    CASE
        WHEN LAG(ma.units_sold) OVER (ORDER BY mc.month_start) > 0
        THEN ROUND(
            (ma.units_sold - LAG(ma.units_sold) OVER (ORDER BY mc.month_start))
            / LAG(ma.units_sold) OVER (ORDER BY mc.month_start) * 100,
            2
        )
        ELSE NULL
    END AS mom_units_change_pct

FROM month_calendar mc
LEFT JOIN monthly_agg ma
    ON mc.month_start = ma.month_start
ORDER BY mc.month_start;


-- ============================================================================
-- View 6: category_kpis
--
-- Combines: category_kpi_summary, dashboard_category_performance
--
-- Granularity: one row per category (7 rows).
-- Includes category rank, top-3 flag, and revenue share %.
-- ============================================================================

CREATE OR REPLACE VIEW gold.category_kpis AS
WITH category_agg AS (
    SELECT
        p.category,
        ROUND(SUM(st.total_amount), 2)     AS total_revenue,
        SUM(st.quantity_sold)               AS units_sold,
        COUNT(DISTINCT st.transaction_id)   AS transaction_count,
        COUNT(DISTINCT st.product_id)       AS unique_products_sold,
        ROUND(AVG(st.total_amount), 2)       AS avg_transaction_value
    FROM silver.sales_transactions st
    INNER JOIN silver.products p
        ON st.product_id = p.product_id
    GROUP BY p.category
),

category_total AS (
    SELECT SUM(total_revenue) AS grand_total
    FROM category_agg
),

ranked AS (
    SELECT
        ca.*,
        ROW_NUMBER() OVER (
            ORDER BY ca.total_revenue DESC, ca.category
        ) AS category_rank
    FROM category_agg ca
)

SELECT
    r.category,
    r.total_revenue,
    r.units_sold,
    r.transaction_count,
    r.unique_products_sold,
    r.avg_transaction_value,
    r.category_rank,
    CASE WHEN r.category_rank <= 3 THEN TRUE ELSE FALSE END  AS top_3_category_flag,
    ROUND(
        r.total_revenue / NULLIF(ct.grand_total, 0) * 100,
        2
    )                                                         AS revenue_share_pct
FROM ranked r
CROSS JOIN category_total ct
ORDER BY r.category_rank;


-- ============================================================================
-- View 7: enterprise_kpi
--
-- Combines: enterprise_kpi_summary, dashboard_enterprise_summary
--
-- Granularity: single row (chain-wide summary).
-- ============================================================================

CREATE OR REPLACE VIEW gold.enterprise_kpi AS
WITH sales_metrics AS (
    SELECT
        ROUND(SUM(total_amount), 2)              AS total_revenue,
        COUNT(DISTINCT transaction_id)           AS transaction_count,
        ROUND(AVG(total_amount), 2)              AS avg_transaction_value,
        COUNT(DISTINCT product_id)              AS unique_products_sold,
        COUNT(DISTINCT store_id)                 AS active_stores,
        SUM(quantity_sold)                       AS units_sold
    FROM silver.sales_transactions
),

inventory_metrics AS (
    SELECT
        SUM(quantity_on_hand)                                           AS current_inventory_units,
        SUM(CASE WHEN quantity_on_hand = 0  THEN 1 ELSE 0 END)          AS out_of_stock_skus,
        SUM(CASE WHEN quantity_on_hand < 50  THEN 1 ELSE 0 END)          AS low_stock_skus
    FROM silver.inventory
),

product_count AS (
    SELECT COUNT(DISTINCT product_id) AS total_products_in_catalog
    FROM silver.products
),

store_count AS (
    SELECT COUNT(DISTINCT store_id) AS total_stores_in_master
    FROM silver.stores
),

monthly_avg AS (
    SELECT ROUND(AVG(monthly_revenue), 2) AS avg_monthly_revenue
    FROM (
        SELECT DATE_TRUNC('MONTH', sale_date) AS month_start,
               SUM(total_amount)                AS monthly_revenue
        FROM silver.sales_transactions
        GROUP BY DATE_TRUNC('MONTH', sale_date)
    ) m
)

SELECT
    '2024-01-01'                                                AS project_start_date,
    '2024-12-31'                                                AS project_end_date,
    sm.total_revenue,
    sm.transaction_count,
    sm.avg_transaction_value,
    sm.unique_products_sold,
    pc.total_products_in_catalog,
    sm.active_stores,
    sc.total_stores_in_master,
    im.current_inventory_units,
    sm.units_sold,
    ROUND(
        sm.units_sold / NULLIF(im.current_inventory_units, 0),
        4
    )                                                          AS inventory_turnover_ratio,
    'APPROXIMATE_UNITS_BASED_SINGLE_SNAPSHOT'                  AS inventory_turnover_status,
    100.0                                                      AS market_share_pct,
    'INTERNAL_CHAIN_SHARE'                                    AS market_share_status,
    TRUE                                                        AS internal_store_revenue_share_available,
    im.out_of_stock_skus,
    im.low_stock_skus,
    ma.avg_monthly_revenue,
    '2024-01-01 to 2024-12-31'                                  AS dashboard_period

FROM sales_metrics sm
CROSS JOIN inventory_metrics im
CROSS JOIN product_count pc
CROSS JOIN store_count sc
CROSS JOIN monthly_avg ma;


-- ============================================================================
-- View 8: inventory_status
--
-- Replaces: inventory_status
--
-- Granularity: one row per store×product (70 rows).
-- Simple stock-status classification.
-- ============================================================================

CREATE OR REPLACE VIEW gold.inventory_status AS
SELECT
    i.inventory_id,
    i.store_id,
    s.store_name,
    s.location,
    i.product_id,
    p.product_name,
    p.category,
    p.supplier_id,
    sup.supplier_name,
    i.quantity_on_hand,
    CASE
        WHEN i.quantity_on_hand = 0  THEN 'OUT_OF_STOCK'
        WHEN i.quantity_on_hand < 50 THEN 'LOW_STOCK'
        ELSE 'HEALTHY'
    END AS stock_status,
    CASE
        WHEN i.quantity_on_hand = 0  THEN 3
        WHEN i.quantity_on_hand < 50 THEN 2
        ELSE 1
    END AS stock_priority
FROM silver.inventory i
INNER JOIN silver.stores s
    ON i.store_id = s.store_id
INNER JOIN silver.products p
    ON i.product_id = p.product_id
LEFT JOIN silver.suppliers sup
    ON p.supplier_id = sup.supplier_id;


-- ============================================================================
-- View 9: supplier_scorecard
--
-- Replaces: supplier_scorecard
--
-- Granularity: one row per supplier (12 rows).
-- Supplier delivery performance based on delivered vs pending orders.
-- ============================================================================

CREATE OR REPLACE VIEW gold.supplier_scorecard AS
SELECT
    sup.supplier_id,
    sup.supplier_name,

    COUNT(po.po_id)                                             AS total_purchase_orders,

    COUNT(
        CASE WHEN po.delivery_date IS NOT NULL THEN po.po_id END
    )                                                           AS delivered_orders,

    COUNT(
        CASE WHEN po.delivery_date IS NULL     THEN po.po_id END
    )                                                           AS failed_or_incomplete_orders,

    ROUND(
        COUNT(
            CASE WHEN po.delivery_date IS NOT NULL THEN po.po_id END
        )
        / NULLIF(COUNT(po.po_id), 0)
        * 100,
        2
    )                                                           AS delivery_reliability_pct,

    ROUND(
        AVG(
            CASE WHEN po.delivery_date IS NOT NULL THEN po.lead_time_days END
        ),
        2
    )                                                           AS average_lead_time_days,

    MIN(
        CASE WHEN po.delivery_date IS NOT NULL THEN po.lead_time_days END
    )                                                           AS minimum_lead_time_days,

    MAX(
        CASE WHEN po.delivery_date IS NOT NULL THEN po.lead_time_days END
    )                                                           AS maximum_lead_time_days,

    'RELIABILITY_BASED_ON_DELIVERED_ORDERS'
        AS reliability_definition

FROM silver.suppliers sup
LEFT JOIN silver.purchase_orders po
    ON sup.supplier_id = po.supplier_id
GROUP BY
    sup.supplier_id,
    sup.supplier_name;


-- ============================================================================
-- View 10: store_category_top3
--
-- Purpose: Top 3 categories by sales revenue for each store.
--          Used by the Comparative Store Performance Dashboard (Problem 5).
--
-- Granularity: up to 3 rows per store (15 rows max for 5 stores).
-- ============================================================================

CREATE OR REPLACE VIEW gold.store_category_top3 AS
WITH store_category_sales AS (
    SELECT
        s.store_id,
        s.store_name,
        p.category,
        SUM(st.total_amount)   AS category_revenue,
        SUM(st.quantity_sold)   AS units_sold,
        COUNT(DISTINCT st.transaction_id) AS transaction_count
    FROM silver.sales_transactions st
    INNER JOIN silver.stores s
        ON st.store_id = s.store_id
    INNER JOIN silver.products p
        ON st.product_id = p.product_id
    GROUP BY
        s.store_id,
        s.store_name,
        p.category
),
ranked AS (
    SELECT
        store_id,
        store_name,
        category,
        category_revenue,
        units_sold,
        transaction_count,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY category_revenue DESC
        ) AS cat_rank
    FROM store_category_sales
)
SELECT
    store_id,
    store_name,
    category,
    ROUND(category_revenue, 2)    AS category_revenue,
    units_sold,
    transaction_count,
    cat_rank
FROM ranked
WHERE cat_rank <= 3
ORDER BY store_id, cat_rank;


-- ============================================================================
-- End of Gold Layer Views
-- ============================================================================
