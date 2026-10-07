-- ============================================================================
-- MegaMart Databricks Capstone
-- Subproject: Inventory Optimization
-- File: inventory_analysis.sql
--
-- Purpose:
--   Answer six inventory-optimization business questions using the cleaned
--   Silver-layer tables and Gold-layer views.
--
-- Source tables (Unity Catalog: megamart_dev.silver):
--   megamart_dev.silver.inventory             (inventory_id, store_id, product_id, quantity_on_hand)
--   megamart_dev.silver.products             (product_id, product_name, category, unit_price, supplier_id)
--   megamart_dev.silver.stores               (store_id, store_name, location, manager_name)
--   megamart_dev.silver.suppliers            (supplier_id, supplier_name, contact_person, phone)
--   megamart_dev.silver.sales_transactions   (transaction_id, store_id, product_id, quantity_sold, unit_price, total_amount, sale_date)
--   megamart_dev.silver.purchase_orders      (po_id, supplier_id, order_date, delivery_date, po_status, lead_time_days)
--
-- Gold-layer views (Unity Catalog: megamart_dev.gold):
--   megamart_dev.gold.inventory_status        — stock status by store x product (70 rows)
--   megamart_dev.gold.product_performance     — sales velocity by store x product (70 rows)
--   megamart_dev.gold.supplier_scorecard      — supplier delivery performance (5 rows)
--
-- Gold-layer table (produced by inventory_optimizer.py):
--   megamart_dev.gold.reorder_recommendations — reorder point recommendations (70 rows)
--
-- Data period: 2024-01-01 to 2024-12-31
--
-- Notes on data limitations:
--   1. Purchase orders do not contain product_id, quantity_ordered, or a
--      promised/SLA delivery date.  Therefore supplier reliability is measured
--      by the percentage of POs with a non-null delivery_date (delivered vs.
--      pending), not by an on-time SLA.
--   2. The source generator does NOT provide a product unit cost, so true
--      profitability cannot be computed.  Inventory analysis focuses on units
--      and velocity, not cost.
-- ============================================================================


USE CATALOG megamart_dev;


-- ============================================================================
-- QUESTION 22: Which products are currently out of stock or low stock (< 50 units)?
--
-- View used : gold.inventory_status
-- Logic    : quantity_on_hand = 0  -> OUT_OF_STOCK
--            quantity_on_hand < 50 -> LOW_STOCK
-- ============================================================================

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
FROM gold.inventory_status
WHERE stock_status IN ('OUT_OF_STOCK', 'LOW_STOCK')
ORDER BY
    stock_priority DESC,
    quantity_on_hand ASC,
    store_id,
    product_id;


-- ============================================================================
-- QUESTION 23: Which products have the highest sales velocity (fastest moving)?
--
-- View used : gold.product_performance
-- Logic    : velocity_class = 'FAST_MOVING' means the store-product combination
--            falls in the top 20th percentile by total quantity sold across
--            all store-product combinations.
--            average_daily_sales = total_quantity_sold / 366 (2024 is a leap year)
-- ============================================================================

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
FROM gold.product_performance
WHERE velocity_class = 'FAST_MOVING'
ORDER BY
    overall_velocity_rank;


-- ============================================================================
-- QUESTION 24: Which products are slow-moving and overstock candidates?
--
-- View used : gold.product_performance
-- Logic    : velocity_class = 'SLOW_MOVING' means the store-product combination
--            falls in the bottom 20th percentile by total quantity sold.
--            Overstock candidates = slow-moving AND quantity_on_hand >= 50
--            (i.e., plenty of stock but very low sales velocity).
-- ============================================================================

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
FROM gold.product_performance
WHERE velocity_class = 'SLOW_MOVING'
  AND quantity_on_hand >= 50
ORDER BY
    average_daily_sales ASC,
    quantity_on_hand DESC;


-- ============================================================================
-- QUESTION 25: What is the average days to deliver for each supplier?
--
-- View used : gold.supplier_scorecard
-- Logic    : average_lead_time_days is computed from delivered purchase orders
--            only (those with a non-null delivery_date).
--            lead_time_days = delivery_date - order_date (pre-computed in Silver).
-- ============================================================================

SELECT
    supplier_id,
    supplier_name,
    total_purchase_orders,
    delivered_orders,
    ROUND(average_lead_time_days, 2)  AS avg_days_to_deliver,
    minimum_lead_time_days            AS min_days_to_deliver,
    maximum_lead_time_days            AS max_days_to_deliver
FROM gold.supplier_scorecard
ORDER BY
    average_lead_time_days ASC,
    supplier_id;


-- ============================================================================
-- QUESTION 26: Which suppliers are most reliable (on-time delivery rate)?
--
-- View used : gold.supplier_scorecard
-- Logic    : delivery_reliability_pct = delivered_orders / total_purchase_orders * 100
--            Because no SLA threshold is defined in the source data, reliability
--            is measured as the percentage of POs that have been delivered
--            (delivery_date IS NOT NULL), not as on-time-vs-SLA.
--            The reliability_definition column documents this explicitly.
-- ============================================================================

SELECT
    supplier_id,
    supplier_name,
    total_purchase_orders,
    delivered_orders,
    failed_or_incomplete_orders,
    delivery_reliability_pct,
    ROUND(average_lead_time_days, 2)  AS avg_lead_time_days,
    reliability_definition
FROM gold.supplier_scorecard
ORDER BY
    delivery_reliability_pct DESC,
    average_lead_time_days ASC,
    supplier_id;


-- ============================================================================
-- QUESTION 27: What is the optimal reorder point for each product-store combination?
--
-- Table used : gold.reorder_recommendations
--             (produced by inventory_optimizer.py)
--
-- Formula   :
--   Safety Stock     = Z * DailyDemandStdDev * sqrt(LeadTime)
--   Reorder Point    = AverageDailyDemand * LeadTime + SafetyStock
--   Recommended Qty  = max(ReorderPoint - QuantityOnHand, 0)
--
-- Z = 1.65 (approx. 95% one-sided service level)
--
-- Only rows where a reorder is recommended (recommendation_status = 'REORDER')
-- are returned, sorted by priority then recommended quantity.
-- ============================================================================

SELECT
    store_id,
    store_name,
    product_id,
    product_name,
    category,
    supplier_id,
    supplier_name,
    quantity_on_hand,
    average_daily_demand,
    planning_lead_time_days,
    safety_stock,
    reorder_point,
    recommended_order_quantity,
    supplier_delivery_reliability_pct,
    replenishment_priority,
    recommendation_status,
    service_level_z,
    reorder_method
FROM gold.reorder_recommendations
WHERE recommendation_status = 'REORDER'
ORDER BY
    CASE replenishment_priority
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        ELSE 4
    END,
    recommended_order_quantity DESC,
    store_id,
    product_id;


-- ============================================================================
-- BONUS: Store-level inventory summary
--
-- Shows how many products at each store are out-of-stock, low-stock, or
-- healthy, plus total quantity on hand.
-- ============================================================================

SELECT
    store_id,
    store_name,
    COUNT(*) AS total_products,
    SUM(CASE WHEN stock_status = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS out_of_stock_products,
    SUM(CASE WHEN stock_status = 'LOW_STOCK'   THEN 1 ELSE 0 END) AS low_stock_products,
    SUM(CASE WHEN stock_status = 'HEALTHY'     THEN 1 ELSE 0 END) AS healthy_stock_products,
    SUM(quantity_on_hand) AS total_quantity_on_hand
FROM gold.inventory_status
GROUP BY
    store_id,
    store_name
ORDER BY
    store_id;


-- ============================================================================
-- End of inventory_analysis.sql
-- ============================================================================
