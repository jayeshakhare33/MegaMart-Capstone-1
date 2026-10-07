-- MegaMart database schema for Databricks Unity Catalog / Delta Lake
-- Catalog (database): megamart_dev
-- Tables are created in the catalog's default schema.
--

CREATE CATALOG IF NOT EXISTS megamart_dev;

USE CATALOG megamart_dev;

CREATE SCHEMA IF NOT EXISTS default;
USE SCHEMA default;

-- ============================================================
-- 1. STORES
-- ============================================================
CREATE TABLE IF NOT EXISTS stores (
    store_id        INT          NOT NULL,
    store_name      VARCHAR(100),
    location        VARCHAR(100),
    manager_name    VARCHAR(100),

    CONSTRAINT pk_stores
        PRIMARY KEY (store_id)
) USING DELTA;


-- ============================================================
-- 2. PRODUCTS
-- ============================================================
CREATE TABLE IF NOT EXISTS products (
    product_id      INT           NOT NULL,
    product_name    VARCHAR(150),
    category        VARCHAR(100),
    unit_price      DECIMAL(10,2),

    CONSTRAINT pk_products
        PRIMARY KEY (product_id)
) USING DELTA;


-- ============================================================
-- 3. SUPPLIERS
-- ============================================================
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     INT           NOT NULL,
    supplier_name   VARCHAR(150),
    contact_person  VARCHAR(100),
    phone           VARCHAR(20),

    CONSTRAINT pk_suppliers
        PRIMARY KEY (supplier_id)
) USING DELTA;


-- ============================================================
-- 4. SALES_TRANSACTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id  INT   NOT NULL,
    store_id        INT,
    product_id      INT,
    quantity_sold   INT,
    sale_date       DATE,

    CONSTRAINT pk_sales_transactions
        PRIMARY KEY (transaction_id),

    CONSTRAINT fk_sales_store
        FOREIGN KEY (store_id)
        REFERENCES stores (store_id),

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
) USING DELTA;


-- ============================================================
-- 5. INVENTORY
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id      INT   NOT NULL,
    store_id          INT,
    product_id        INT,
    quantity_on_hand  INT,

    CONSTRAINT pk_inventory
        PRIMARY KEY (inventory_id),

    CONSTRAINT fk_inventory_store
        FOREIGN KEY (store_id)
        REFERENCES stores (store_id),

    CONSTRAINT fk_inventory_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
) USING DELTA;


-- ============================================================
-- 6. PURCHASE_ORDERS
-- ============================================================
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id          INT   NOT NULL,
    supplier_id    INT,
    order_date     DATE,
    delivery_date  DATE,

    CONSTRAINT pk_purchase_orders
        PRIMARY KEY (po_id),

    CONSTRAINT fk_purchase_orders_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id)
) USING DELTA;
