"""
MegaMart Databricks Capstone
Project Configuration

Purpose
-------
Central configuration for the entire MegaMart data pipeline.

Design rule
-----------
Pipeline notebooks should import configuration values from this file
instead of hard-coding project-specific settings.
"""


# =============================================================================
# PROJECT
# =============================================================================

PROJECT_NAME = "MegaMart Supermarket Chain Data Engineering Capstone"

ENVIRONMENT = "dev"


# =============================================================================
# UNITY CATALOG
# =============================================================================

CATALOG_NAME = "megamart_dev"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
AUDIT_SCHEMA = "audit"


# =============================================================================
# UNITY CATALOG VOLUME
# =============================================================================

RAW_VOLUME_NAME = "raw_files"

RAW_VOLUME_PATH = (
    f"/Volumes/"
    f"{CATALOG_NAME}/"
    f"{BRONZE_SCHEMA}/"
    f"{RAW_VOLUME_NAME}"
)


# =============================================================================
# SOURCE FILES
# =============================================================================

SOURCE_FILES = {
    "stores": "stores.csv",
    "products": "products.csv",
    "suppliers": "suppliers.csv",
    "sales_transactions": "sales_transactions.csv",
    "inventory": "inventory.csv",
    "purchase_orders": "purchase_orders.csv",
}


# =============================================================================
# SOURCE TABLE NAMES
#
# These are the Bronze table names that will be created during ingestion.
# =============================================================================

BRONZE_TABLES = {
    "stores": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.stores_raw",
    "products": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products_raw",
    "suppliers": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.suppliers_raw",
    "sales_transactions": (
        f"{CATALOG_NAME}.{BRONZE_SCHEMA}.sales_transactions_raw"
    ),
    "inventory": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.inventory_raw",
    "purchase_orders": (
        f"{CATALOG_NAME}.{BRONZE_SCHEMA}.purchase_orders_raw"
    ),
}


# =============================================================================
# SILVER TABLE NAMES
# =============================================================================

SILVER_TABLES = {
    "stores": f"{CATALOG_NAME}.{SILVER_SCHEMA}.stores",
    "products": f"{CATALOG_NAME}.{SILVER_SCHEMA}.products",
    "suppliers": f"{CATALOG_NAME}.{SILVER_SCHEMA}.suppliers",
    "sales_transactions": (
        f"{CATALOG_NAME}.{SILVER_SCHEMA}.sales_transactions"
    ),
    "inventory": f"{CATALOG_NAME}.{SILVER_SCHEMA}.inventory",
    "purchase_orders": (
        f"{CATALOG_NAME}.{SILVER_SCHEMA}.purchase_orders"
    ),
}


# =============================================================================
# AUDIT TABLES
# =============================================================================

AUDIT_TABLES = {
    "pipeline_runs": (
        f"{CATALOG_NAME}.{AUDIT_SCHEMA}.pipeline_runs"
    ),
    "data_quality_issues": (
        f"{CATALOG_NAME}.{AUDIT_SCHEMA}.data_quality_issues"
    ),
}


# =============================================================================
# GOLD TABLE NAMES
# =============================================================================

GOLD_TABLES = {
    "store_sales_summary": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.store_sales_summary"
    ),
    "top_products": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_products"
    ),
    "store_revenue_ranking": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.store_revenue_ranking"
    ),
    "monthly_store_sales": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.monthly_store_sales"
    ),
    "product_store_sales": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.product_store_sales"
    ),
    "store_product_diversity": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.store_product_diversity"
    ),
    "inventory_status": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.inventory_status"
    ),
    "product_velocity": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.product_velocity"
    ),
    "supplier_scorecard": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.supplier_scorecard"
    ),
    "reorder_recommendations": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.reorder_recommendations"
    ),
    "store_kpis": (
        f"{CATALOG_NAME}.{GOLD_SCHEMA}.store_kpis"
    ),
}


# =============================================================================
# BUSINESS DATE RANGE
#
# The PRD defines the project data period as January 2024 through
# December 2024.
# =============================================================================

PROJECT_START_DATE = "2024-01-01"
PROJECT_END_DATE = "2024-12-31"


# =============================================================================
# DATA QUALITY RULES
# =============================================================================

# PRD requirement:
# Products with unit_price <= 0 are invalid.
MIN_VALID_UNIT_PRICE = 0.0


# PRD requirement:
# Inventory below 50 units is considered low stock.
LOW_STOCK_THRESHOLD = 50


# Phone numbers will be normalized before validation.
VALID_INDIAN_PHONE_LENGTHS = (10, 12)


# =============================================================================
# INVENTORY CONFIGURATION
# =============================================================================

# Safety stock will initially be calculated by the inventory stage.
# Keep the default at zero until we define the final business rule.
DEFAULT_SAFETY_STOCK = 0.0


# =============================================================================
# SUPPLIER DELIVERY CONFIGURATION
# =============================================================================

# IMPORTANT:
# The PRD asks for supplier "on-time delivery rate", but it does not
# define an SLA threshold.
#
# Therefore we do NOT silently invent an SLA here.
#
# Set this value only after deciding/documenting the business assumption.
ON_TIME_DELIVERY_SLA_DAYS = None


# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================

CSV_READ_OPTIONS = {
    "header": True,
    "inferSchema": False,
    "mode": "PERMISSIVE",
}


# =============================================================================
# GENERAL SETTINGS
# =============================================================================

UNKNOWN_VALUE = "UNKNOWN"

DATE_FORMAT = "yyyy-MM-dd"


# =============================================================================
# DEVELOPMENT / DEBUGGING
# =============================================================================

SHOW_SAMPLE_ROWS = 10