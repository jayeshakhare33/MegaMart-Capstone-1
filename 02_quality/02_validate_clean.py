# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 02 - Data Quality Validation

Purpose
-------
Validate the Bronze datasets produced by Stage 01.

Responsibilities
----------------
1. Validate data types and required fields.
2. Detect duplicate business records.
3. Detect invalid numeric values.
4. Detect invalid dates.
5. Validate referential integrity.
6. Detect source-format inconsistencies.
7. Record all quality findings in the Audit layer.
8. Produce a quality summary for the current pipeline run.

Important
---------
Bronze tables are NEVER modified by this notebook.

The quality stage records what is wrong and what should happen later.
The Silver stage will perform the actual type conversion, standardization,
and cleaning decisions.
"""

# COMMAND ----------

# =========================
# 1. Imports
# =========================

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.project_config import (
    CATALOG_NAME,
    PROJECT_START_DATE,
    PROJECT_END_DATE,
    BRONZE_TABLES,
    AUDIT_TABLES,
)

from shared.common import (
    create_run_id,
)


# COMMAND ----------

# =========================
# 2. Pipeline Configuration
# =========================

PIPELINE_NAME = "megamart_pipeline"
STAGE_NAME = "02_quality"

RUN_ID = create_run_id(STAGE_NAME)

PIPELINE_START_TIME = datetime.now(timezone.utc)

print(f"Pipeline : {PIPELINE_NAME}")
print(f"Stage    : {STAGE_NAME}")
print(f"Run ID   : {RUN_ID}")
print(f"Project period: {PROJECT_START_DATE} -> {PROJECT_END_DATE}")


# COMMAND ----------

# =========================
# 3. Audit Table Names
# =========================

QUALITY_ISSUES_TABLE = AUDIT_TABLES["data_quality_issues"]
PIPELINE_RUNS_TABLE = AUDIT_TABLES["pipeline_runs"]

print(f"Quality issues table: {QUALITY_ISSUES_TABLE}")
print(f"Pipeline runs table : {PIPELINE_RUNS_TABLE}")


# COMMAND ----------

# =========================
# 4. Helper Function
# =========================

def build_issue_df(
    df,
    dataset_name,
    record_id_column,
    condition,
    issue_type,
    severity,
    description,
    action_taken,
):
    """
    Build a standardized DataFrame containing quality issues.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Source DataFrame.

    dataset_name : str
        Logical dataset name.

    record_id_column : str
        Column used to identify the affected record.

    condition : pyspark.sql.Column
        Boolean Spark condition selecting affected rows.

    issue_type : str
        Quality issue category.

    severity : str
        INFO, WARNING, or ERROR.

    description : str
        Explanation of the issue.

    action_taken : str
        What the downstream Silver stage should do.

    Returns
    -------
    pyspark.sql.DataFrame
    """

    return (
        df
        .filter(condition)
        .select(
            F.lit(RUN_ID).alias("run_id"),
            F.lit(dataset_name).alias("dataset"),
            F.col(record_id_column)
            .cast("string")
            .alias("record_identifier"),
            F.lit(issue_type).alias("issue_type"),
            F.lit(severity).alias("severity"),
            F.lit(description).alias("description"),
            F.lit(action_taken).alias("action_taken"),
            F.to_json(
                F.struct(
                    *[
                        F.col(column)
                        for column in df.columns
                    ]
                )
            ).alias("raw_record"),
            F.current_timestamp().alias("detected_at"),
        )
    )


# COMMAND ----------

# =========================
# 5. Load Bronze Tables
# =========================

print("\n=== Loading Bronze Tables ===")

bronze = {}

for dataset_name, table_name in BRONZE_TABLES.items():

    bronze[dataset_name] = spark.table(table_name)

    count = bronze[dataset_name].count()

    print(
        f"✓ {dataset_name:22s} "
        f"{count:>6,} records"
    )


# COMMAND ----------

# =========================
# 6. Referential Reference Data
# =========================

stores_df = bronze["stores"].select(
    F.col("store_id").alias("reference_store_id")
).dropDuplicates()

products_df = bronze["products"].select(
    F.col("product_id").alias("reference_product_id")
).dropDuplicates()

suppliers_df = bronze["suppliers"].select(
    F.col("supplier_id").alias("reference_supplier_id")
).dropDuplicates()


# COMMAND ----------

# =========================
# 7. Stores Quality Checks
# =========================

print("\n=== STORES QUALITY CHECKS ===")

stores = bronze["stores"]

store_issues = []


# Required store_id
store_issues.append(
    build_issue_df(
        stores,
        "stores",
        "store_id",
        (
            F.col("store_id").isNull()
            | F.col("store_id").cast("int").isNull()
        ),
        "INVALID_STORE_ID",
        "ERROR",
        "store_id is missing or cannot be converted to an integer.",
        "QUARANTINE_RECORD",
    )
)


# Duplicate store_id
store_duplicate_window = Window.partitionBy(
    "store_id"
).orderBy(
    F.col("store_id")
)

stores_with_duplicate_rank = stores.withColumn(
    "_duplicate_rank",
    F.row_number().over(store_duplicate_window),
)

store_issues.append(
    build_issue_df(
        stores_with_duplicate_rank,
        "stores",
        "store_id",
        F.col("_duplicate_rank") > 1,
        "DUPLICATE_STORE_ID",
        "ERROR",
        "Multiple records contain the same store_id.",
        "KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES",
    )
)


# Missing manager name
store_issues.append(
    build_issue_df(
        stores,
        "stores",
        "store_id",
        F.col("manager_name").isNull(),
        "MISSING_MANAGER_NAME",
        "WARNING",
        "manager_name is NULL.",
        "STANDARDIZE_TO_UNKNOWN_IN_SILVER",
    )
)


# Combine store issues
stores_quality = store_issues[0]

for issue_df in store_issues[1:]:
    stores_quality = stores_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 8. Products Quality Checks
# =========================

print("\n=== PRODUCTS QUALITY CHECKS ===")

products = bronze["products"]

product_issues = []


# Required product_id
product_issues.append(
    build_issue_df(
        products,
        "products",
        "product_id",
        (
            F.col("product_id").isNull()
            | F.col("product_id").cast("int").isNull()
        ),
        "INVALID_PRODUCT_ID",
        "ERROR",
        "product_id is missing or cannot be converted to an integer.",
        "QUARANTINE_RECORD",
    )
)


# Invalid supplier ID format
product_issues.append(
    build_issue_df(
        products,
        "products",
        "product_id",
        (
            F.col("supplier_id").isNull()
            | F.col("supplier_id").cast("int").isNull()
        ),
        "INVALID_SUPPLIER_ID",
        "ERROR",
        "supplier_id is missing or cannot be converted to an integer.",
        "QUARANTINE_RECORD",
    )
)


# Invalid unit price
product_issues.append(
    build_issue_df(
        products,
        "products",
        "product_id",
        (
            F.col("unit_price").isNull()
            | F.col("unit_price").cast("double").isNull()
            | (
                F.col("unit_price")
                .cast("double")
                <= 0
            )
        ),
        "INVALID_UNIT_PRICE",
        "ERROR",
        "unit_price is missing, non-numeric, zero, or negative.",
        "QUARANTINE_RECORD",
    )
)


# Duplicate product_id
product_duplicate_window = Window.partitionBy(
    "product_id"
).orderBy(
    F.col("product_id")
)

products_with_duplicate_rank = products.withColumn(
    "_duplicate_rank",
    F.row_number().over(product_duplicate_window),
)

product_issues.append(
    build_issue_df(
        products_with_duplicate_rank,
        "products",
        "product_id",
        F.col("_duplicate_rank") > 1,
        "DUPLICATE_PRODUCT_ID",
        "ERROR",
        "Multiple records contain the same product_id.",
        "KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES",
    )
)


# Product -> supplier referential integrity
products_missing_supplier = (
    products
    .join(
        suppliers_df,
        products["supplier_id"]
        == suppliers_df["reference_supplier_id"],
        "left",
    )
)

product_issues.append(
    build_issue_df(
        products_missing_supplier,
        "products",
        "product_id",
        F.col("reference_supplier_id").isNull(),
        "INVALID_SUPPLIER_REFERENCE",
        "ERROR",
        "product.supplier_id does not reference an existing supplier.",
        "QUARANTINE_RECORD",
    )
)


products_quality = product_issues[0]

for issue_df in product_issues[1:]:
    products_quality = products_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 9. Suppliers Quality Checks
# =========================

print("\n=== SUPPLIERS QUALITY CHECKS ===")

suppliers = bronze["suppliers"]

supplier_issues = []


# Supplier ID
supplier_issues.append(
    build_issue_df(
        suppliers,
        "suppliers",
        "supplier_id",
        (
            F.col("supplier_id").isNull()
            | F.col("supplier_id").cast("int").isNull()
        ),
        "INVALID_SUPPLIER_ID",
        "ERROR",
        "supplier_id is missing or cannot be converted to an integer.",
        "QUARANTINE_RECORD",
    )
)


# Duplicate supplier ID
supplier_duplicate_window = Window.partitionBy(
    "supplier_id"
).orderBy(
    F.col("supplier_id")
)

suppliers_with_duplicate_rank = suppliers.withColumn(
    "_duplicate_rank",
    F.row_number().over(supplier_duplicate_window),
)

supplier_issues.append(
    build_issue_df(
        suppliers_with_duplicate_rank,
        "suppliers",
        "supplier_id",
        F.col("_duplicate_rank") > 1,
        "DUPLICATE_SUPPLIER_ID",
        "ERROR",
        "Multiple records contain the same supplier_id.",
        "KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES",
    )
)


# Phone normalization
supplier_phone_check = (
    suppliers
    .withColumn(
        "_phone_digits",
        F.regexp_replace(
            F.col("phone"),
            r"[^0-9]",
            "",
        ),
    )
    .withColumn(
        "_standard_phone",
        F.when(
            F.col("_phone_digits").startswith("91")
            & (F.length("_phone_digits") == 12),
            F.substring(
                F.col("_phone_digits"),
                3,
                10,
            ),
        ).otherwise(
            F.col("_phone_digits")
        ),
    )
)


# Invalid normalized phone
supplier_issues.append(
    build_issue_df(
        supplier_phone_check,
        "suppliers",
        "supplier_id",
        (
            F.col("_standard_phone").isNull()
            | (F.length("_standard_phone") != 10)
        ),
        "INVALID_PHONE_FORMAT",
        "ERROR",
        "Phone number cannot be normalized to a 10-digit Indian phone number.",
        "QUARANTINE_RECORD",
    )
)


# Inconsistent source phone format
supplier_issues.append(
    build_issue_df(
        supplier_phone_check,
        "suppliers",
        "supplier_id",
        (
            F.col("phone").isNotNull()
            & (
                F.col("phone")
                != F.col("_standard_phone")
            )
            & (
                F.length("_standard_phone") == 10
            )
        ),
        "INCONSISTENT_PHONE_FORMAT",
        "WARNING",
        "Phone number uses a non-standard source format.",
        "NORMALIZE_PHONE_IN_SILVER",
    )
)


suppliers_quality = supplier_issues[0]

for issue_df in supplier_issues[1:]:
    suppliers_quality = suppliers_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 10. Sales Quality Checks
# =========================

print("\n=== SALES QUALITY CHECKS ===")

sales = bronze["sales_transactions"]

sales_typed = (
    sales
    .withColumn(
        "_transaction_id_int",
        F.col("transaction_id").cast("long"),
    )
    .withColumn(
        "_store_id_int",
        F.col("store_id").cast("int"),
    )
    .withColumn(
        "_product_id_int",
        F.col("product_id").cast("int"),
    )
    .withColumn(
        "_quantity_int",
        F.col("quantity_sold").cast("int"),
    )
    .withColumn(
        "_unit_price_double",
        F.col("unit_price").cast("double"),
    )
    .withColumn(
        "_total_amount_double",
        F.col("total_amount").cast("double"),
    )
    .withColumn(
        "_sale_date",
        F.to_date(
            F.col("sale_date"),
            "yyyy-MM-dd",
        ),
    )
)


sales_issues = []


# Transaction ID
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_transaction_id_int").isNull()
            | F.col("transaction_id").isNull()
        ),
        "INVALID_TRANSACTION_ID",
        "ERROR",
        "transaction_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Store reference
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_store_id_int").isNull()
            | F.col("store_id").isNull()
        ),
        "INVALID_STORE_REFERENCE",
        "ERROR",
        "store_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Product reference
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_product_id_int").isNull()
            | F.col("product_id").isNull()
        ),
        "INVALID_PRODUCT_REFERENCE",
        "ERROR",
        "product_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Quantity
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_quantity_int").isNull()
            | (F.col("_quantity_int") <= 0)
        ),
        "INVALID_QUANTITY",
        "ERROR",
        "quantity_sold must be a positive integer.",
        "QUARANTINE_RECORD",
    )
)


# Unit price
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_unit_price_double").isNull()
            | (
                F.col("_unit_price_double")
                <= 0
            )
        ),
        "INVALID_SALE_UNIT_PRICE",
        "ERROR",
        "Sales transaction unit_price must be greater than zero.",
        "QUARANTINE_RECORD",
    )
)


# Total amount
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.abs(
                F.col("_total_amount_double")
                - (
                    F.col("_quantity_int")
                    * F.col("_unit_price_double")
                )
            ) > 0.01
        ),
        "INVALID_TOTAL_AMOUNT",
        "ERROR",
        "total_amount does not equal quantity_sold multiplied by unit_price.",
        "QUARANTINE_RECORD",
    )
)


# Sale date
sales_issues.append(
    build_issue_df(
        sales_typed,
        "sales_transactions",
        "transaction_id",
        (
            F.col("_sale_date").isNull()
            | (
                F.col("_sale_date")
                < F.to_date(
                    F.lit(PROJECT_START_DATE)
                )
            )
            | (
                F.col("_sale_date")
                > F.to_date(
                    F.lit(PROJECT_END_DATE)
                )
            )
        ),
        "INVALID_SALE_DATE",
        "ERROR",
        "sale_date is invalid or outside the generated 2024 sales period.",
        "QUARANTINE_RECORD",
    )
)


# Store foreign key
sales_with_store_check = (
    sales_typed
    .join(
        stores_df,
        sales_typed["store_id"]
        == stores_df["reference_store_id"],
        "left",
    )
)

sales_issues.append(
    build_issue_df(
        sales_with_store_check,
        "sales_transactions",
        "transaction_id",
        F.col("reference_store_id").isNull(),
        "INVALID_STORE_REFERENCE",
        "ERROR",
        "sales store_id does not reference an existing store.",
        "QUARANTINE_RECORD",
    )
)


# Product foreign key
sales_with_product_check = (
    sales_typed
    .join(
        products_df,
        sales_typed["product_id"]
        == products_df["reference_product_id"],
        "left",
    )
)

sales_issues.append(
    build_issue_df(
        sales_with_product_check,
        "sales_transactions",
        "transaction_id",
        F.col("reference_product_id").isNull(),
        "INVALID_PRODUCT_REFERENCE",
        "ERROR",
        "sales product_id does not reference an existing product.",
        "QUARANTINE_RECORD",
    )
)


# Exact duplicate business records.
#
# transaction_id is intentionally excluded because the generator
# creates duplicate records using a different transaction_id.

sales_duplicate_window = Window.partitionBy(
    "store_id",
    "product_id",
    "quantity_sold",
    "unit_price",
    "total_amount",
    "sale_date",
).orderBy(
    F.col("transaction_id").cast("long")
)

sales_with_duplicate_rank = sales_typed.withColumn(
    "_duplicate_rank",
    F.row_number().over(sales_duplicate_window),
)

sales_issues.append(
    build_issue_df(
        sales_with_duplicate_rank,
        "sales_transactions",
        "transaction_id",
        F.col("_duplicate_rank") > 1,
        "DUPLICATE_SALES_RECORD",
        "ERROR",
        (
            "Multiple sales records have the same store, product, "
            "quantity, unit price, total amount, and sale date."
        ),
        "KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES",
    )
)


sales_quality = sales_issues[0]

for issue_df in sales_issues[1:]:
    sales_quality = sales_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 11. Inventory Quality Checks
# =========================

print("\n=== INVENTORY QUALITY CHECKS ===")

inventory = bronze["inventory"]

inventory_typed = (
    inventory
    .withColumn(
        "_inventory_id_int",
        F.col("inventory_id").cast("long"),
    )
    .withColumn(
        "_store_id_int",
        F.col("store_id").cast("int"),
    )
    .withColumn(
        "_product_id_int",
        F.col("product_id").cast("int"),
    )
    .withColumn(
        "_quantity_int",
        F.col("quantity_on_hand").cast("int"),
    )
)

inventory_issues = []


# Inventory ID
inventory_issues.append(
    build_issue_df(
        inventory_typed,
        "inventory",
        "inventory_id",
        (
            F.col("_inventory_id_int").isNull()
            | F.col("inventory_id").isNull()
        ),
        "INVALID_INVENTORY_ID",
        "ERROR",
        "inventory_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Store ID
inventory_issues.append(
    build_issue_df(
        inventory_typed,
        "inventory",
        "inventory_id",
        (
            F.col("_store_id_int").isNull()
            | F.col("store_id").isNull()
        ),
        "INVALID_STORE_REFERENCE",
        "ERROR",
        "inventory store_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Product ID
inventory_issues.append(
    build_issue_df(
        inventory_typed,
        "inventory",
        "inventory_id",
        (
            F.col("_product_id_int").isNull()
            | F.col("product_id").isNull()
        ),
        "INVALID_PRODUCT_REFERENCE",
        "ERROR",
        "inventory product_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Quantity
inventory_issues.append(
    build_issue_df(
        inventory_typed,
        "inventory",
        "inventory_id",
        (
            F.col("_quantity_int").isNull()
            | (F.col("_quantity_int") < 0)
        ),
        "INVALID_INVENTORY_QUANTITY",
        "ERROR",
        "quantity_on_hand must be a non-negative integer.",
        "QUARANTINE_RECORD",
    )
)


# Store foreign key
inventory_with_store_check = (
    inventory_typed
    .join(
        stores_df,
        inventory_typed["store_id"]
        == stores_df["reference_store_id"],
        "left",
    )
)

inventory_issues.append(
    build_issue_df(
        inventory_with_store_check,
        "inventory",
        "inventory_id",
        F.col("reference_store_id").isNull(),
        "INVALID_STORE_REFERENCE",
        "ERROR",
        "inventory store_id does not reference an existing store.",
        "QUARANTINE_RECORD",
    )
)


# Product foreign key
inventory_with_product_check = (
    inventory_typed
    .join(
        products_df,
        inventory_typed["product_id"]
        == products_df["reference_product_id"],
        "left",
    )
)

inventory_issues.append(
    build_issue_df(
        inventory_with_product_check,
        "inventory",
        "inventory_id",
        F.col("reference_product_id").isNull(),
        "INVALID_PRODUCT_REFERENCE",
        "ERROR",
        "inventory product_id does not reference an existing product.",
        "QUARANTINE_RECORD",
    )
)


# Duplicate inventory combination
inventory_duplicate_window = Window.partitionBy(
    "store_id",
    "product_id",
).orderBy(
    F.col("inventory_id").cast("long")
)

inventory_with_duplicate_rank = inventory_typed.withColumn(
    "_duplicate_rank",
    F.row_number().over(inventory_duplicate_window),
)

inventory_issues.append(
    build_issue_df(
        inventory_with_duplicate_rank,
        "inventory",
        "inventory_id",
        F.col("_duplicate_rank") > 1,
        "DUPLICATE_STORE_PRODUCT_INVENTORY",
        "ERROR",
        "More than one inventory record exists for a store/product combination.",
        "KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES",
    )
)


inventory_quality = inventory_issues[0]

for issue_df in inventory_issues[1:]:
    inventory_quality = inventory_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 12. Purchase Order Checks
# =========================

print("\n=== PURCHASE ORDER QUALITY CHECKS ===")

purchase_orders = bronze["purchase_orders"]

purchase_orders_typed = (
    purchase_orders
    .withColumn(
        "_po_id_int",
        F.col("po_id").cast("long"),
    )
    .withColumn(
        "_supplier_id_int",
        F.col("supplier_id").cast("int"),
    )
    .withColumn(
        "_order_date",
        F.to_date(
            F.col("order_date"),
            "yyyy-MM-dd",
        ),
    )
    .withColumn(
        "_delivery_date",
        F.to_date(
            F.col("delivery_date"),
            "yyyy-MM-dd",
        ),
    )
)

purchase_order_issues = []


# PO ID
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("_po_id_int").isNull()
            | F.col("po_id").isNull()
        ),
        "INVALID_PO_ID",
        "ERROR",
        "po_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Supplier ID
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("_supplier_id_int").isNull()
            | F.col("supplier_id").isNull()
        ),
        "INVALID_SUPPLIER_REFERENCE",
        "ERROR",
        "supplier_id is missing or invalid.",
        "QUARANTINE_RECORD",
    )
)


# Order date
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("_order_date").isNull()
            | (
                F.col("_order_date")
                < F.to_date(
                    F.lit(PROJECT_START_DATE)
                )
            )
            | (
                F.col("_order_date")
                > F.to_date(
                    F.lit(PROJECT_END_DATE)
                )
            )
        ),
        "INVALID_ORDER_DATE",
        "ERROR",
        "order_date is invalid or outside the generated 2024 order period.",
        "QUARANTINE_RECORD",
    )
)


# Delivery date parsing
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("delivery_date").isNotNull()
            & (F.trim(F.col("delivery_date")) != "")
            & F.col("_delivery_date").isNull()
        ),
        "INVALID_DELIVERY_DATE",
        "ERROR",
        "delivery_date is present but cannot be parsed as yyyy-MM-dd.",
        "QUARANTINE_RECORD",
    )
)


# Pending delivery
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("delivery_date").isNull()
            | (F.trim(F.col("delivery_date")) == "")
        ),
        "PENDING_PURCHASE_ORDER",
        "INFO",
        "delivery_date is missing; the generated source treats this as pending.",
        "CLASSIFY_AS_PENDING_IN_SILVER",
    )
)


# Delivery before order
purchase_order_issues.append(
    build_issue_df(
        purchase_orders_typed,
        "purchase_orders",
        "po_id",
        (
            F.col("_delivery_date").isNotNull()
            & F.col("_order_date").isNotNull()
            & (
                F.col("_delivery_date")
                < F.col("_order_date")
            )
        ),
        "INVALID_DELIVERY_SEQUENCE",
        "ERROR",
        "delivery_date occurs before order_date.",
        "QUARANTINE_RECORD",
    )
)


# Supplier foreign key
purchase_orders_with_supplier_check = (
    purchase_orders_typed
    .join(
        suppliers_df,
        purchase_orders_typed["supplier_id"]
        == suppliers_df["reference_supplier_id"],
        "left",
    )
)

purchase_order_issues.append(
    build_issue_df(
        purchase_orders_with_supplier_check,
        "purchase_orders",
        "po_id",
        F.col("reference_supplier_id").isNull(),
        "INVALID_SUPPLIER_REFERENCE",
        "ERROR",
        "purchase_orders supplier_id does not reference an existing supplier.",
        "QUARANTINE_RECORD",
    )
)


purchase_orders_quality = purchase_order_issues[0]

for issue_df in purchase_order_issues[1:]:
    purchase_orders_quality = purchase_orders_quality.unionByName(
        issue_df
    )


# COMMAND ----------

# =========================
# 13. Combine All Issues
# =========================

print("\n=== Combining Quality Findings ===")

all_quality_issues = (
    stores_quality
    .unionByName(products_quality)
    .unionByName(suppliers_quality)
    .unionByName(sales_quality)
    .unionByName(inventory_quality)
    .unionByName(purchase_orders_quality)
)


# Remove any accidental duplicate issue records.
all_quality_issues = (
    all_quality_issues
    .dropDuplicates(
        [
            "run_id",
            "dataset",
            "record_identifier",
            "issue_type",
        ]
    )
)


# COMMAND ----------

# =========================
# 14. Create Audit Tables
# =========================

print("\n=== Preparing Audit Tables ===")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
        {QUALITY_ISSUES_TABLE}
    (
        run_id STRING,
        dataset STRING,
        record_identifier STRING,
        issue_type STRING,
        severity STRING,
        description STRING,
        action_taken STRING,
        raw_record STRING,
        detected_at TIMESTAMP
    )
    USING DELTA
    """
)


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
        {PIPELINE_RUNS_TABLE}
    (
        run_id STRING,
        pipeline_name STRING,
        stage_name STRING,
        status STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        records_processed BIGINT,
        records_failed BIGINT,
        error_message STRING
    )
    USING DELTA
    """
)


# COMMAND ----------

# =========================
# 15. Write Quality Issues
# =========================

issue_count = all_quality_issues.count()

print(
    f"Total quality findings detected: {issue_count:,}"
)

if issue_count > 0:

    (
        all_quality_issues
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUALITY_ISSUES_TABLE)
    )

    print(
        f"✓ Quality findings written to "
        f"{QUALITY_ISSUES_TABLE}"
    )

else:

    print("✓ No quality findings detected.")


# COMMAND ----------

# =========================
# 16. Quality Summary
# =========================

print("\n=== QUALITY SUMMARY ===")

quality_summary = (
    all_quality_issues
    .groupBy(
        "dataset",
        "severity",
    )
    .count()
    .orderBy(
        "dataset",
        "severity",
    )
)

quality_summary.show(truncate=False)


# COMMAND ----------

# =========================
# 17. Issue-Type Summary
# =========================

print("\n=== ISSUE TYPE SUMMARY ===")

issue_type_summary = (
    all_quality_issues
    .groupBy(
        "issue_type",
        "severity",
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)

issue_type_summary.show(
    100,
    truncate=False,
)


# COMMAND ----------

# =========================
# 18. Error vs Warning vs Info
# =========================

severity_summary = (
    all_quality_issues
    .groupBy("severity")
    .count()
    .orderBy("severity")
)

print("\n=== SEVERITY SUMMARY ===")

severity_summary.show(truncate=False)


# COMMAND ----------

# =========================
# 19. Pipeline Run Statistics
# =========================

records_processed = sum(
    bronze[dataset_name].count()
    for dataset_name in bronze
)

error_count = (
    all_quality_issues
    .filter(
        F.col("severity") == "ERROR"
    )
    .count()
)

warning_count = (
    all_quality_issues
    .filter(
        F.col("severity") == "WARNING"
    )
    .count()
)

info_count = (
    all_quality_issues
    .filter(
        F.col("severity") == "INFO"
    )
    .count()
)

PIPELINE_END_TIME = datetime.now(timezone.utc)

pipeline_status = (
    "SUCCESS_WITH_ERRORS"
    if error_count > 0
    else "SUCCESS"
)

# Use Spark literals with explicit types instead of
# spark.createDataFrame(..., None), which can fail under
# Spark Connect because None has no inferable type.

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

        F.lit(pipeline_status)
        .cast("string")
        .alias("status"),

        F.lit(PIPELINE_START_TIME)
        .cast("timestamp")
        .alias("start_time"),

        F.lit(PIPELINE_END_TIME)
        .cast("timestamp")
        .alias("end_time"),

        F.lit(records_processed)
        .cast("long")
        .alias("records_processed"),

        F.lit(error_count)
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
    .saveAsTable(PIPELINE_RUNS_TABLE)
)


# COMMAND ----------

# =========================
# 20. Display Important Findings
# =========================

print("\n=== IMPORTANT QUALITY FINDINGS ===")

(
    all_quality_issues
    .filter(
        F.col("severity").isin(
            ["ERROR", "WARNING"]
        )
    )
    .select(
        "dataset",
        "record_identifier",
        "issue_type",
        "severity",
        "description",
        "action_taken",
    )
    .orderBy(
        "dataset",
        "record_identifier",
        "issue_type",
    )
    .show(
        100,
        truncate=False,
    )
)


# COMMAND ----------

# =========================
# 21. Final Status
# =========================

print("\n========================================")
print("Data quality validation completed.")
print("========================================")

print(f"Run ID            : {RUN_ID}")
print(f"Records processed : {records_processed:,}")
print(f"Errors            : {error_count:,}")
print(f"Warnings          : {warning_count:,}")
print(f"Info findings     : {info_count:,}")
print(f"Status            : {pipeline_status}")

print("\nAudit tables:")

print(f"✓ {QUALITY_ISSUES_TABLE}")
print(f"✓ {PIPELINE_RUNS_TABLE}")

print("\nBronze tables were not modified.")