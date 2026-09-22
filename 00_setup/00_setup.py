# Databricks notebook source
# COMMAND ----------
"""
MegaMart Databricks Capstone
Stage: 00 - Project Setup

Purpose:
    Prepare the Databricks environment used by the MegaMart pipeline.

Responsibilities:
    1. Create the project catalog.
    2. Create Bronze, Silver, Gold, and Audit schemas.
    3. Create the Unity Catalog volume used for raw source files.
    4. Verify that the required objects exist.

This notebook does NOT:
    - ingest CSV files
    - clean data
    - transform data
    - create business/analytical tables
"""

# COMMAND ----------

# =========================
# 1. Project Configuration
# =========================

CATALOG_NAME = "megamart_dev"

SCHEMAS = [
    "bronze",
    "silver",
    "gold",
    "audit",
]

RAW_VOLUME_NAME = "raw_files"

RAW_VOLUME_FULL_NAME = (
    f"{CATALOG_NAME}.bronze.{RAW_VOLUME_NAME}"
)

RAW_VOLUME_PATH = (
    f"/Volumes/{CATALOG_NAME}/bronze/{RAW_VOLUME_NAME}"
)

print(f"Catalog     : {CATALOG_NAME}")
print(f"Schemas     : {SCHEMAS}")
print(f"Raw volume  : {RAW_VOLUME_FULL_NAME}")
print(f"Raw path    : {RAW_VOLUME_PATH}")


# COMMAND ----------

# =========================
# 2. Create Project Catalog
# =========================

print(f"Creating catalog: {CATALOG_NAME}")

try:
    spark.sql(
        f"""
        CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
        """
    )

    print(f"Catalog ready: {CATALOG_NAME}")

except Exception as exc:
    raise RuntimeError(
        f"""
Unable to create catalog '{CATALOG_NAME}'.

This normally means your Databricks user does not have
permission to create catalogs.

Ask your Databricks administrator/instructor for:
    - access to an existing catalog, OR
    - permission to create the project catalog.

Original error:
{exc}
"""
    ) from exc


# COMMAND ----------

# =========================
# 3. Use Project Catalog
# =========================

spark.sql(
    f"""
    USE CATALOG `{CATALOG_NAME}`
    """
)

print(f"Using catalog: {CATALOG_NAME}")


# COMMAND ----------

# =========================
# 4. Create Schemas
# =========================

for schema_name in SCHEMAS:
    print(f"Creating schema: {CATALOG_NAME}.{schema_name}")

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
            `{CATALOG_NAME}`.`{schema_name}`
        """
    )

print("All project schemas are ready.")


# COMMAND ----------

# =========================
# 5. Create Raw Data Volume
# =========================

print(f"Creating volume: {RAW_VOLUME_FULL_NAME}")

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
        `{CATALOG_NAME}`.`bronze`.`{RAW_VOLUME_NAME}`
    """
)

print(f"Raw volume ready: {RAW_VOLUME_FULL_NAME}")


# COMMAND ----------

# =========================
# 6. Verify Catalog
# =========================

print("\n=== Catalog Verification ===")

catalog_result = spark.sql(
    f"""
    SHOW CATALOGS
    """
)

catalog_result.filter(
    catalog_result.catalog == CATALOG_NAME
).show(truncate=False)


# COMMAND ----------

# =========================
# 7. Verify Schemas
# =========================

print("\n=== Schema Verification ===")

schema_result = spark.sql(
    f"""
    SHOW SCHEMAS IN `{CATALOG_NAME}`
    """
)

schema_result.show(truncate=False)


# COMMAND ----------

# =========================
# 8. Verify Volume
# =========================

print("\n=== Volume Verification ===")

volume_result = spark.sql(
    f"""
    SHOW VOLUMES IN
        `{CATALOG_NAME}`.`bronze`
    """
)

volume_result.show(truncate=False)


# COMMAND ----------

# =========================
# 9. Final Environment Check
# =========================

required_objects = [
    f"{CATALOG_NAME}.bronze",
    f"{CATALOG_NAME}.silver",
    f"{CATALOG_NAME}.gold",
    f"{CATALOG_NAME}.audit",
    RAW_VOLUME_FULL_NAME,
]

print("\n=== Required Project Objects ===")

for object_name in required_objects:
    print(f"✓ {object_name}")

print("\n========================================")
print("MegaMart project setup completed.")
print("========================================")
print(f"Raw files should be placed under:")
print(RAW_VOLUME_PATH)