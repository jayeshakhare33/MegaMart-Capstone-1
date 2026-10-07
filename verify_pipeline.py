# Databricks notebook source
# COMMAND ----------
"""
MegaMart Pipeline Verification Script
==================================

Run this AFTER the full pipeline completes to verify that every
layer (Bronze → Silver → Gold → Audit) was created correctly.

Checks performed:
  1. Catalog, schemas, and volume exist
  2. 6 Bronze tables exist with data (MANAGED type)
  3. 6 Silver tables exist with data, correct type, and correct column dtypes
  4. 9 Gold views exist with data (VIEW type)
  5. 1 Gold table (reorder_recommendations) exists with data (MANAGED type)
  6. 2 Audit tables exist with data
  7. No SDP artifacts remain (STREAMING_TABLE, MATERIALIZED_VIEW, __materialization*)
  8. No duplicate transaction_ids in Silver
  9. Printed PASS/FAIL summary

Usage:  Run as a standalone script or as a notebook task in a Job
        after the main pipeline finishes.
"""

# COMMAND ----------

# =========================
# Configuration
# =========================

CATALOG = "megamart_dev"
SCHEMAS = ["bronze", "silver", "gold", "audit"]
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

BRONZE_TABLES = [
    "stores_raw",
    "products_raw",
    "suppliers_raw",
    "sales_transactions_raw",
    "inventory_raw",
    "purchase_orders_raw",
]

SILVER_TABLES = [
    "stores",
    "products",
    "suppliers",
    "sales_transactions",
    "inventory",
    "purchase_orders",
]

GOLD_VIEWS = [
    "store_performance",
    "product_performance",
    "top_products",
    "monthly_store_trends",
    "monthly_kpis",
    "category_kpis",
    "enterprise_kpi",
    "inventory_status",
    "supplier_scorecard",
]

GOLD_TABLES = ["reorder_recommendations"]
AUDIT_TABLES = ["pipeline_runs", "data_quality_issues"]

# Silver column type expectations: {table: {column: expected_type_fragment}}
SILVER_TYPE_CHECKS = {
    "sales_transactions": {
        "transaction_date": "date",
        "quantity": "int",
        "unit_price": "decimal",
    },
    "stores": {
        "store_id": "int",
    },
    "products": {
        "product_id": "int",
        "unit_price": "decimal",
    },
}

# =========================
# Helpers
# =========================

results = []


def check(label, passed, detail=""):
    """Record a check result and print it immediately."""
    icon = "\u2705" if passed else "\u274c"
    msg = f"{icon} {label}"
    if detail:
        msg += f" \u2014 {detail}"
    print(msg)
    results.append((label, passed))


def get_object_type(schema, name):
    """Return the table_type from information_schema, or None if not found."""
    rows = spark.sql(
        f"""
        SELECT table_type
        FROM system.information_schema.tables
        WHERE table_catalog = '{CATALOG}'
          AND table_schema   = '{schema}'
          AND table_name     = '{name}'
        """
    ).collect()
    if rows:
        return rows[0]["table_type"]
    return None


def get_row_count(schema, name):
    """Return row count for a table or view."""
    return spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{schema}.{name}"
    ).collect()[0]["cnt"]


# COMMAND ----------

# =========================
# 1. Catalog, Schemas, Volume
# =========================

print("\n" + "=" * 60)
print("1. CATALOG, SCHEMAS, VOLUME")
print("=" * 60)

# Catalog
try:
    catalog_names = [r["catalog"] for r in spark.sql("SHOW CATALOGS").collect()]
    check("Catalog exists", CATALOG in catalog_names, f"Found: {catalog_names}")
except Exception as exc:
    check("Catalog exists", False, str(exc)[:120])

# Schemas
try:
    schema_names = [r["databaseName"] for r in spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`").collect()]
    for s in SCHEMAS:
        check(f"Schema {CATALOG}.{s} exists", s in schema_names)
except Exception as exc:
    for s in SCHEMAS:
        check(f"Schema {CATALOG}.{s} exists", False, str(exc)[:120])

# Volume + CSVs
try:
    vol_rows = spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.bronze").collect()
    vol_names = [r["volume_name"] for r in vol_rows]
    check("Volume raw_files exists", "raw_files" in vol_names)
except Exception as exc:
    check("Volume raw_files exists", False, str(exc)[:120])

try:
    files = dbutils.fs.ls(VOLUME_PATH)
    csv_files = [f.name for f in files if f.name.endswith(".csv")]
    expected_csvs = [
        "stores.csv",
        "products.csv",
        "suppliers.csv",
        "sales_transactions.csv",
        "inventory.csv",
        "purchase_orders.csv",
    ]
    missing = [f for f in expected_csvs if f not in csv_files]
    check(
        "All 6 CSV files present",
        len(missing) == 0,
        f"Found {len(csv_files)} CSVs" + (f", missing: {missing}" if missing else ""),
    )
except Exception as exc:
    check("All 6 CSV files present", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 2. Bronze Tables (6 MANAGED)
# =========================

print("\n" + "=" * 60)
print("2. BRONZE TABLES (6 expected, all MANAGED)")
print("=" * 60)

for table in BRONZE_TABLES:
    try:
        obj_type = get_object_type("bronze", table)
        if obj_type is None:
            check(f"bronze.{table}", False, "Not found")
            continue
        count = get_row_count("bronze", table)
        type_ok = obj_type == "MANAGED"
        check(
            f"bronze.{table} ({count} rows)",
            type_ok and count > 0,
            f"type={obj_type}, rows={count}",
        )
    except Exception as exc:
        check(f"bronze.{table}", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 3. Silver Tables (6 MANAGED + type checks)
# =========================

print("\n" + "=" * 60)
print("3. SILVER TABLES (6 expected, all MANAGED)")
print("=" * 60)

for table in SILVER_TABLES:
    try:
        obj_type = get_object_type("silver", table)
        if obj_type is None:
            check(f"silver.{table}", False, "Not found")
            continue
        count = get_row_count("silver", table)
        type_ok = obj_type == "MANAGED"
        check(
            f"silver.{table} ({count} rows)",
            type_ok and count > 0,
            f"type={obj_type}, rows={count}",
        )
    except Exception as exc:
        check(f"silver.{table}", False, str(exc)[:120])

# Column-level type checks for key Silver tables
print("\n--- Silver Column Type Checks ---")
for table, type_checks in SILVER_TYPE_CHECKS.items():
    try:
        desc_rows = spark.sql(f"DESCRIBE TABLE {CATALOG}.silver.{table}").collect()
        col_types = {}
        for row in desc_rows:
            col_name = row[0]
            data_type = row[1]
            if col_name and not col_name.startswith("#") and col_name != "":
                col_types[col_name] = data_type
        for col, expected in type_checks.items():
            actual = col_types.get(col, "NOT FOUND")
            passed = expected.lower() in actual.lower()
            check(
                f"silver.{table}.{col} type",
                passed,
                f"Expected ~{expected}, got {actual}",
            )
    except Exception as exc:
        check(f"silver.{table} type check", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 4. Gold Views (9 VIEW)
# =========================

print("\n" + "=" * 60)
print("4. GOLD VIEWS (9 expected, all VIEW)")
print("=" * 60)

for view in GOLD_VIEWS:
    try:
        obj_type = get_object_type("gold", view)
        if obj_type is None:
            check(f"gold.{view}", False, "Not found")
            continue
        count = get_row_count("gold", view)
        type_ok = obj_type == "VIEW"
        check(
            f"gold.{view} ({count} rows)",
            type_ok and count > 0,
            f"type={obj_type}, rows={count}",
        )
    except Exception as exc:
        check(f"gold.{view}", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 5. Gold Table (1 MANAGED)
# =========================

print("\n" + "=" * 60)
print("5. GOLD TABLE (1 expected, MANAGED)")
print("=" * 60)

for table in GOLD_TABLES:
    try:
        obj_type = get_object_type("gold", table)
        if obj_type is None:
            check(f"gold.{table}", False, "Not found")
            continue
        count = get_row_count("gold", table)
        type_ok = obj_type == "MANAGED"
        check(
            f"gold.{table} ({count} rows)",
            type_ok and count > 0,
            f"type={obj_type}, rows={count}",
        )
    except Exception as exc:
        check(f"gold.{table}", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 6. Audit Tables (2 MANAGED)
# =========================

print("\n" + "=" * 60)
print("6. AUDIT TABLES (2 expected, MANAGED)")
print("=" * 60)

for table in AUDIT_TABLES:
    try:
        obj_type = get_object_type("audit", table)
        if obj_type is None:
            check(f"audit.{table}", False, "Not found")
            continue
        count = get_row_count("audit", table)
        check(
            f"audit.{table} ({count} rows)",
            count > 0,
            f"type={obj_type}, rows={count}",
        )
    except Exception as exc:
        check(f"audit.{table}", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 7. No SDP Artifacts
# =========================

print("\n" + "=" * 60)
print("7. NO SDP ARTIFACTS")
print("=" * 60)

try:
    sdp_rows = spark.sql(
        f"""
        SELECT table_schema, table_name, table_type
        FROM system.information_schema.tables
        WHERE table_catalog = '{CATALOG}'
          AND table_schema != 'information_schema'
          AND (
                table_type IN ('STREAMING_TABLE', 'MATERIALIZED_VIEW')
             OR table_name LIKE '__materialization%'
             OR table_name LIKE 'event_log%'
          )
        """
    ).collect()

    if len(sdp_rows) == 0:
        check("No SDP artifacts found", True, "Clean catalog")
    else:
        names = [f"{r['table_schema']}.{r['table_name']} ({r['table_type']})" for r in sdp_rows]
        check(
            "No SDP artifacts found",
            False,
            f"Found {len(sdp_rows)}: {names[:5]}",
        )
except Exception as exc:
    check("No SDP artifacts found", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 8. No Duplicates in Silver.sales_transactions
# =========================

print("\n" + "=" * 60)
print("8. DATA QUALITY \u2014 NO DUPLICATES")
print("=" * 60)

try:
    dup_rows = spark.sql(
        f"""
        SELECT transaction_id, COUNT(*) AS dup_count
        FROM {CATALOG}.silver.sales_transactions
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
        """
    ).collect()

    check(
        "No duplicate transaction_ids",
        len(dup_rows) == 0,
        f"Found {len(dup_rows)} duplicates" if dup_rows else "All unique",
    )
except Exception as exc:
    check("No duplicate transaction_ids", False, str(exc)[:120])


# COMMAND ----------

# =========================
# 9. Summary Report
# =========================

print("\n" + "=" * 60)
print("VERIFICATION SUMMARY")
print("=" * 60)

total = len(results)
passed = sum(1 for _, p in results if p)
failed = total - passed

print(f"\nTotal checks: {total}")
print(f"Passed:      {passed}")
print(f"Failed:      {failed}")

if failed > 0:
    print("\n--- Failed Checks ---")
    for label, p in results:
        if not p:
            print(f"  \u274c {label}")
    print(f"\n{'=' * 60}")
    print(f"VERIFICATION FAILED \u2014 {failed} check(s) need attention.")
    print(f"{'=' * 60}")
else:
    print(f"\n{'=' * 60}")
    print(f"VERIFICATION PASSED \u2014 All {total} checks successful!")
    print(f"{'=' * 60}")