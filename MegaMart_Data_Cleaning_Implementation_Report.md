# MegaMart Data Cleaning and Validation Decisions
## Project Implementation Summary

This document records the actual cleaning, validation, and transformation decisions implemented in the MegaMart pipeline during the Bronze-to-Silver stage.

The project follows the source-of-truth rule documented in the data quality workflow:
- Bronze preserves the raw generated data without modification
- The quality stage validates and records issues in the audit layer
- Silver applies the approved cleaning logic and produces the analysis-ready dataset

---

## 1. Data Quality Pipeline Approach

The implemented pipeline follows this pattern:

Generated CSV files

        |
        v
Unity Catalog Volume
        
        |
        v
Bronze Delta tables
        
        |
        v
Quality validation
        
        |
        +----> Audit findings
        |
        v
Silver Delta tables

### Bronze principle
Bronze stores the raw generated data and is not modified by the quality stage.

### Quality principle
The quality stage detects and records issues in:
- `megamart_dev.audit.data_quality_issues`

and records execution in:
- `megamart_dev.audit.pipeline_runs`

### Silver principle
Silver applies the documented cleaning and standardization decisions and stores the corrected analytical dataset.

---

## 2. Actual Quality Findings from the Project Run

The completed quality run processed:
- Stores: 5
- Products: 14
- Suppliers: 5
- Sales transactions: 2,004
- Inventory: 70
- Purchase orders: 300
- Total records processed across Bronze tables: 2,398

The project detected 76 findings:

- ERROR: 16
- WARNING: 3
- INFO: 57

The actual issue summary was:

| Dataset | Issue | Severity | Count |
|---|---|---:|---:|
| Products | Invalid unit price | ERROR | 2 |
| Sales transactions | Duplicate sales business record | ERROR | 14 |
| Stores | Missing manager name | WARNING | 1 |
| Suppliers | Inconsistent phone format | WARNING | 2 |
| Purchase orders | Pending purchase order | INFO | 57 |

---

## 3. Implemented Cleaning and Validation Decisions

### 3.1 Duplicate transaction records
The project identified duplicate business records in the sales table.  
The duplicate logic did not rely only on `transaction_id`, because the generator intentionally creates duplicate-like business records with different IDs.

The actual duplicate check compares:
- `store_id`
- `product_id`
- `quantity_sold`
- `unit_price`
- `total_amount`
- `sale_date`

### Decision implemented
The pipeline keeps the first record in each duplicate business group and removes subsequent duplicates from Silver.

Action:
- `KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES`

### Why this was implemented
The project determined that same-store, same-product, same-date, same-quantity, same-price records represent the same business event even when the generated `transaction_id` differs.

### Result
- Bronze sales transactions: 2,004
- Silver sales transactions: 1,990
- Duplicates removed: 14

---

### 3.2 Missing manager name
One store record had a missing `manager_name`.

### Decision implemented
The Silver version replaces missing manager names with:
- `UNKNOWN`

### Why this was implemented
The record remains valid as a store, and the missing manager value does not invalidate the store itself. Replacing the NULL value keeps the record usable for analysis while preserving the missingness in Bronze and audit logs.

### Result
Store 4 appears in Silver with:
- `manager_name = UNKNOWN`

---

### 3.3 Pending purchase orders
The generated dataset includes purchase orders with blank `delivery_date` values.  
The project found that these are treated as pending orders rather than cancelled orders.

### Decision implemented
Blank `delivery_date` is classified as:
- `PENDING`

and not as cancelled.

### Why this was implemented
The data generator defines these as pending orders. There was no evidence in the data that a blank delivery date means cancellation.

### Result
- Delivered: 243
- Pending: 57

For pending orders, `lead_time_days` is kept as `NULL` in Silver.

---

### 3.4 Invalid product prices
Two product records contained invalid prices:
- `product_id = 1005` -> `unit_price = -100.00`
- `product_id = 1007` -> `unit_price = 0.00`

### Decision implemented
The project retained the product record but converted the invalid price to:
- `NULL`

and marked the row as:
- `PRICE_QUARANTINED`

### Why this was implemented
The project decided not to delete the full product record because it still contains valid product metadata and should remain available for master-data analysis. Only the invalid price attribute is quarantined.

### Result
- Valid products: 12
- Price quarantined: 2

---

### 3.5 Inconsistent supplier phone formats
The supplier phone values were generated in inconsistent formats, including:
- `9000000001`
- `+91-9000000002`
- `919000000004`

### Decision implemented
The phone values are normalized in Silver to a standard 10-digit representation.

### Why this was implemented
The issue is formatting inconsistency, not necessarily invalid supplier data. The project treats it as a warning and normalizes it to keep the data consistent for reporting.

---

### 3.6 Inventory zero stock
The generator intentionally creates inventory rows with zero stock.

### Decision implemented
The project does not treat zero stock as invalid data. These records remain in Silver as valid business cases.

### Why this was implemented
Zero inventory is a meaningful retail condition, especially for stockout analysis and inventory optimization. Removing these rows would remove important business signals.

---

### 3.7 Future delivery dates
The generator creates purchase orders near year-end with delivery dates in the following year.

Example:
- `order_date = 2024-12-30`
- `delivery_date = 2025-01-04`

### Decision implemented
The project does not reject a delivery date simply because it falls in the next year.  
The validation checks:
- that the date is parseable
- that `delivery_date >= order_date`

### Why this was implemented
A future delivery date is valid when it is consistent with the actual order period and the generator’s delivery window.

---

## 4. Rules Applied Per Table

### 4.1 Stores
Implemented validations:
- `store_id` must be unique and not null
- `store_name` cannot be empty
- `location` cannot be empty
- `manager_name` may be converted to `UNKNOWN` if missing

### 4.2 Products
Implemented validations:
- `product_id` must be unique and not null
- `product_name` cannot be empty
- `category` cannot be empty
- `unit_price` must be greater than zero; otherwise set to `NULL`

### 4.3 Suppliers
Implemented validations:
- `supplier_id` must be unique and not null
- `supplier_name` cannot be empty
- `phone` is normalized if inconsistent
- missing contact values may be set to `UNKNOWN`

### 4.4 Sales transactions
Implemented validations:
- `transaction_id` must be unique and not null
- `store_id` must exist in the stores table
- `product_id` must exist in the products table
- `quantity_sold` must be greater than zero
- `sale_date` must be valid

### 4.5 Inventory
Implemented validations:
- `inventory_id` must be unique and not null
- `store_id` and `product_id` must exist
- `quantity_on_hand` must be greater than or equal to zero

### 4.6 Purchase orders
Implemented validations:
- `po_id` must be unique and not null
- `supplier_id` must exist
- `order_date` must be valid
- `delivery_date` may be null for pending orders
- if present, `delivery_date` must be greater than or equal to `order_date`

---

## 5. Final Silver Output

After the implemented cleaning logic was applied, the resulting Silver tables were:

- Silver stores: 5
- Silver products: 14
- Silver suppliers: 5
- Silver sales_transactions: 1,990
- Silver inventory: 70
- Silver purchase_orders: 300

---

## 6. Auditability

All quality findings are written to:
- `megamart_dev.audit.data_quality_issues`

and pipeline execution records are written to:
- `megamart_dev.audit.pipeline_runs`

This ensures traceability of:
- what was cleaned
- why it was cleaned
- which issue caused the transformation
- what happened to the original raw record

---

## 7. Summary of Implementation

The actual project pipeline does not simply remove all bad data. It follows a practical data quality strategy:

- keep raw Bronze data untouched
- record every data issue in audit tables
- fix or quarantine only the fields that are invalid
- preserve structurally valid records
- treat intentional business situations such as zero stock and pending orders as valid conditions
- maintain traceability for every cleaning decision

This approach aligns with the project’s business goal of producing a reliable analytical dataset without losing useful operational context.
