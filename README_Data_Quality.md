# MegaMart Capstone — Data Quality Handling

## Purpose

This document records the observations, decisions, and justification used while moving the MegaMart data from **Bronze to Silver**.

**Source-of-truth rule:** the uploaded `generate_sample_data.py` is the authoritative source for the actual dataset, fields, record-generation behavior, and intentionally injected issues. The PRD is used for business/project requirements, but it does not override the generator when the two differ.

---

## Data-quality pipeline approach

The project uses the following pattern:

```text
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
```

### Bronze principle

Bronze preserves the source data and is not modified by the quality stage.

### Quality principle

The quality stage detects and records issues in:

```text
megamart_dev.audit.data_quality_issues
```

and records pipeline execution in:

```text
megamart_dev.audit.pipeline_runs
```

### Silver principle

Silver applies the documented cleaning and standardization decisions.

---

# Quality findings from the current run

The completed quality run processed:

- Stores: 5
- Products: 14
- Suppliers: 5
- Sales transactions: 2,004
- Inventory: 70
- Purchase orders: 300
- Total records processed across Bronze tables: 2,398

The quality stage detected **76 findings**:

| Severity | Count |
|---|---:|
| ERROR | 16 |
| WARNING | 3 |
| INFO | 57 |
| **Total** | **76** |

The actual issue summary was:

| Dataset | Issue | Severity | Count |
|---|---|---:|---:|
| Products | Invalid unit price | ERROR | 2 |
| Sales transactions | Duplicate sales business record | ERROR | 14 |
| Stores | Missing manager name | WARNING | 1 |
| Suppliers | Inconsistent phone format | WARNING | 2 |
| Purchase orders | Pending purchase order | INFO | 57 |

---

# 1. Duplicate transaction records

### PRD requirement

The PRD describes duplicate transaction records using the same product, store, date, and quantity.

### Observation

The Bronze sales table contained **2,004 records**.

The quality stage detected **14 duplicate business records**.

The duplicate check did **not** use `transaction_id` as the only duplicate key. Instead, it compared:

```text
store_id
product_id
quantity_sold
unit_price
total_amount
sale_date
```

This was important because the generator creates duplicate business records with a **different transaction_id**.

The generator explicitly adds duplicate-looking sales rows with a different transaction ID. During the actual quality run, 14 duplicate business records were detected.

### Decision

Keep the first record in each duplicate business group and remove subsequent duplicates from Silver.

The selected action is:

```text
KEEP_FIRST_RECORD_AND_QUARANTINE_DUPLICATES
```

### Justification

A different `transaction_id` does not necessarily make two otherwise identical business events different.

For this dataset, the combination of:

```text
store + product + quantity + price + amount + date
```

is more useful for identifying accidental duplicate sales than relying on the generated identifier alone.

Bronze remains unchanged so the original duplicate records are still available for audit and investigation.

### Silver result

```text
Bronze sales transactions : 2,004
Silver sales transactions : 1,990
Duplicates removed        : 14
```

---

# 2. Missing values in manager_name and contact_person

## manager_name

### Observation

The generator intentionally sets one store's manager to `None`.

The quality stage detected:

```text
store_id = 4
manager_name = NULL
```

This produced one WARNING.

### Decision

Convert the missing manager value to:

```text
UNKNOWN
```

in Silver.

### Justification

The record still represents a valid store, and the missing manager value does not invalidate the store itself.

Replacing the missing value with `UNKNOWN` keeps the store record available for analysis while making the missingness explicit.

The original NULL is preserved in Bronze and the quality finding records that the value was missing.

### Silver result

Store 4 appears with:

```text
manager_name = UNKNOWN
```

## contact_person

### Observation

The generated supplier data did **not** produce a missing `contact_person` value in the current dataset.

Therefore, no missing-contact-person finding was generated in the current quality run.

### Decision

The Silver transformation still standardizes a null or blank `contact_person` to:

```text
UNKNOWN
```

should such a value occur.

### Justification

This keeps the supplier record usable without inventing a person's name.

It also gives the pipeline deterministic behavior if a future input file contains a missing supplier contact.

---

# 3. Null delivery_date for purchase orders

### PRD requirement

The PRD mentions null delivery dates for cancelled orders.

### Generator observation

The actual generator does **not** label a blank `delivery_date` as cancelled.

It explicitly generates some purchase orders without a delivery date and describes them as **pending**.

The current dataset contains:

```text
Delivered : 243
Pending   : 57
```

The quality stage therefore generated:

```text
PENDING_PURCHASE_ORDER = 57
```

with INFO severity.

### Decision

A blank `delivery_date` is classified as:

```text
PENDING
```

not `CANCELLED`.

### Justification

The generator is the source of truth for the actual input behavior.

There is no evidence in the generated data that a blank delivery date means cancellation. Therefore, assigning `CANCELLED` would introduce an unsupported business assumption.

For delivered orders, Silver calculates:

```text
lead_time_days =
delivery_date - order_date
```

For pending orders:

```text
lead_time_days = NULL
```

### Result

```text
DELIVERED = 243
PENDING   = 57
```

This allows supplier lead-time analysis to use only orders with an actual delivery date.

---

# 4. Incorrect unit prices

### Observation

The generated product master intentionally contains two invalid values:

```text
product_id 1005 -> unit_price = -100.00
product_id 1007 -> unit_price = 0.00
```

The quality stage detected:

```text
INVALID_UNIT_PRICE = 2
```

both with ERROR severity.

### Decision

The product records are retained in Silver, but their invalid `unit_price` is converted to:

```text
NULL
```

and the row is marked:

```text
data_quality_status = PRICE_QUARANTINED
```

### Justification

The product record itself remains structurally useful because it still contains:

```text
product_id
product_name
category
supplier_id
```

Deleting the entire product record would unnecessarily break product-level relationships and downstream analysis.

The invalid price is the specific faulty attribute, so only that attribute is quarantined.

This preserves the product dimension while preventing the invalid price from silently becoming a valid analytical value.

### Important observation about sales

The generated `sales_transactions.csv` has its own:

```text
unit_price
total_amount
```

fields.

The generator calculates transaction values from the original in-memory product prices used during sales generation. The later corruption of the product CSV price does not automatically rewrite the sales CSV.

Therefore, Silver sales analysis should use the **transaction-level values from `sales_transactions`**, not overwrite them with the product-master price.

---

# 5. Invalid dates / future dates

### PRD requirement

The PRD mentions future dates for historical data.

### Observation from the actual generator

The generator creates sales dates and purchase-order `order_date` values within 2024.

However, a purchase order placed near the end of December 2024 can have a delivery date in early January 2025 because the generator adds a random delivery time of 3–14 days.

The current Bronze data already contains examples such as:

```text
order_date    = 2024-12-30
delivery_date = 2025-01-04
```

and:

```text
order_date    = 2024-12-28
delivery_date = 2025-01-10
```

### Decision

Do **not** reject a delivery date merely because its year is 2025.

Instead:

- Validate that the date can be parsed.
- Validate that `delivery_date >= order_date`.
- Allow a delivery date after the 2024 order period when it is the natural result of the generator's 3–14 day delivery window.

### Justification

Rejecting all 2025 dates would incorrectly classify valid generated deliveries as bad data.

The important business rule is temporal consistency between the order and delivery dates, not simply matching the calendar year.

### Result in current dataset

No invalid date finding was generated by the current quality run.

---

# 6. Store IDs that reference non-existent stores

### Observation

The generator creates stores with IDs:

```text
1
2
3
4
5
```

and sales/inventory store IDs are generated from the same range.

Therefore, the current dataset did **not** produce an invalid store reference.

### Decision

Still perform referential-integrity checks during the quality stage.

For any future record whose `store_id` does not match an existing store:

```text
QUARANTINE_RECORD
```

### Justification

A sales or inventory record cannot be reliably attributed to a store that does not exist in the store master.

Keeping such records in analytical Silver data would create orphaned records and could distort store-level reporting.

The current run found no such issue, so no records were removed for this reason.

---

# 7. Inconsistent phone-number formats

### Observation

The generator intentionally creates supplier phone numbers in different source formats, including:

```text
9000000001
+91-9000000002
919000000004
```

The quality stage detected:

```text
INCONSISTENT_PHONE_FORMAT = 2
```

with WARNING severity.

### Decision

Normalize phone numbers in Silver to a standard 10-digit representation:

```text
9000000002
9000000004
```

### Justification

The underlying phone number is not necessarily incorrect; the problem is source-format inconsistency.

This is therefore treated as a WARNING rather than an ERROR.

Normalization removes formatting variation while preserving the supplier contact information.

The original source representation remains available in Bronze.

---

# 8. Out-of-stock inventory

### Observation

The generator intentionally creates inventory records with:

```text
quantity_on_hand = 0
```

for some store/product combinations.

The generator treats these as intentional stockout situations.

### Decision

Do **not** treat zero inventory as a bad record.

Keep it in Silver as:

```text
quantity_on_hand = 0
```

and classify it later as:

```text
OUT_OF_STOCK
```

in the inventory-analysis stage.

### Justification

Zero stock is a valid business condition, not a data-quality defect.

The purpose of the capstone is specifically to identify stockouts and low-stock situations.

Removing those rows would destroy one of the most important business signals in the project.

---

# Overall data-quality decisions

| Issue | Current observation | Decision | Severity | Justification |
|---|---|---|---|---|
| Duplicate sales | 14 detected | Keep first; remove duplicate business records from Silver | ERROR | Prevent double counting while preserving Bronze/audit evidence |
| Missing manager | 1 record | Convert to `UNKNOWN` | WARNING | Store remains analytically usable |
| Missing contact person | Not present in current generator output | Convert null/blank to `UNKNOWN` if encountered | WARNING/handling rule | Avoid inventing a name |
| Null delivery date | 57 records | Classify as `PENDING` | INFO | Generator defines these as pending, not cancelled |
| Invalid product price | 2 records | Set price to `NULL`, mark `PRICE_QUARANTINED` | ERROR | Preserve product record while preventing invalid price use |
| Future delivery dates | 2025 delivery dates can occur | Allow when consistent with order date | Not a defect by itself | Generator permits 3–14 day delivery after a 2024 order |
| Invalid store reference | None observed | Quarantine if encountered | ERROR | Prevent orphaned analytical records |
| Phone formatting | 2 inconsistent formats | Normalize to 10 digits | WARNING | Formatting problem, not necessarily invalid business data |
| Zero inventory | Some intentional stockouts | Keep as valid business condition | Not a defect | Stockout is a required business-analysis signal |

---

# Silver-layer result

After applying the documented decisions:

```text
Silver stores                : 5
Silver products              : 14
Silver suppliers             : 5
Silver sales_transactions    : 1,990
Silver inventory             : 70
Silver purchase_orders       : 300
```

Product quality status:

```text
VALID              : 12
PRICE_QUARANTINED  : 2
```

Purchase-order status:

```text
DELIVERED : 243
PENDING   : 57
```

Sales duplicates removed:

```text
14
```

Bronze remains unchanged.

---

# Auditability

All quality findings are written to:

```text
megamart_dev.audit.data_quality_issues
```

and stage execution is written to:

```text
megamart_dev.audit.pipeline_runs
```

Each quality finding retains the run ID, dataset, record identifier, issue type, severity, description, action taken, and raw-record information.

This allows the project to explain not only **what was cleaned**, but also **why it was cleaned and what happened to the original record**.
