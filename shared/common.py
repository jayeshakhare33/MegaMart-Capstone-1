"""
MegaMart Databricks Capstone
Shared Python Utilities

Purpose
-------
Reusable functions used by multiple pipeline stages.

Rules
-----
- No notebook execution logic belongs here.
- No hard-coded project paths belong here.
- Functions should be reusable across ingestion, quality, modeling,
  sales, inventory, and KPI stages.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence


# ============================================================================
# GENERAL PYTHON UTILITIES
# ============================================================================

def get_utc_timestamp() -> datetime:
    """
    Return the current UTC timestamp.

    Returns
    -------
    datetime
        Timezone-aware UTC datetime.
    """
    return datetime.now(timezone.utc)


def create_run_id(stage_name: str) -> str:
    """
    Create a simple unique pipeline run identifier.

    Parameters
    ----------
    stage_name : str
        Name of the pipeline stage.

    Returns
    -------
    str
        Example:
        ingestion_20260922T121530_123456
    """
    timestamp = get_utc_timestamp().strftime("%Y%m%dT%H%M%S_%f")
    normalized_stage = normalize_string(stage_name).replace(" ", "_")

    return f"{normalized_stage}_{timestamp}"


def normalize_string(value: Any) -> str:
    """
    Normalize a string value by trimming whitespace and converting
    None-like values to an empty string.

    Parameters
    ----------
    value : Any
        Input value.

    Returns
    -------
    str
        Normalized string.
    """
    if value is None:
        return ""

    return str(value).strip()


# ============================================================================
# DATA QUALITY UTILITIES
# ============================================================================

def validate_required_columns(
    df: Any,
    required_columns: Sequence[str],
) -> None:
    """
    Validate that a Spark DataFrame contains all required columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Spark DataFrame to validate.

    required_columns : Sequence[str]
        Columns required by the current pipeline stage.

    Raises
    ------
    ValueError
        If one or more required columns are missing.
    """
    actual_columns = set(df.columns)
    missing_columns = [
        column
        for column in required_columns
        if column not in actual_columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(sorted(missing_columns))
        )


def normalize_phone(phone: Any) -> str | None:
    """
    Normalize a phone number into a digits-only representation.

    Examples
    --------
    '+91-9000000002' -> '919000000002'
    '91 9000000004'  -> '919000000004'
    '9000000001'     -> '9000000001'

    Parameters
    ----------
    phone : Any
        Raw phone number.

    Returns
    -------
    str | None
        Digits-only phone number, or None for an empty value.
    """
    if phone is None:
        return None

    value = str(phone).strip()

    if not value:
        return None

    digits = re.sub(r"\D", "", value)

    return digits if digits else None


def is_valid_indian_phone(phone: Any) -> bool:
    """
    Perform a basic phone-number format validation.

    This function checks the normalized digit length only.
    It does not verify whether the number actually exists.

    Accepted forms:
        10 digits
        12 digits beginning with 91

    Parameters
    ----------
    phone : Any
        Phone number.

    Returns
    -------
    bool
        True if the normalized value has an accepted format.
    """
    normalized = normalize_phone(phone)

    if normalized is None:
        return False

    if len(normalized) == 10:
        return True

    if len(normalized) == 12 and normalized.startswith("91"):
        return True

    return False


def calculate_row_hash(
    values: Iterable[Any],
) -> str:
    """
    Create a deterministic SHA-256 hash from a collection of values.

    Useful for:
        - duplicate detection
        - record fingerprinting
        - data lineage checks

    Parameters
    ----------
    values : Iterable[Any]
        Values that make up the record fingerprint.

    Returns
    -------
    str
        SHA-256 hexadecimal hash.
    """
    normalized_values = [
        normalize_string(value)
        for value in values
    ]

    record_string = "||".join(normalized_values)

    return hashlib.sha256(
        record_string.encode("utf-8")
    ).hexdigest()


def validate_positive_number(
    value: Any,
    field_name: str = "value",
) -> bool:
    """
    Check whether a value is numeric and greater than zero.

    Parameters
    ----------
    value : Any
        Value to validate.

    field_name : str
        Field name used in documentation/error messages.

    Returns
    -------
    bool
        True when value is a positive number.

    Notes
    -----
    field_name is intentionally accepted so callers can use this
    utility while keeping validation logic readable.
    """
    _ = field_name

    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def validate_non_negative_number(value: Any) -> bool:
    """
    Check whether a value is numeric and greater than or equal to zero.
    """
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


# ============================================================================
# BUSINESS-METRIC UTILITIES
# ============================================================================

def calculate_revenue(
    quantity: Any,
    unit_price: Any,
) -> float:
    """
    Calculate revenue as quantity multiplied by unit price.

    Parameters
    ----------
    quantity : Any
        Quantity sold.

    unit_price : Any
        Unit selling price.

    Returns
    -------
    float
        Calculated revenue.

    Raises
    ------
    ValueError
        If either input cannot be converted to a number.
    """
    try:
        quantity_value = float(quantity)
        price_value = float(unit_price)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Quantity and unit_price must be numeric."
        ) from exc

    return quantity_value * price_value


def classify_stock_level(
    quantity_on_hand: Any,
    low_stock_threshold: int = 50,
) -> str:
    """
    Classify inventory stock level.

    Business rules
    --------------
    0              -> OUT_OF_STOCK
    < threshold    -> LOW_STOCK
    >= threshold   -> HEALTHY

    Parameters
    ----------
    quantity_on_hand : Any
        Current inventory quantity.

    low_stock_threshold : int
        Threshold below which inventory is considered low.

    Returns
    -------
    str
        OUT_OF_STOCK, LOW_STOCK, or HEALTHY.
    """
    try:
        quantity = float(quantity_on_hand)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "quantity_on_hand must be numeric."
        ) from exc

    if quantity == 0:
        return "OUT_OF_STOCK"

    if quantity < low_stock_threshold:
        return "LOW_STOCK"

    return "HEALTHY"


def calculate_reorder_point(
    average_daily_demand: float,
    average_lead_time_days: float,
    safety_stock: float,
) -> float:
    """
    Calculate inventory reorder point.

    Formula
    -------
    reorder point =
        average daily demand * average lead time
        + safety stock

    Parameters
    ----------
    average_daily_demand : float
        Average units sold per day.

    average_lead_time_days : float
        Average supplier delivery time.

    safety_stock : float
        Additional stock kept as a buffer.

    Returns
    -------
    float
        Reorder point.
    """
    if average_daily_demand < 0:
        raise ValueError(
            "average_daily_demand cannot be negative."
        )

    if average_lead_time_days < 0:
        raise ValueError(
            "average_lead_time_days cannot be negative."
        )

    if safety_stock < 0:
        raise ValueError(
            "safety_stock cannot be negative."
        )

    return (
        average_daily_demand * average_lead_time_days
        + safety_stock
    )


# ============================================================================
# AUDIT / LOGGING UTILITIES
# ============================================================================

def build_audit_record(
    run_id: str,
    pipeline_name: str,
    stage_name: str,
    status: str,
    records_processed: int = 0,
    records_failed: int = 0,
    error_message: str | None = None,
) -> dict[str, Any]:
    """
    Build a standard pipeline audit record.

    This returns a Python dictionary. The calling notebook is
    responsible for writing it into the audit table.

    Parameters
    ----------
    run_id : str
        Unique pipeline execution ID.

    pipeline_name : str
        Overall pipeline name.

    stage_name : str
        Current pipeline stage.

    status : str
        RUNNING, SUCCESS, FAILED, etc.

    records_processed : int
        Number of records processed.

    records_failed : int
        Number of failed records.

    error_message : str | None
        Error description, if applicable.

    Returns
    -------
    dict[str, Any]
        Standardized audit record.
    """
    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "status": status,
        "records_processed": records_processed,
        "records_failed": records_failed,
        "error_message": error_message,
        "event_timestamp": get_utc_timestamp(),
    }


def build_quality_issue_record(
    run_id: str,
    table_name: str,
    record_identifier: str,
    issue_type: str,
    severity: str,
    description: str,
    action_taken: str,
) -> dict[str, Any]:
    """
    Build a standardized data-quality issue record.

    The calling pipeline stage is responsible for writing this
    record into the audit.data_quality_issues table.

    Parameters
    ----------
    run_id : str
        Pipeline execution ID.

    table_name : str
        Source or target table associated with the issue.

    record_identifier : str
        Identifier for the affected record.

    issue_type : str
        Category of issue.

    severity : str
        Issue severity.

    description : str
        Explanation of the issue.

    action_taken : str
        How the pipeline handled the issue.

    Returns
    -------
    dict[str, Any]
        Standardized quality issue record.
    """
    return {
        "run_id": run_id,
        "table_name": table_name,
        "record_identifier": record_identifier,
        "issue_type": issue_type,
        "severity": severity,
        "description": description,
        "action_taken": action_taken,
        "detected_at": get_utc_timestamp(),
    }