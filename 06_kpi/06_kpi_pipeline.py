# Databricks source file
# MegaMart Capstone - Stage 06: KPI Pipeline
#
# Purpose:
#   Build Gold KPI tables from Silver data and publish CSV/HTML/PDF KPI
#   artifacts to the Unity Catalog Volume created during setup.
#
# This file intentionally does NOT create infrastructure. It only creates
# run-specific output folders/tables.

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T


# -----------------------------------------------------------------------------
# 1. Resolve project root so config/shared imports work from a Git-backed
#    Databricks folder regardless of the notebook/script working directory.
# -----------------------------------------------------------------------------

def _find_project_root() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    cwd = Path.cwd().resolve()
    candidates = [here, *here.parents, cwd, *cwd.parents]
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / "config" / "project_config.py").exists() and (
            candidate / "shared" / "common.py"
        ).exists():
            return candidate
    raise RuntimeError(
        "Could not locate project root containing config/project_config.py "
        "and shared/common.py. Run this file from the MegaMart-Capstone-1 Git folder."
    )


PROJECT_ROOT = _find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.project_config import (  # noqa: E402
    AUDIT_TABLES,
    CATALOG_NAME,
    GOLD_TABLES,
    PROJECT_END_DATE,
    PROJECT_START_DATE,
    SILVER_TABLES,
)
from shared.common import create_run_id, get_utc_timestamp  # noqa: E402


spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

STAGE_NAME = "06_kpi"
PIPELINE_NAME = "megamart_pipeline"
RUN_ID = create_run_id(STAGE_NAME)
RUN_TIMESTAMP = get_utc_timestamp()

GOLD_SCHEMA = f"{CATALOG_NAME}.gold"
AUDIT_SCHEMA = f"{CATALOG_NAME}.audit"

# report_artifacts volume is created by 00_setup/admin work.
REPORT_BASE = f"/Volumes/{CATALOG_NAME}/gold/report_artifacts/kpi"
RUN_OUTPUT_PATH = f"{REPORT_BASE}/{RUN_ID}"


# -----------------------------------------------------------------------------
# 2. Helpers
# -----------------------------------------------------------------------------

def table_name(table_key: str) -> str:
    return SILVER_TABLES[table_key]


def gold_name(table_key: str, fallback: str) -> str:
    return GOLD_TABLES.get(table_key, fallback)


def ensure_columns(df: DataFrame, required: List[str], table: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{table} is missing required columns: {missing}")


def write_gold(df: DataFrame, full_table_name: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )


def save_csv(df: DataFrame, file_name: str) -> None:
    # Spark CSV output is a directory with part files. Pandas gives the
    # requested single CSV artifact and the data sizes here are intentionally
    # small (5/12/70/1 rows).
    pdf = df.toPandas()
    output = Path(f"{RUN_OUTPUT_PATH}/{file_name}")
    output.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(output, index=False)


def html_table(df: DataFrame, max_rows: int = 25) -> str:
    pdf = df.limit(max_rows).toPandas()
    if pdf.empty:
        return "<p>No rows.</p>"
    return pdf.to_html(index=False, border=0, classes="data-table")


def build_pdf(pdf_path: str, sections: List[tuple[str, DataFrame]]) -> None:
    # Matplotlib is used only for the PDF artifact. All source data comes from
    # Spark and the PDF is written directly to the Unity Catalog Volume path.
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    with PdfPages(pdf_path) as pdf:
        for title, df in sections:
            local = df.toPandas()
            fig = plt.figure(figsize=(11, 8.5))
            fig.text(0.05, 0.95, title, fontsize=16, weight="bold", va="top")

            if local.empty:
                fig.text(0.05, 0.85, "No rows available.", fontsize=11)
            else:
                # Keep PDF readable and compact for the small capstone outputs.
                display_df = local.head(30).copy()
                ax = fig.add_axes([0.03, 0.08, 0.94, 0.80])
                ax.axis("off")
                table = ax.table(
                    cellText=display_df.astype(str).values,
                    colLabels=display_df.columns.tolist(),
                    cellLoc="left",
                    loc="upper left",
                )
                table.auto_set_font_size(False)
                table.set_fontsize(7)
                table.scale(1, 1.25)

            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)


# -----------------------------------------------------------------------------
# 3. Load Silver source tables
# -----------------------------------------------------------------------------

def load_sources() -> Dict[str, DataFrame]:
    sources = {
        "sales": spark.table(table_name("sales_transactions")),
        "stores": spark.table(table_name("stores")),
        "products": spark.table(table_name("products")),
        "inventory": spark.table(table_name("inventory")),
    }

    ensure_columns(
        sources["sales"],
        [
            "transaction_id",
            "store_id",
            "product_id",
            "quantity_sold",
            "total_amount",
            "sale_date",
        ],
        "silver.sales_transactions",
    )
    ensure_columns(sources["stores"], ["store_id", "store_name"], "silver.stores")
    ensure_columns(
        sources["products"], ["product_id", "product_name", "category"], "silver.products"
    )
    ensure_columns(
        sources["inventory"], ["store_id", "product_id", "quantity_on_hand"], "silver.inventory"
    )

    return sources


# -----------------------------------------------------------------------------
# 4. Gold KPI tables
# -----------------------------------------------------------------------------

def build_store_kpis(sales: DataFrame, stores: DataFrame) -> DataFrame:
    agg = (
        sales.groupBy("store_id")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
            F.countDistinct("product_id").alias("unique_products_sold"),
        )
    )

    total_revenue = agg.agg(F.sum("total_revenue").alias("grand_total")).first()["grand_total"] or 0.0

    return (
        agg.join(stores.select("store_id", "store_name"), "store_id", "left")
        .withColumn(
            "store_revenue_share_pct",
            F.when(F.lit(total_revenue) > 0,
                   F.round(F.col("total_revenue") / F.lit(float(total_revenue)) * 100, 2))
             .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "inventory_turnover",
            F.lit(None).cast("decimal(18,4)"),
        )
        .withColumn(
            "inventory_turnover_status",
            F.lit("NOT_CALCULABLE_SINGLE_INVENTORY_SNAPSHOT"),
        )
        .withColumn(
            "market_share_pct",
            F.lit(None).cast("decimal(18,4)"),
        )
        .withColumn(
            "market_share_status",
            F.lit("NOT_CALCULABLE_NO_EXTERNAL_MARKET_TOTAL"),
        )
        .withColumn("_kpi_run_id", F.lit(RUN_ID))
        .withColumn("_kpi_processed_at", F.current_timestamp())
        .select(
            "store_id",
            "store_name",
            "total_revenue",
            "transaction_count",
            "avg_transaction_value",
            "unique_products_sold",
            "store_revenue_share_pct",
            "inventory_turnover",
            "inventory_turnover_status",
            "market_share_pct",
            "market_share_status",
            "_kpi_run_id",
            "_kpi_processed_at",
        )
        .orderBy("store_id")
    )


def build_category_kpis(sales: DataFrame, products: DataFrame) -> DataFrame:
    joined = sales.join(
        products.select("product_id", "category"),
        "product_id",
        "left",
    )

    return (
        joined.groupBy("category")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.sum("quantity_sold").cast("long").alias("units_sold"),
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.countDistinct("product_id").alias("unique_products_sold"),
            F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        )
        .withColumn(
            "category_rank",
            F.row_number().over(
                Window.orderBy(F.col("total_revenue").desc(), F.col("category"))
            ),
        )
        .withColumn("top_3_category_flag", F.when(F.col("category_rank") <= 3, F.lit(True)).otherwise(F.lit(False)))
        .withColumn("_kpi_run_id", F.lit(RUN_ID))
        .withColumn("_kpi_processed_at", F.current_timestamp())
        .orderBy("category_rank")
    )


def build_monthly_kpis(sales: DataFrame, products: DataFrame) -> DataFrame:
    start_expr = F.to_date(F.lit(PROJECT_START_DATE))
    end_expr = F.to_date(F.lit(PROJECT_END_DATE))

    month_calendar = (
        spark.sql(
            f"SELECT explode(sequence(to_date('{PROJECT_START_DATE}'), "
            f"to_date('{PROJECT_END_DATE}'), interval 1 month)) AS month_start"
        )
        .select(F.date_trunc("month", F.col("month_start")).cast("date").alias("month_start"))
        .dropDuplicates()
    )

    month_agg = (
        sales.withColumn("month_start", F.trunc("sale_date", "month"))
        .join(products.select("product_id", "category"), "product_id", "left")
        .groupBy("month_start")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
            F.countDistinct("product_id").alias("unique_products_sold"),
            F.sum("quantity_sold").cast("long").alias("units_sold"),
            F.countDistinct("category").alias("categories_sold"),
        )
    )

    window = Window.orderBy("month_start")

    return (
        month_calendar.join(month_agg, "month_start", "left")
        .fillna(
            0,
            subset=[
                "total_revenue",
                "transaction_count",
                "avg_transaction_value",
                "unique_products_sold",
                "units_sold",
                "categories_sold",
            ],
        )
        .withColumn("mom_revenue_change_pct",
                    F.when(F.lag("total_revenue").over(window) > 0,
                           F.round((F.col("total_revenue") - F.lag("total_revenue").over(window)) /
                                   F.lag("total_revenue").over(window) * 100, 2))
                     .otherwise(F.lit(None).cast("decimal(18,2)")))
        .withColumn("mom_transaction_change_pct",
                    F.when(F.lag("transaction_count").over(window) > 0,
                           F.round((F.col("transaction_count") - F.lag("transaction_count").over(window)) /
                                   F.lag("transaction_count").over(window) * 100, 2))
                     .otherwise(F.lit(None).cast("decimal(18,2)")))
        .withColumn("mom_units_change_pct",
                    F.when(F.lag("units_sold").over(window) > 0,
                           F.round((F.col("units_sold") - F.lag("units_sold").over(window)) /
                                   F.lag("units_sold").over(window) * 100, 2))
                     .otherwise(F.lit(None).cast("decimal(18,2)")))
        .withColumn("_kpi_run_id", F.lit(RUN_ID))
        .withColumn("_kpi_processed_at", F.current_timestamp())
        .orderBy("month_start")
    )


def build_enterprise_kpis(
    sales: DataFrame,
    inventory: DataFrame,
    products: DataFrame,
    stores: DataFrame,
    store_kpis: DataFrame,
    category_kpis: DataFrame,
    monthly_kpis: DataFrame,
) -> DataFrame:
    sales_metrics = sales.agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction_value"),
        F.countDistinct("product_id").alias("unique_products_sold"),
        F.countDistinct("store_id").alias("active_stores"),
    ).first()

    inventory_metrics = inventory.agg(
        F.sum("quantity_on_hand").cast("long").alias("current_inventory_units"),
        F.sum(F.when(F.col("quantity_on_hand") == 0, 1).otherwise(0)).alias("out_of_stock_skus"),
        F.sum(F.when(F.col("quantity_on_hand") < 50, 1).otherwise(0)).alias("low_stock_skus"),
    ).first()

    product_count = products.select("product_id").distinct().count()
    store_count = stores.select("store_id").distinct().count()

    # The generator only provides one inventory snapshot, so a standard
    # inventory turnover calculation (e.g. COGS / average inventory) is not
    # supported. We expose the exact limitation instead of inventing cost or
    # average-inventory data.
    annual_revenue = float(sales_metrics["total_revenue"] or 0.0)
    current_inventory_units = int(inventory_metrics["current_inventory_units"] or 0)
    annual_units = sales.agg(F.sum("quantity_sold").alias("units")).first()["units"] or 0

    avg_monthly_revenue = monthly_kpis.agg(F.avg("total_revenue").alias("avg_monthly_revenue")).first()["avg_monthly_revenue"] or 0.0

    row_schema = T.StructType([
        T.StructField("kpi_run_id", T.StringType(), False),
        T.StructField("project_start_date", T.StringType(), False),
        T.StructField("project_end_date", T.StringType(), False),
        T.StructField("total_revenue", T.DoubleType(), True),
        T.StructField("transaction_count", T.LongType(), True),
        T.StructField("avg_transaction_value", T.DoubleType(), True),
        T.StructField("unique_products_sold", T.LongType(), True),
        T.StructField("total_products_in_catalog", T.LongType(), True),
        T.StructField("active_stores", T.LongType(), True),
        T.StructField("total_stores_in_master", T.LongType(), True),
        T.StructField("current_inventory_units", T.LongType(), True),
        T.StructField("units_sold", T.LongType(), True),
        T.StructField("inventory_turnover", T.DoubleType(), True),
        T.StructField("inventory_turnover_status", T.StringType(), False),
        T.StructField("market_share_pct", T.DoubleType(), True),
        T.StructField("market_share_status", T.StringType(), False),
        T.StructField("internal_store_revenue_share_available", T.BooleanType(), False),
        T.StructField("out_of_stock_skus", T.LongType(), True),
        T.StructField("low_stock_skus", T.LongType(), True),
        T.StructField("avg_monthly_revenue", T.DoubleType(), True),
        T.StructField("generated_at_utc", T.TimestampType(), False),
    ])

    data = [
        (
            RUN_ID,
            PROJECT_START_DATE,
            PROJECT_END_DATE,
            annual_revenue,
            int(sales_metrics["transaction_count"] or 0),
            float(sales_metrics["avg_transaction_value"] or 0.0),
            int(sales_metrics["unique_products_sold"] or 0),
            int(product_count),
            int(sales_metrics["active_stores"] or 0),
            int(store_count),
            current_inventory_units,
            int(annual_units),
            None,
            "NOT_CALCULABLE_SINGLE_INVENTORY_SNAPSHOT",
            None,
            "NOT_CALCULABLE_NO_EXTERNAL_MARKET_TOTAL",
            True,
            int(inventory_metrics["out_of_stock_skus"] or 0),
            int(inventory_metrics["low_stock_skus"] or 0),
            float(avg_monthly_revenue),
            RUN_TIMESTAMP,
        )
    ]

    return spark.createDataFrame(data, schema=row_schema)


# -----------------------------------------------------------------------------
# 5. Audit helpers
# -----------------------------------------------------------------------------

def write_pipeline_audit(status: str, records_processed: int, message: str = "") -> None:
    """Append to the existing audit table without assuming its exact schema."""
    audit_table = AUDIT_TABLES["pipeline_runs"]
    existing_schema = spark.table(audit_table).schema
    now = get_utc_timestamp()

    values = {}
    for field in existing_schema.fields:
        name = field.name.lower()
        if name in {"run_id", "pipeline_run_id"}:
            value = RUN_ID
        elif name in {"pipeline_name", "job_name"}:
            value = PIPELINE_NAME
        elif name in {"stage_name", "stage", "step_name"}:
            value = STAGE_NAME
        elif name in {"status", "run_status"}:
            value = status
        elif name in {"started_at", "start_time", "run_started_at"}:
            value = RUN_TIMESTAMP
        elif name in {"ended_at", "end_time", "completed_at", "run_ended_at"}:
            value = now
        elif name in {"records_processed", "record_count", "row_count", "rows_processed"}:
            value = int(records_processed)
        elif name in {"message", "error_message", "details"}:
            value = message
        else:
            # Preserve the table's declared type for fields that belong to
            # earlier stages but are not needed by this KPI stage.
            if isinstance(field.dataType, T.StringType):
                value = ""
            elif isinstance(field.dataType, (T.IntegerType, T.LongType, T.ShortType, T.ByteType, T.DoubleType, T.FloatType, T.DecimalType)):
                value = 0
            elif isinstance(field.dataType, T.BooleanType):
                value = False
            else:
                value = None
        values[field.name] = value

    audit_df = spark.createDataFrame([values], schema=existing_schema)
    audit_df.write.mode("append").format("delta").saveAsTable(audit_table)


# -----------------------------------------------------------------------------
# 6. Main execution
# -----------------------------------------------------------------------------

def main() -> None:
    print("=" * 80)
    print(f"MEGAMART {STAGE_NAME.upper()} START")
    print(f"Run ID: {RUN_ID}")
    print(f"Project period: {PROJECT_START_DATE} -> {PROJECT_END_DATE}")
    print(f"Output path: {RUN_OUTPUT_PATH}")
    print("=" * 80)

    sources = load_sources()

    sales = sources["sales"].filter(
        (F.col("sale_date") >= F.to_date(F.lit(PROJECT_START_DATE))) &
        (F.col("sale_date") <= F.to_date(F.lit(PROJECT_END_DATE)))
    )

    print(f"Silver sales rows in project period: {sales.count():,}")
    print(f"Silver inventory rows: {sources['inventory'].count():,}")

    store_table = gold_name("store_kpi", f"{GOLD_SCHEMA}.store_kpi_summary")
    category_table = gold_name("category_kpi", f"{GOLD_SCHEMA}.category_kpi_summary")
    monthly_table = gold_name("monthly_kpi", f"{GOLD_SCHEMA}.monthly_kpi_summary")
    enterprise_table = gold_name("enterprise_kpi", f"{GOLD_SCHEMA}.enterprise_kpi_summary")

    store_kpis = build_store_kpis(sales, sources["stores"])
    category_kpis = build_category_kpis(sales, sources["products"])
    monthly_kpis = build_monthly_kpis(sales, sources["products"])
    enterprise_kpis = build_enterprise_kpis(
        sales,
        sources["inventory"],
        sources["products"],
        sources["stores"],
        store_kpis,
        category_kpis,
        monthly_kpis,
    )

    write_gold(store_kpis, store_table)
    write_gold(category_kpis, category_table)
    write_gold(monthly_kpis, monthly_table)
    write_gold(enterprise_kpis, enterprise_table)

    # Validate Gold row counts before publishing artifacts.
    counts = {
        "store_kpi_summary": store_kpis.count(),
        "category_kpi_summary": category_kpis.count(),
        "monthly_kpi_summary": monthly_kpis.count(),
        "enterprise_kpi_summary": enterprise_kpis.count(),
    }

    if counts["store_kpi_summary"] != sources["stores"].select("store_id").distinct().count():
        raise RuntimeError("Store KPI row count does not match the store master count.")
    if counts["monthly_kpi_summary"] != 12:
        raise RuntimeError(f"Monthly KPI table expected 12 project months, found {counts['monthly_kpi_summary']}.")
    if counts["enterprise_kpi_summary"] != 1:
        raise RuntimeError("Enterprise KPI table must contain exactly one row.")

    Path(RUN_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    save_csv(store_kpis, "store_kpi_summary.csv")
    save_csv(category_kpis, "category_kpi_summary.csv")
    save_csv(monthly_kpis, "monthly_kpi_summary.csv")
    save_csv(enterprise_kpis, "enterprise_kpi_summary.csv")

    enterprise_row = enterprise_kpis.first().asDict()
    top3_categories = [
        r["category"]
        for r in category_kpis.filter(F.col("category_rank") <= 3)
        .orderBy("category_rank")
        .select("category")
        .collect()
    ]

    latest_month = (
        monthly_kpis.orderBy(F.col("month_start").desc()).limit(1).toPandas().to_dict("records")
    )
    latest_month_html = ""
    if latest_month:
        latest_month_html = f"<pre>{latest_month[0]}</pre>"

    html_path = Path(f"{RUN_OUTPUT_PATH}/kpi_summary.html")
    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"utf-8\">
<title>MegaMart KPI Summary</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 32px; }}
h1, h2 {{ margin-bottom: 8px; }}
.meta {{ color: #555; margin-bottom: 20px; }}
.card {{ display: inline-block; vertical-align: top; min-width: 180px; margin: 0 12px 12px 0; padding: 14px; border: 1px solid #ddd; border-radius: 6px; }}
.card .value {{ font-size: 24px; font-weight: 700; }}
.card .label {{ color: #666; font-size: 13px; margin-top: 4px; }}
.data-table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
.data-table th, .data-table td {{ border: 1px solid #ddd; padding: 7px; text-align: left; font-size: 12px; }}
.data-table th {{ background: #f3f3f3; }}
.note {{ padding: 12px; border-left: 4px solid #999; background: #fafafa; margin: 16px 0; }}
</style>
</head>
<body>
<h1>MegaMart KPI Summary</h1>
<div class=\"meta\">Run ID: {RUN_ID} | Project period: {PROJECT_START_DATE} to {PROJECT_END_DATE}</div>

<div class=\"card\"><div class=\"value\">₹{enterprise_row['total_revenue']:,.2f}</div><div class=\"label\">Total Revenue</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['transaction_count']:,}</div><div class=\"label\">Transactions</div></div>
<div class=\"card\"><div class=\"value\">₹{enterprise_row['avg_transaction_value']:,.2f}</div><div class=\"label\">Average Transaction Value</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['unique_products_sold']}</div><div class=\"label\">Unique Products Sold</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['current_inventory_units']:,}</div><div class=\"label\">Current Inventory Units</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['out_of_stock_skus']}</div><div class=\"label\">Out-of-Stock SKUs</div></div>
<div class=\"card\"><div class=\"value\">{enterprise_row['low_stock_skus']}</div><div class=\"label\">Low-Stock SKUs</div></div>

<h2>Top 3 Categories by Revenue</h2>
<p>{', '.join(top3_categories) if top3_categories else 'No category data'}</p>

<h2>Store KPI Summary</h2>
{html_table(store_kpis)}

<h2>Category KPI Summary</h2>
{html_table(category_kpis)}

<h2>Month-on-Month Trend</h2>
{html_table(monthly_kpis)}

<div class=\"note\"><strong>Inventory turnover:</strong> not calculated because the source data contains a single inventory snapshot and no cost-of-goods-sold / average-inventory history.</div>
<div class=\"note\"><strong>Market share:</strong> not calculated because the generated dataset contains MegaMart sales only and no external market-total or competitor sales data. Store revenue share within MegaMart is provided in the store KPI table.</div>

<h2>Latest Month Record</h2>
{latest_month_html}
</body>
</html>"""
    html_path.write_text(html, encoding="utf-8")

    pdf_path = Path(f"{RUN_OUTPUT_PATH}/kpi_summary.pdf")
    build_pdf(
        str(pdf_path),
        [
            ("Enterprise KPI Summary", enterprise_kpis),
            ("Store KPI Summary", store_kpis),
            ("Top Categories", category_kpis.filter(F.col("category_rank") <= 3)),
            ("Month-on-Month Trend", monthly_kpis),
        ],
    )

    write_pipeline_audit(
        status="SUCCESS",
        records_processed=int(counts["monthly_kpi_summary"]),
        message=(
            "Gold KPI tables built and CSV/HTML/PDF artifacts published. "
            "Inventory turnover and market share remain explicitly uncalculated "
            "where source data does not support them."
        ),
    )

    print("\nGold KPI tables:")
    print(f"  {store_table}: {counts['store_kpi_summary']} rows")
    print(f"  {category_table}: {counts['category_kpi_summary']} rows")
    print(f"  {monthly_table}: {counts['monthly_kpi_summary']} rows")
    print(f"  {enterprise_table}: {counts['enterprise_kpi_summary']} rows")

    print("\nArtifacts:")
    print(f"  {RUN_OUTPUT_PATH}/store_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/category_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/monthly_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/enterprise_kpi_summary.csv")
    print(f"  {RUN_OUTPUT_PATH}/kpi_summary.html")
    print(f"  {RUN_OUTPUT_PATH}/kpi_summary.pdf")
    print("\nSTAGE 06_KPI COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        try:
            write_pipeline_audit(
                status="FAILED",
                records_processed=0,
                message=str(exc)[:4000],
            )
        except Exception as audit_exc:
            print(f"Audit write failed after stage failure: {audit_exc}")
        print(f"STAGE 06_KPI FAILED: {exc}")
        raise
