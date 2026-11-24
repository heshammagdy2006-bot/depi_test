
import os
import sys
import csv
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    sum as spark_sum,
    to_timestamp,
    to_date
)


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data_lake" / "bronze"

REPORT_ROOT = PROJECT_ROOT / "reports" / "silver_profiling"

METADATA_COLUMNS = [
    "_ingestion_timestamp",
    "_source_file",
    "_batch_id"
]


# ============================================================
# 2. CANDIDATE PRIMARY KEYS
# ============================================================

CANDIDATE_KEYS = {
    "branches": ["branch_id"],

    "branch_inventory": [
        "branch_id",
        "inventory_item_id"
    ],

    "branch_inventory_transactions": [
        "transaction_id"
    ],

    "branch_warehouse_distances": [
        "branch_id",
        "warehouse_id"
    ],

    "customers": ["customer_id"],

    "food_menu": ["item_id"],

    "inventory_items": ["inventory_item_id"],

    "inventory_movements": ["movement_id"],

    "menu_item_ingredients": [
        "item_id",
        "inventory_item_id"
    ],

    "menu_prices": [
        "item_id",
        "branch_id",
        "valid_from"
    ],

    "orders": ["order_id"],

    "order_items": ["order_item_id"],

    "order_status_history": ["status_event_id"],

    "procurement_officers": ["officer_id"],

    "purchase_orders": ["purchase_order_id"],

    "purchase_order_items": ["po_item_id"],

    "replenishment_requests": ["request_id"],

    "replenishment_request_items": ["request_item_id"],

    "replenishment_status_history": ["status_event_id"],

    "shipments": ["shipment_id"],

    "shipment_items": ["shipment_item_id"],

    "suppliers": ["supplier_id"],

    "warehouses": ["warehouse_id"],

    "warehouse_allocations": ["allocation_id"],

    "warehouse_inventory": [
        "warehouse_id",
        "inventory_item_id"
    ]
}


# ============================================================
# 3. NUMERIC COLUMNS
# ============================================================

NUMERIC_COLUMNS = {
    "branch_inventory": [
        "quantity_on_hand",
        "reorder_level"
    ],

    "branch_inventory_transactions": [
        "quantity_change"
    ],

    "branch_warehouse_distances": [
        "distance_km"
    ],

    "inventory_movements": [
        "quantity_change"
    ],

    "menu_item_ingredients": [
        "quantity_required"
    ],

    "menu_prices": [
        "selling_price"
    ],

    "order_items": [
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total"
    ],

    "purchase_order_items": [
        "ordered_qty",
        "unit_cost",
        "received_qty"
    ],

    "replenishment_request_items": [
        "requested_qty"
    ],

    "shipment_items": [
        "shipped_qty",
        "received_qty"
    ],

    "warehouse_allocations": [
        "allocated_qty",
        "allocation_sequence"
    ],

    "warehouse_inventory": [
        "quantity_on_hand",
        "reserved_quantity",
        "available_quantity"
    ]
}


# Negative values are allowed in movement tables.
ALLOW_NEGATIVE = {
    "branch_inventory_transactions": [
        "quantity_change"
    ],

    "inventory_movements": [
        "quantity_change"
    ]
}


# ============================================================
# 4. DATE COLUMNS
# ============================================================

TIMESTAMP_COLUMNS = {
    "branch_inventory": [
        "last_updated_at"
    ],

    "branch_inventory_transactions": [
        "transaction_datetime"
    ],

    "customers": [
        "created_at"
    ],

    "inventory_movements": [
        "movement_datetime"
    ],

    "orders": [
        "order_datetime"
    ],

    "order_status_history": [
        "status_datetime"
    ],

    "replenishment_requests": [
        "request_created_at"
    ],

    "replenishment_status_history": [
        "status_datetime"
    ],

    "shipments": [
        "dispatched_at",
        "received_at"
    ],

    "warehouse_allocations": [
        "allocation_datetime"
    ],

    "warehouse_inventory": [
        "last_updated_at"
    ]
}


DATE_COLUMNS = {
    "menu_prices": [
        "valid_from",
        "valid_to"
    ],

    "purchase_orders": [
        "order_date",
        "expected_delivery_date"
    ]
}


# ============================================================
# 5. SPARK SESSION
# ============================================================

def create_spark_session():

    os.environ["HADOOP_HOME"] = r"C:\hadoop"

    os.environ["PYSPARK_PYTHON"] = sys.executable

    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("SilverDataProfiling")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 6. GET BRONZE TABLES
# ============================================================

def get_bronze_tables():

    if not BRONZE_PATH.exists():
        raise FileNotFoundError(
            f"Bronze directory not found: {BRONZE_PATH}"
        )

    return sorted([
        path
        for path in BRONZE_PATH.iterdir()
        if path.is_dir() and (path / "_SUCCESS").exists()
    ])


# ============================================================
# 7. READ BRONZE TABLE
# ============================================================

def read_bronze_table(spark, table_path):

    return spark.read.parquet(str(table_path))


# ============================================================
# 8. COLUMN PROFILING
# ============================================================

def profile_columns(df, table_name):

    source_columns = [
        c for c in df.columns
        if c not in METADATA_COLUMNS
    ]

    expressions = []

    for column_name in source_columns:

        expressions.append(
            spark_sum(
                when(
                    col(column_name).isNull(),
                    1
                ).otherwise(0)
            ).alias(f"{column_name}__nulls")
        )

        expressions.append(
            spark_sum(
                when(
                    trim(col(column_name)) == "",
                    1
                ).otherwise(0)
            ).alias(f"{column_name}__empty")
        )

    if not expressions:
        return []

    results = df.agg(*expressions).first()

    report = []

    for column_name in source_columns:

        null_count = results[f"{column_name}__nulls"] or 0

        empty_count = results[f"{column_name}__empty"] or 0

        report.append({
            "table_name": table_name,
            "column_name": column_name,
            "null_count": null_count,
            "empty_count": empty_count
        })

    return report


# ============================================================
# 9. NUMERIC PROFILING
# ============================================================

def profile_numeric_columns(df, table_name):

    numeric_columns = NUMERIC_COLUMNS.get(
        table_name,
        []
    )

    report = []

    allowed_negative_columns = ALLOW_NEGATIVE.get(
        table_name,
        []
    )

    for column_name in numeric_columns:

        if column_name not in df.columns:
            continue

        numeric_value = col(column_name).cast("double")

        invalid_count = df.filter(
            col(column_name).isNotNull()
            & (trim(col(column_name)) != "")
            & numeric_value.isNull()
        ).count()

        negative_count = df.filter(
            numeric_value < 0
        ).count()

        report.append({
            "table_name": table_name,
            "column_name": column_name,
            "invalid_numeric_count": invalid_count,
            "negative_count": negative_count,
            "negative_allowed": (
                column_name in allowed_negative_columns
            )
        })

    return report


# ============================================================
# 10. DATE PROFILING
# ============================================================

def profile_date_columns(df, table_name):

    report = []

    timestamp_columns = TIMESTAMP_COLUMNS.get(
        table_name,
        []
    )

    date_columns = DATE_COLUMNS.get(
        table_name,
        []
    )

    for column_name in timestamp_columns:

        if column_name not in df.columns:
            continue

        invalid_count = df.filter(
            col(column_name).isNotNull()
            & (trim(col(column_name)) != "")
            & to_timestamp(
                col(column_name),
                "yyyy-MM-dd HH:mm:ss"
            ).isNull()
        ).count()

        report.append({
            "table_name": table_name,
            "column_name": column_name,
            "date_type": "TIMESTAMP",
            "invalid_date_count": invalid_count
        })

    for column_name in date_columns:

        if column_name not in df.columns:
            continue

        invalid_count = df.filter(
            col(column_name).isNotNull()
            & (trim(col(column_name)) != "")
            & to_date(
                col(column_name),
                "yyyy-MM-dd"
            ).isNull()
        ).count()

        report.append({
            "table_name": table_name,
            "column_name": column_name,
            "date_type": "DATE",
            "invalid_date_count": invalid_count
        })

    return report


# ============================================================
# 11. KEY PROFILING
# ============================================================

def profile_candidate_keys(df, table_name):

    keys = CANDIDATE_KEYS.get(table_name, [])

    if not keys:
        return {}

    missing_keys = [
        key for key in keys
        if key not in df.columns
    ]

    if missing_keys:
        return {
            "candidate_key": ",".join(keys),
            "key_null_rows": None,
            "duplicate_key_groups": None,
            "key_status": "MISSING_COLUMNS"
        }

    null_condition = None

    for key in keys:

        condition = (
            col(key).isNull()
            | (trim(col(key)) == "")
        )

        if null_condition is None:
            null_condition = condition
        else:
            null_condition = null_condition | condition

    key_null_rows = df.filter(null_condition).count()

    duplicate_key_groups = (
        df.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    return {
        "candidate_key": ",".join(keys),
        "key_null_rows": key_null_rows,
        "duplicate_key_groups": duplicate_key_groups,
        "key_status": (
            "PASS"
            if key_null_rows == 0
            and duplicate_key_groups == 0
            else "CHECK"
        )
    }


# ============================================================
# 12. FULL ROW DUPLICATES
# ============================================================

def count_full_duplicates(df):

    source_columns = [
        c for c in df.columns
        if c not in METADATA_COLUMNS
    ]

    total_rows = df.count()

    unique_rows = (
        df.select(*source_columns)
        .dropDuplicates()
        .count()
    )

    return total_rows - unique_rows


# ============================================================
# 13. PROFILE ONE TABLE
# ============================================================

def profile_table(spark, table_path):

    table_name = table_path.name

    print("\n" + "=" * 65)
    print(f"PROFILING TABLE: {table_name}")
    print("=" * 65)

    df = read_bronze_table(spark, table_path)

    total_rows = df.count()

    total_columns = len([
        c for c in df.columns
        if c not in METADATA_COLUMNS
    ])

    column_report = profile_columns(
        df,
        table_name
    )

    numeric_report = profile_numeric_columns(
        df,
        table_name
    )

    date_report = profile_date_columns(
        df,
        table_name
    )

    key_report = profile_candidate_keys(
        df,
        table_name
    )

    duplicate_rows = count_full_duplicates(df)

    print(f"Rows: {total_rows}")
    print(f"Columns: {total_columns}")
    print(f"Full Duplicate Rows: {duplicate_rows}")

    if key_report:
        print(
            f"Candidate Key: {key_report['candidate_key']}"
        )
        print(
            f"Key Null Rows: {key_report['key_null_rows']}"
        )
        print(
            f"Duplicate Key Groups: "
            f"{key_report['duplicate_key_groups']}"
        )
        print(
            f"Key Status: {key_report['key_status']}"
        )

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "full_duplicate_rows": duplicate_rows,
        "key_report": key_report,
        "column_report": column_report,
        "numeric_report": numeric_report,
        "date_report": date_report
    }


# ============================================================
# 14. SAVE CSV REPORTS
# ============================================================

def save_csv_report(file_path, rows, fieldnames):

    with open(
        file_path,
        mode="w",
        newline="",
        encoding="utf-8-sig"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            extrasaction="ignore"
        )

        writer.writeheader()

        writer.writerows(rows)


def save_reports(results):

    run_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    report_path = REPORT_ROOT / f"run_{run_id}"

    report_path.mkdir(
        parents=True,
        exist_ok=False
    )

    table_rows = []
    column_rows = []
    numeric_rows = []
    date_rows = []
    key_rows = []

    for result in results:

        table_rows.append({
            "table_name": result["table_name"],
            "total_rows": result["total_rows"],
            "total_columns": result["total_columns"],
            "full_duplicate_rows": result["full_duplicate_rows"]
        })

        column_rows.extend(
            result["column_report"]
        )

        numeric_rows.extend(
            result["numeric_report"]
        )

        date_rows.extend(
            result["date_report"]
        )

        if result["key_report"]:

            key_rows.append({
                "table_name": result["table_name"],
                **result["key_report"]
            })

    save_csv_report(
        report_path / "table_profile.csv",
        table_rows,
        [
            "table_name",
            "total_rows",
            "total_columns",
            "full_duplicate_rows"
        ]
    )

    save_csv_report(
        report_path / "column_profile.csv",
        column_rows,
        [
            "table_name",
            "column_name",
            "null_count",
            "empty_count"
        ]
    )

    save_csv_report(
        report_path / "numeric_profile.csv",
        numeric_rows,
        [
            "table_name",
            "column_name",
            "invalid_numeric_count",
            "negative_count",
            "negative_allowed"
        ]
    )

    save_csv_report(
        report_path / "date_profile.csv",
        date_rows,
        [
            "table_name",
            "column_name",
            "date_type",
            "invalid_date_count"
        ]
    )

    save_csv_report(
        report_path / "key_profile.csv",
        key_rows,
        [
            "table_name",
            "candidate_key",
            "key_null_rows",
            "duplicate_key_groups",
            "key_status"
        ]
    )

    return report_path


# ============================================================
# 15. MAIN
# ============================================================

def main():

    spark = None

    results = []

    errors = []

    try:

        spark = create_spark_session()

        table_paths = get_bronze_tables()

        print("\n" + "=" * 65)
        print("SILVER DATA PROFILING")
        print("=" * 65)

        print(f"Bronze Tables Found: {len(table_paths)}")

        for table_path in table_paths:

            try:

                result = profile_table(
                    spark,
                    table_path
                )

                results.append(result)

            except Exception as error:

                print(
                    f"\nERROR profiling {table_path.name}: {error}"
                )

                errors.append({
                    "table_name": table_path.name,
                    "error": str(error)
                })

        if results:

            report_path = save_reports(results)

            print("\n" + "=" * 65)
            print("PROFILING SUMMARY")
            print("=" * 65)

            print(f"Tables Profiled: {len(results)}")
            print(f"Tables Failed: {len(errors)}")

            print("\nReports Saved To:")
            print(report_path)

        if errors:

            print("\nTABLE ERRORS:")

            for error in errors:
                print(
                    f"{error['table_name']}: {error['error']}"
                )

        print("\nDATA PROFILING COMPLETED!")

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
