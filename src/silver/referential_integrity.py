
import os
import csv
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# ENVIRONMENT
# ---------------------------------------------------------

os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.storagelevel import StorageLevel


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data_lake" / "bronze"
REPORTS_PATH = PROJECT_ROOT / "reports" / "silver_referential_integrity"


# ---------------------------------------------------------
# FOREIGN KEY RELATIONSHIPS
# (child_table, child_column, parent_table, parent_column)
# ---------------------------------------------------------

RELATIONSHIPS = [
    ("branch_inventory", "branch_id", "branches", "branch_id"),
    ("branch_inventory", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("branch_inventory_transactions", "branch_id", "branches", "branch_id"),
    ("branch_inventory_transactions", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("orders", "branch_id", "branches", "branch_id"),
    ("orders", "customer_id", "customers", "customer_id"),

    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "item_id", "food_menu", "item_id"),

    ("order_status_history", "order_id", "orders", "order_id"),

    ("branch_warehouse_distances", "branch_id", "branches", "branch_id"),
    ("branch_warehouse_distances", "warehouse_id", "warehouses", "warehouse_id"),

    ("inventory_movements", "warehouse_id", "warehouses", "warehouse_id"),
    ("inventory_movements", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("menu_item_ingredients", "item_id", "food_menu", "item_id"),
    ("menu_item_ingredients", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("menu_prices", "item_id", "food_menu", "item_id"),
    ("menu_prices", "branch_id", "branches", "branch_id"),

    ("procurement_officers", "warehouse_id", "warehouses", "warehouse_id"),

    ("purchase_orders", "warehouse_id", "warehouses", "warehouse_id"),
    ("purchase_orders", "officer_id", "procurement_officers", "officer_id"),
    ("purchase_orders", "supplier_id", "suppliers", "supplier_id"),

    ("purchase_order_items", "purchase_order_id", "purchase_orders", "purchase_order_id"),
    ("purchase_order_items", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("replenishment_requests", "branch_id", "branches", "branch_id"),

    ("replenishment_request_items", "request_id", "replenishment_requests", "request_id"),
    ("replenishment_request_items", "inventory_item_id", "inventory_items", "inventory_item_id"),

    ("replenishment_status_history", "request_id", "replenishment_requests", "request_id"),

    ("shipments", "request_id", "replenishment_requests", "request_id"),

    ("shipment_items", "shipment_id", "shipments", "shipment_id"),
    ("shipment_items", "request_item_id", "replenishment_request_items", "request_item_id"),

    ("warehouse_allocations", "request_item_id", "replenishment_request_items", "request_item_id"),
    ("warehouse_allocations", "warehouse_id", "warehouses", "warehouse_id"),

    ("warehouse_inventory", "warehouse_id", "warehouses", "warehouse_id"),
    ("warehouse_inventory", "inventory_item_id", "inventory_items", "inventory_item_id"),
]


# ---------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("Silver_Referential_Integrity")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ---------------------------------------------------------
# DATA ACCESS
# ---------------------------------------------------------

def load_table(spark, table_name, table_cache):

    if table_name not in table_cache:

        table_path = BRONZE_PATH / table_name

        if not table_path.exists():
            raise FileNotFoundError(
                f"Bronze table not found: {table_name}"
            )

        df = (
            spark.read
            .parquet(str(table_path))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        table_cache[table_name] = df

    return table_cache[table_name]


def normalize_key(column_name):

    return trim(col(column_name).cast("string"))


# ---------------------------------------------------------
# SINGLE RELATIONSHIP CHECK
# ---------------------------------------------------------

def check_relationship(
    spark,
    relationship,
    table_cache,
    count_cache
):

    child_table, child_column, parent_table, parent_column = relationship

    result = {
        "child_table": child_table,
        "child_column": child_column,
        "parent_table": parent_table,
        "parent_column": parent_column,
        "total_child_rows": None,
        "null_fk_rows": None,
        "non_null_fk_rows": None,
        "matched_rows": None,
        "orphan_rows": None,
        "match_rate_percent": None,
        "status": None,
        "error": None
    }

    try:

        child_df = load_table(spark, child_table, table_cache)
        parent_df = load_table(spark, parent_table, table_cache)

        # Validate column existence
        if child_column not in child_df.columns:
            raise ValueError(
                f"Missing child column: {child_column}"
            )

        if parent_column not in parent_df.columns:
            raise ValueError(
                f"Missing parent column: {parent_column}"
            )

        # Cache total row count per table
        if child_table not in count_cache:
            count_cache[child_table] = child_df.count()

        total_rows = count_cache[child_table]

        # NULL and empty FK values
        child_key = normalize_key(child_column)

        null_condition = (
            col(child_column).isNull() |
            (child_key == "")
        )

        null_fk_rows = (
            child_df
            .filter(null_condition)
            .count()
        )

        non_null_fk_rows = total_rows - null_fk_rows

        # Child records with valid, non-empty FK values
        child_keys = (
            child_df
            .filter(~null_condition)
            .select(child_key.alias("_fk"))
        )

        # Distinct parent keys
        parent_keys = (
            parent_df
            .filter(
                col(parent_column).isNotNull() &
                (normalize_key(parent_column) != "")
            )
            .select(
                normalize_key(parent_column).alias("_pk")
            )
            .distinct()
        )

        # Find child records without a matching parent
        orphan_rows = (
            child_keys
            .join(
                parent_keys,
                child_keys["_fk"] == parent_keys["_pk"],
                "left_anti"
            )
            .count()
        )

        matched_rows = non_null_fk_rows - orphan_rows

        if non_null_fk_rows > 0:
            match_rate = round(
                (matched_rows / non_null_fk_rows) * 100,
                2
            )
        else:
            match_rate = None

        if orphan_rows > 0:
            status = "FAIL"
        elif null_fk_rows > 0:
            status = "CHECK_NULLS"
        else:
            status = "PASS"

        result.update({
            "total_child_rows": total_rows,
            "null_fk_rows": null_fk_rows,
            "non_null_fk_rows": non_null_fk_rows,
            "matched_rows": matched_rows,
            "orphan_rows": orphan_rows,
            "match_rate_percent": match_rate,
            "status": status
        })

        return result

    except Exception as error:

        result["status"] = "ERROR"
        result["error"] = str(error)

        return result


# ---------------------------------------------------------
# SAVE REPORT
# ---------------------------------------------------------

def save_report(results):

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_dir = REPORTS_PATH / f"run_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=False)

    report_file = report_dir / "referential_integrity.csv"

    fieldnames = list(results[0].keys()) if results else []

    with open(
        report_file,
        mode="w",
        newline="",
        encoding="utf-8-sig"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(results)

    return report_file


# ---------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------

def print_summary(results):

    total = len(results)

    passed = sum(
        1 for row in results
        if row["status"] == "PASS"
    )

    failed = sum(
        1 for row in results
        if row["status"] == "FAIL"
    )

    check_nulls = sum(
        1 for row in results
        if row["status"] == "CHECK_NULLS"
    )

    errors = sum(
        1 for row in results
        if row["status"] == "ERROR"
    )

    total_orphans = sum(
        row["orphan_rows"] or 0
        for row in results
    )

    total_nulls = sum(
        row["null_fk_rows"] or 0
        for row in results
    )

    print("\n" + "=" * 65)
    print("REFERENTIAL INTEGRITY SUMMARY")
    print("=" * 65)

    print(f"Total Relationships : {total}")
    print(f"PASSED              : {passed}")
    print(f"FAILED              : {failed}")
    print(f"CHECK NULLS         : {check_nulls}")
    print(f"ERRORS              : {errors}")

    print("-" * 65)

    print(f"Total Orphan Rows   : {total_orphans}")
    print(f"Total NULL FK Rows  : {total_nulls}")

    print("=" * 65)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():

    print("=" * 65)
    print("SILVER REFERENTIAL INTEGRITY CHECK")
    print("=" * 65)

    spark = create_spark_session()

    table_cache = {}
    count_cache = {}

    results = []

    try:

        print(f"\nRelationships to Check: {len(RELATIONSHIPS)}")

        for index, relationship in enumerate(
            RELATIONSHIPS,
            start=1
        ):

            child_table, child_column, parent_table, parent_column = relationship

            print("\n" + "-" * 65)
            print(
                f"[{index}/{len(RELATIONSHIPS)}] "
                f"{child_table}.{child_column} "
                f"-> {parent_table}.{parent_column}"
            )

            result = check_relationship(
                spark,
                relationship,
                table_cache,
                count_cache
            )

            results.append(result)

            if result["status"] == "ERROR":

                print("Status       : ERROR")
                print(f"Error        : {result['error']}")

            else:

                print(f"Child Rows   : {result['total_child_rows']}")
                print(f"NULL FK Rows : {result['null_fk_rows']}")
                print(f"Matched Rows : {result['matched_rows']}")
                print(f"Orphan Rows  : {result['orphan_rows']}")
                print(f"Match Rate   : {result['match_rate_percent']}")
                print(f"Status       : {result['status']}")

        report_file = save_report(results)

        print_summary(results)

        print(f"\nReport saved to:\n{report_file}")

    finally:

        for df in table_cache.values():
            df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
