

import os
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(r"D:\depi_project")
BRONZE_ROOT = PROJECT_ROOT / "data_lake" / "bronze"
REPORT_ROOT = PROJECT_ROOT / "reports" / "temporal_integrity"

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


# ============================================================
# 2. SPARK SESSION
# ============================================================

def create_spark_session():

    python_executable = sys.executable

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("Restaurant_Temporal_Integrity")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================
# 3. LOAD BRONZE TABLES
# ============================================================

def load_table(spark, table_name):

    path = BRONZE_ROOT / table_name

    if not path.exists():
        raise FileNotFoundError(
            f"Bronze table not found: {path}"
        )

    return spark.read.parquet(str(path))


def load_all_tables(spark):

    table_names = [
        "orders",
        "order_status_history",
        "replenishment_requests",
        "replenishment_request_items",
        "replenishment_status_history",
        "warehouse_allocations",
        "shipments",
        "shipment_items",
        "inventory_movements",
        "branch_inventory_transactions",
    ]

    tables = {}

    for name in table_names:
        print(f"Loading: {name}")
        tables[name] = load_table(spark, name)

    return tables


# ============================================================
# 4. TIMESTAMP PARSING
# ============================================================

def add_timestamp_column(df, source_column, target_column):

    return df.withColumn(
        target_column,
        F.to_timestamp(
            F.col(source_column),
            TIMESTAMP_FORMAT
        )
    )


def prepare_tables(tables):

    date_columns = {
        "orders": {
            "order_datetime": "order_ts"
        },

        "order_status_history": {
            "status_datetime": "status_ts"
        },

        "replenishment_requests": {
            "request_created_at": "request_ts"
        },

        "replenishment_status_history": {
            "status_datetime": "status_ts"
        },

        "warehouse_allocations": {
            "allocation_datetime": "allocation_ts"
        },

        "shipments": {
            "dispatched_at": "dispatched_ts",
            "received_at": "received_ts"
        },

        "inventory_movements": {
            "movement_datetime": "movement_ts"
        },

        "branch_inventory_transactions": {
            "transaction_datetime": "transaction_ts"
        }
    }

    prepared = {}

    for table_name, df in tables.items():

        if table_name in date_columns:

            for source_col, target_col in date_columns[
                table_name
            ].items():

                df = add_timestamp_column(
                    df,
                    source_col,
                    target_col
                )

        prepared[table_name] = df

    return prepared


# ============================================================
# 5. VIOLATION MANAGEMENT
# ============================================================

def create_violation(
    df,
    rule_id,
    rule_name,
    severity,
    entity_type,
    entity_id_column,
    event_datetime_column,
    reference_datetime_column,
    message
):

    return (
        df
        .withColumn("rule_id", F.lit(rule_id))
        .withColumn("rule_name", F.lit(rule_name))
        .withColumn("severity", F.lit(severity))
        .withColumn("entity_type", F.lit(entity_type))
        .withColumn(
            "entity_id",
            F.col(entity_id_column).cast("string")
        )
        .withColumn(
            "event_datetime",
            F.col(event_datetime_column).cast("string")
        )
        .withColumn(
            "reference_datetime",
            F.col(reference_datetime_column).cast("string")
        )
        .withColumn("violation_reason", F.lit(message))
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "entity_type",
            "entity_id",
            "event_datetime",
            "reference_datetime",
            "violation_reason"
        )
    )


def empty_violations(spark):

    schema = """
        rule_id STRING,
        rule_name STRING,
        severity STRING,
        entity_type STRING,
        entity_id STRING,
        event_datetime STRING,
        reference_datetime STRING,
        violation_reason STRING
    """

    return spark.createDataFrame([], schema)


# ============================================================
# 6. ORDER LIFECYCLE CHECKS
# ============================================================

def check_order_lifecycle(tables, spark):

    violations = empty_violations(spark)

    orders = tables["orders"].select(
        "order_id",
        "order_ts"
    )

    history = tables["order_status_history"]

    joined = history.join(
        orders,
        on="order_id",
        how="inner"
    )

    invalid = joined.filter(
        F.col("status_ts").isNotNull()
        & F.col("order_ts").isNotNull()
        & (F.col("status_ts") < F.col("order_ts"))
    )

    result = create_violation(
        invalid,
        "T01",
        "Order Event Before Order Creation",
        "ERROR",
        "ORDER",
        "order_id",
        "status_ts",
        "order_ts",
        "Order status event occurred before order creation."
    )

    violations = violations.unionByName(result)

    return violations


# ============================================================
# 7. REPLENISHMENT LIFECYCLE CHECKS
# ============================================================

def check_replenishment_lifecycle(tables, spark):

    violations = empty_violations(spark)

    requests = tables["replenishment_requests"].select(
        "request_id",
        "request_ts"
    )

    history = tables["replenishment_status_history"]

    joined = history.join(
        requests,
        on="request_id",
        how="inner"
    )

    invalid = joined.filter(
        F.col("status_ts").isNotNull()
        & F.col("request_ts").isNotNull()
        & (F.col("status_ts") < F.col("request_ts"))
    )

    result = create_violation(
        invalid,
        "T03",
        "Replenishment Event Before Request Creation",
        "ERROR",
        "REPLENISHMENT_REQUEST",
        "request_id",
        "status_ts",
        "request_ts",
        "Replenishment status event occurred before request creation."
    )

    violations = violations.unionByName(result)

    return violations


# ============================================================
# 8. ALLOCATION LIFECYCLE CHECKS
# ============================================================

def check_allocation_lifecycle(tables, spark):

    violations = empty_violations(spark)

    request_items = tables[
        "replenishment_request_items"
    ].select(
        "request_item_id",
        "request_id"
    )

    requests = tables[
        "replenishment_requests"
    ].select(
        "request_id",
        "request_ts"
    )

    allocations = tables["warehouse_allocations"]

    joined = (
        allocations
        .join(
            request_items,
            on="request_item_id",
            how="inner"
        )
        .join(
            requests,
            on="request_id",
            how="inner"
        )
    )

    invalid = joined.filter(
        F.col("allocation_ts").isNotNull()
        & F.col("request_ts").isNotNull()
        & (F.col("allocation_ts") < F.col("request_ts"))
    )

    result = create_violation(
        invalid,
        "T05",
        "Allocation Before Request Creation",
        "ERROR",
        "ALLOCATION",
        "allocation_id",
        "allocation_ts",
        "request_ts",
        "Warehouse allocation occurred before request creation."
    )

    violations = violations.unionByName(result)

    # Allocation sequence consistency.
    window_spec = (
        Window
        .partitionBy("request_item_id")
        .orderBy("allocation_sequence")
    )

    ordered = (
        allocations
        .withColumn(
            "sequence_number",
            F.col("allocation_sequence").cast("integer")
        )
        .withColumn(
            "previous_allocation_ts",
            F.lag("allocation_ts").over(window_spec)
        )
    )

    sequence_issues = ordered.filter(
        F.col("previous_allocation_ts").isNotNull()
        & F.col("allocation_ts").isNotNull()
        & (
            F.col("allocation_ts")
            < F.col("previous_allocation_ts")
        )
    )

    sequence_result = create_violation(
        sequence_issues,
        "T06",
        "Allocation Sequence Temporal Mismatch",
        "WARNING",
        "ALLOCATION",
        "allocation_id",
        "allocation_ts",
        "previous_allocation_ts",
        "Allocation sequence order does not match chronological order."
    )

    violations = violations.unionByName(sequence_result)

    return violations


# ============================================================
# 9. SHIPMENT LIFECYCLE CHECKS
# ============================================================

def check_shipment_lifecycle(tables, spark):

    violations = empty_violations(spark)

    requests = tables[
        "replenishment_requests"
    ].select(
        "request_id",
        "request_ts"
    )

    shipments = tables["shipments"]

    joined = shipments.join(
        requests,
        on="request_id",
        how="inner"
    )

    # Shipment dispatched before request creation.
    invalid_dispatch = joined.filter(
        F.col("dispatched_ts").isNotNull()
        & F.col("request_ts").isNotNull()
        & (
            F.col("dispatched_ts")
            < F.col("request_ts")
        )
    )

    result = create_violation(
        invalid_dispatch,
        "T07",
        "Shipment Before Request Creation",
        "ERROR",
        "SHIPMENT",
        "shipment_id",
        "dispatched_ts",
        "request_ts",
        "Shipment dispatched before its replenishment request was created."
    )

    violations = violations.unionByName(result)

    # Shipment received before dispatch.
    invalid_receipt = shipments.filter(
        F.col("received_ts").isNotNull()
        & F.col("dispatched_ts").isNotNull()
        & (
            F.col("received_ts")
            < F.col("dispatched_ts")
        )
    )

    result = create_violation(
        invalid_receipt,
        "T08",
        "Shipment Received Before Dispatch",
        "ERROR",
        "SHIPMENT",
        "shipment_id",
        "received_ts",
        "dispatched_ts",
        "Shipment received before dispatch."
    )

    violations = violations.unionByName(result)

    return violations


# ============================================================
# 10. INVENTORY MOVEMENT CHECKS
# ============================================================

def check_inventory_movements(tables, spark):

    violations = empty_violations(spark)

    movements = tables["inventory_movements"]

    requests = tables[
        "replenishment_requests"
    ].select(
        "request_id",
        "request_ts"
    )

    # Only known Replenishment Allocation references
    # are compared against request creation.
    joined = movements.join(
        requests,
        movements.reference_id == requests.request_id,
        "inner"
    )

    invalid = joined.filter(
        F.col("movement_ts").isNotNull()
        & F.col("request_ts").isNotNull()
        & (
            F.col("movement_ts")
            < F.col("request_ts")
        )
    )

    result = create_violation(
        invalid,
        "T09",
        "Inventory Movement Before Request Creation",
        "WARNING",
        "INVENTORY_MOVEMENT",
        "movement_id",
        "movement_ts",
        "request_ts",
        "Inventory movement occurred before referenced request creation."
    )

    violations = violations.unionByName(result)

    return violations


# ============================================================
# 11. BRANCH INVENTORY TRANSACTION CHECKS
# ============================================================

def check_branch_inventory_transactions(tables, spark):

    violations = empty_violations(spark)

    transactions = tables[
        "branch_inventory_transactions"
    ]

    orders = tables["orders"].select(
        "order_id",
        "order_ts"
    )

    joined = transactions.join(
        orders,
        transactions.reference_id == orders.order_id,
        "inner"
    )

    invalid = joined.filter(
        (F.col("transaction_type") == "Consumption")
        & F.col("transaction_ts").isNotNull()
        & F.col("order_ts").isNotNull()
        & (
            F.col("transaction_ts")
            < F.col("order_ts")
        )
    )

    result = create_violation(
        invalid,
        "T10",
        "Inventory Consumption Before Order",
        "WARNING",
        "BRANCH_TRANSACTION",
        "transaction_id",
        "transaction_ts",
        "order_ts",
        "Inventory consumption occurred before referenced order creation."
    )

    violations = violations.unionByName(result)

    return violations


# ============================================================
# 12. COMBINE ALL CHECKS
# ============================================================

def run_all_checks(tables, spark):

    checks = [
        check_order_lifecycle,
        check_replenishment_lifecycle,
        check_allocation_lifecycle,
        check_shipment_lifecycle,
        check_inventory_movements,
        check_branch_inventory_transactions
    ]

    all_violations = empty_violations(spark)

    for check in checks:

        print("\nRunning:", check.__name__)

        result = check(tables, spark)

        count = result.count()

        print("Violations found:", count)

        all_violations = all_violations.unionByName(
            result
        )

    return all_violations


# ============================================================
# 13. REPORT GENERATION
# ============================================================

def generate_reports(violations, report_path):

    report_path.mkdir(
        parents=True,
        exist_ok=True
    )

    violations.cache()

    print("\nGenerating detailed report...")

    violations.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(report_path / "temporal_violations"))

    summary = (
        violations
        .groupBy(
            "rule_id",
            "rule_name",
            "severity"
        )
        .agg(
            F.count("*").alias("violation_count")
        )
        .orderBy("rule_id")
    )

    summary.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(report_path / "temporal_summary"))

    return summary


# ============================================================
# 14. MAIN
# ============================================================

def main():

    run_id = datetime.now().strftime(
        "run_%Y%m%d_%H%M%S"
    )

    report_path = REPORT_ROOT / run_id

    spark = create_spark_session()

    try:

        print("=" * 70)
        print("RESTAURANT TEMPORAL INTEGRITY ANALYSIS")
        print("=" * 70)

        tables = load_all_tables(spark)

        tables = prepare_tables(tables)

        violations = run_all_checks(
            tables,
            spark
        )

        total = violations.count()

        errors = violations.filter(
            F.col("severity") == "ERROR"
        ).count()

        warnings = violations.filter(
            F.col("severity") == "WARNING"
        ).count()

        summary = generate_reports(
            violations,
            report_path
        )

        print("\n" + "=" * 70)
        print("TEMPORAL INTEGRITY RESULTS")
        print("=" * 70)

        print(f"Total Violations : {total}")
        print(f"Errors           : {errors}")
        print(f"Warnings         : {warnings}")

        print("\nSUMMARY:")
        summary.show(100, truncate=False)

        print("\nREPORT LOCATION:")
        print(report_path)

        print("\nTEMPORAL ANALYSIS COMPLETED")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
