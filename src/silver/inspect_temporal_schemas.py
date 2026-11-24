
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(r"D:\depi_project")
BRONZE_ROOT = PROJECT_ROOT / "data_lake" / "bronze"

TABLES = [
    "orders",
    "order_status_history",
    "replenishment_requests",
    "replenishment_status_history",
    "warehouse_allocations",
    "shipments",
    "shipment_items",
    "inventory_movements",
    "branch_inventory_transactions",
]
TABLES = [
    "replenishment_request_items",
]


def create_spark_session():

    return (
        SparkSession.builder
        .appName("Temporal_Schema_Inspection")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def inspect_table(spark, table_name):

    table_path = BRONZE_ROOT / table_name

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")
    print("=" * 80)

    if not table_path.exists():
        print(f"TABLE NOT FOUND: {table_path}")
        return

    df = spark.read.parquet(str(table_path))

    print("\nCOLUMNS:")
    for field in df.schema.fields:
        print(
            f"{field.name:<35} "
            f"{field.dataType.simpleString()}"
        )

    print(f"\nTOTAL ROWS: {df.count()}")

    print("\nSAMPLE RECORDS:")
    df.limit(3).show(truncate=False)


def main():

    spark = create_spark_session()

    try:
        for table in TABLES:
            inspect_table(spark, table)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
