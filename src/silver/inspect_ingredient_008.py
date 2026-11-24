
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data_lake" / "bronze"


def create_spark_session():

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("InspectIngredient008")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def inspect_ingredient(spark):

    inventory_items = spark.read.parquet(
        str(BRONZE_PATH / "inventory_items")
    )

    print("\nINGREDIENT DETAILS")
    print("=" * 60)

    inventory_items.filter(
        inventory_items.inventory_item_id == "ING008"
    ).show(truncate=False)


def inspect_all_ingredient_duplicates(spark):

    df = spark.read.parquet(
        str(BRONZE_PATH / "menu_item_ingredients")
    )

    duplicate_groups = (
        df.groupBy("item_id", "inventory_item_id")
        .agg(
            count("*").alias("record_count"),
            spark_sum("quantity_required").alias("total_quantity")
        )
        .filter("record_count > 1")
    )

    print("\nDUPLICATE GROUP SUMMARY")
    print("=" * 60)

    duplicate_groups.groupBy("inventory_item_id").agg(
        count("*").alias("duplicate_groups"),
        spark_sum("record_count").alias("total_records"),
        spark_sum("total_quantity").alias("total_quantity")
    ).show(truncate=False)


def main():

    spark = None

    try:

        spark = create_spark_session()

        inspect_ingredient(spark)

        inspect_all_ingredient_duplicates(spark)

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
