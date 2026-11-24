


import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TABLE_PATH = (
    PROJECT_ROOT
    / "data_lake"
    / "bronze"
    / "menu_item_ingredients"
)


def create_spark_session():

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("InspectMenuIngredients")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def read_table(spark):

    return spark.read.parquet(str(TABLE_PATH))


def inspect_duplicate_keys(df):

    print("\n" + "=" * 70)
    print("DUPLICATE BUSINESS KEYS")
    print("=" * 70)

    duplicate_keys = (
        df.groupBy("item_id", "inventory_item_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .orderBy(col("record_count").desc())
    )

    duplicate_keys.show(100, truncate=False)

    return duplicate_keys


def inspect_duplicate_details(df, duplicate_keys):

    print("\n" + "=" * 70)
    print("DETAILS OF DUPLICATE KEYS")
    print("=" * 70)

    details = (
        df.join(
            duplicate_keys.select(
                "item_id",
                "inventory_item_id"
            ),
            on=["item_id", "inventory_item_id"],
            how="inner"
        )
        .select(
            "item_id",
            "inventory_item_id",
            "quantity_required",
            "unit"
        )
        .orderBy(
            "item_id",
            "inventory_item_id"
        )
    )

    details.show(200, truncate=False)


def inspect_exact_duplicates(df):

    print("\n" + "=" * 70)
    print("EXACT DUPLICATE RECORDS")
    print("=" * 70)

    exact_duplicates = (
        df.groupBy(
            "item_id",
            "inventory_item_id",
            "quantity_required",
            "unit"
        )
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .orderBy(col("record_count").desc())
    )

    exact_duplicates.show(100, truncate=False)


def main():

    spark = None

    try:

        spark = create_spark_session()

        df = read_table(spark)

        print("Total Rows:", df.count())

        duplicate_keys = inspect_duplicate_keys(df)

        inspect_duplicate_details(
            df,
            duplicate_keys
        )

        inspect_exact_duplicates(df)

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
