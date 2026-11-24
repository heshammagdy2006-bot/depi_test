
import os
import sys
import hashlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col
)


# ==========================================
# 1. Create Spark Session
# ==========================================

def create_spark_session():

    python_executable = sys.executable

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    spark = (
        SparkSession.builder
        .appName("RestaurantBatchIngestion")
        .master("local[*]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==========================================
# 2. Get Source CSV Files
# ==========================================

def get_source_files():

    source_path = Path("source")

    csv_files = [
        file for file in source_path.glob("*.csv")
        if file.name != "sample_manifest.csv"
    ]

    return sorted(csv_files)


# ==========================================
# 3. Generate Stable Batch ID
# ==========================================

def generate_batch_id(file_path):

    file_hash = hashlib.sha256()

    with open(file_path, "rb") as file:

        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            file_hash.update(chunk)

    return file_hash.hexdigest()


# ==========================================
# 4. Read CSV File
# ==========================================

def read_csv(spark, file_path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(str(file_path))
    )

    return df


# ==========================================
# 5. Add Metadata
# ==========================================

def add_metadata(df, file_path, batch_id):

    df = (
        df.withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            lit(file_path.name)
        )
        .withColumn(
            "_batch_id",
            lit(batch_id)
        )
    )

    return df


# ==========================================
# 6. Check Existing Batch
# ==========================================

def batch_exists(spark, output_path, batch_id):

    if not output_path.exists():
        return False

    success_file = output_path / "_SUCCESS"

    if not success_file.exists():
        raise RuntimeError(
            f"Incomplete Bronze output detected: {output_path}"
        )

    existing_df = spark.read.parquet(str(output_path))

    exists = (
        existing_df
        .filter(col("_batch_id") == batch_id)
        .limit(1)
        .count() > 0
    )

    return exists


# ==========================================
# 7. Write Data to Bronze
# ==========================================

def write_bronze(df, output_path):

    (
        df.write
        .mode("append")
        .format("parquet")
        .save(str(output_path.resolve()))
    )


# 8. Process One CSV File


def process_file(spark, file_path):

    table_name = file_path.stem

    output_path = (
        Path("data_lake")
        / "bronze"
        / table_name
    )

    batch_id = generate_batch_id(file_path)

    print("\n" + "=" * 50)
    print("Processing:", file_path.name)
    print("Batch ID:", batch_id)

    # Check whether this batch was already ingested
    if batch_exists(spark, output_path, batch_id):

        print("SKIPPED: Batch already exists")

        return "skipped"

    # Read source data
    df = read_csv(spark, file_path)

    row_count = df.count()

    print("Source Rows:", row_count)

    # Add ingestion metadata
    df = add_metadata(df, file_path, batch_id)

    # Write to Bronze
    write_bronze(df, output_path)

    print("Saved:", output_path)
    print("Status: SUCCESS")

    return "success"


# 9. Main


def main():

    spark = create_spark_session()

    try:

        files = get_source_files()

        print("\nTotal CSV Files:", len(files))

        results = {
            "success": 0,
            "skipped": 0,
            "failed": 0
        }

        for file_path in files:

            try:

                status = process_file(spark, file_path)

                results[status] += 1

            except Exception as error:

                results["failed"] += 1

                print("\nFAILED:", file_path.name)
                print("Error:", str(error))

        # Final Summary
        print("\n" + "=" * 50)
        print("BATCH INGESTION SUMMARY")
        print("=" * 50)

        print("Total Files:", len(files))
        print("Successful:", results["success"])
        print("Skipped:", results["skipped"])
        print("Failed:", results["failed"])

        print("\nBatch Ingestion Completed!")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
