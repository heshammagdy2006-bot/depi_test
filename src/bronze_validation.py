
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct


# ==========================================
# CONFIGURATION
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_PATH = PROJECT_ROOT / "source"

BRONZE_PATH = PROJECT_ROOT / "data_lake" / "bronze"

EXCLUDED_FILES = ["sample_manifest.csv"]

REQUIRED_METADATA = [
    "_ingestion_timestamp",
    "_source_file",
    "_batch_id"
]


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
        .appName("BronzeDataQualityValidation")
        .master("local[*]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ==========================================
# 2. Get Source Files
# ==========================================

def get_source_files():

    csv_files = [
        file
        for file in SOURCE_PATH.glob("*.csv")
        if file.name not in EXCLUDED_FILES
    ]

    return sorted(csv_files)


# ==========================================
# 3. Read Source CSV
# ==========================================

def read_source_csv(spark, file_path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(str(file_path))
    )

    return df


# ==========================================
# 4. Read Bronze Table
# ==========================================

def read_bronze_table(spark, table_name):

    bronze_table_path = BRONZE_PATH / table_name

    if not bronze_table_path.exists():
        raise FileNotFoundError(
            f"Bronze table not found: {bronze_table_path}"
        )

    if not (bronze_table_path / "_SUCCESS").exists():
        raise RuntimeError(
            f"Bronze table is incomplete: {table_name}"
        )

    df = spark.read.parquet(str(bronze_table_path))

    return df


# ==========================================
# 5. Validate Row Count
# ==========================================

def validate_row_count(source_df, bronze_df):

    source_count = source_df.count()

    bronze_count = bronze_df.count()

    difference = source_count - bronze_count

    status = (
        "PASS"
        if difference == 0
        else "FAIL"
    )

    return {
        "source_rows": source_count,
        "bronze_rows": bronze_count,
        "difference": difference,
        "status": status
    }


# ==========================================
# 6. Validate Metadata Columns
# ==========================================

def validate_metadata(bronze_df):

    existing_columns = bronze_df.columns

    missing_columns = [
        column
        for column in REQUIRED_METADATA
        if column not in existing_columns
    ]

    if missing_columns:

        return {
            "status": "FAIL",
            "missing_columns": missing_columns
        }

    return {
        "status": "PASS",
        "missing_columns": []
    }


# ==========================================
# 7. Validate Batch Information
# ==========================================

def validate_batch_information(bronze_df, table_name):

    batch_summary = (
        bronze_df
        .groupBy("_batch_id", "_source_file")
        .count()
    )

    batch_count = batch_summary.count()

    # A batch should not have a null or empty ID.
    invalid_batch_ids = (
        bronze_df
        .filter(
            col("_batch_id").isNull()
            | (col("_batch_id") == "")
        )
        .limit(1)
        .count()
    )

    # Check whether each batch has the expected source filename.
    expected_source_file = f"{table_name}.csv"

    invalid_source_files = (
        bronze_df
        .filter(
            col("_source_file").isNull()
            | (col("_source_file") != expected_source_file)
        )
        .limit(1)
        .count()
    )

    # Each batch ID should belong to only one source file.
    batch_id_file_pairs = (
        bronze_df
        .select("_batch_id", "_source_file")
        .distinct()
    )

    distinct_batch_ids = (
        batch_id_file_pairs
        .select("_batch_id")
        .distinct()
        .count()
    )

    batch_id_duplicate_mapping = (
        batch_count != distinct_batch_ids
    )

    if (
        invalid_batch_ids > 0
        or invalid_source_files > 0
        or batch_id_duplicate_mapping
    ):

        return {
            "status": "FAIL",
            "batch_count": batch_count,
            "invalid_batch_ids": invalid_batch_ids,
            "invalid_source_files": invalid_source_files,
            "duplicate_batch_mapping": batch_id_duplicate_mapping
        }

    return {
        "status": "PASS",
        "batch_count": batch_count,
        "invalid_batch_ids": 0,
        "invalid_source_files": 0,
        "duplicate_batch_mapping": False
    }


# ==========================================
# 8. Validate One Table
# ==========================================

def validate_table(spark, file_path):

    table_name = file_path.stem

    print("\n" + "=" * 60)
    print("VALIDATING TABLE:", table_name)
    print("=" * 60)

    source_df = read_source_csv(spark, file_path)

    bronze_df = read_bronze_table(spark, table_name)

    # Row Count Check
    row_result = validate_row_count(
        source_df,
        bronze_df
    )

    # Metadata Check
    metadata_result = validate_metadata(
        bronze_df
    )

    # Batch Information Check
    batch_result = validate_batch_information(
        bronze_df,
        table_name
    )

    # Overall Table Status
    checks = [
        row_result["status"],
        metadata_result["status"],
        batch_result["status"]
    ]

    overall_status = (
        "PASS"
        if all(status == "PASS" for status in checks)
        else "FAIL"
    )

    result = {
        "table": table_name,
        "source_rows": row_result["source_rows"],
        "bronze_rows": row_result["bronze_rows"],
        "difference": row_result["difference"],
        "row_count_status": row_result["status"],
        "metadata_status": metadata_result["status"],
        "batch_status": batch_result["status"],
        "batch_count": batch_result["batch_count"],
        "overall_status": overall_status
    }

    print("Source Rows:", result["source_rows"])
    print("Bronze Rows:", result["bronze_rows"])
    print("Difference:", result["difference"])

    print("Row Count:", result["row_count_status"])
    print("Metadata:", result["metadata_status"])
    print("Batch Information:", result["batch_status"])
    print("Batch Count:", result["batch_count"])

    print("OVERALL STATUS:", result["overall_status"])

    return result


# ==========================================
# 9. Generate Final Report
# ==========================================

def print_final_report(results):

    print("\n\n" + "=" * 100)
    print("BRONZE DATA QUALITY VALIDATION REPORT")
    print("=" * 100)

    headers = [
        "TABLE",
        "SOURCE",
        "BRONZE",
        "DIFF",
        "BATCHES",
        "STATUS"
    ]

    print(
        f"{headers[0]:35}"
        f"{headers[1]:>12}"
        f"{headers[2]:>12}"
        f"{headers[3]:>10}"
        f"{headers[4]:>10}"
        f"{headers[5]:>12}"
    )

    print("-" * 100)

    for result in results:

        print(
            f"{result['table']:35}"
            f"{result['source_rows']:>12}"
            f"{result['bronze_rows']:>12}"
            f"{result['difference']:>10}"
            f"{result['batch_count']:>10}"
            f"{result['overall_status']:>12}"
        )

    print("-" * 100)

    passed = sum(
        1 for result in results
        if result["overall_status"] == "PASS"
    )

    failed = sum(
        1 for result in results
        if result["overall_status"] == "FAIL"
    )

    errors = len(results) - passed - failed

    print("\nTOTAL TABLES:", len(results))
    print("PASSED:", passed)
    print("FAILED:", failed)
    print("ERRORS:", errors)

    print("\nBRONZE VALIDATION COMPLETED!")


# ==========================================
# 10. Main
# ==========================================

def main():

    spark = create_spark_session()

    results = []

    try:

        files = get_source_files()

        print("\nTotal Source Tables:", len(files))

        for file_path in files:

            try:

                result = validate_table(
                    spark,
                    file_path
                )

                results.append(result)

            except Exception as error:

                print("\nERROR:", file_path.name)
                print("Reason:", str(error))

                results.append({
                    "table": file_path.stem,
                    "source_rows": 0,
                    "bronze_rows": 0,
                    "difference": 0,
                    "row_count_status": "ERROR",
                    "metadata_status": "ERROR",
                    "batch_status": "ERROR",
                    "batch_count": 0,
                    "overall_status": "ERROR"
                })

        print_final_report(results)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
