
import os
import sys

# Tell Spark to use the current virtual environment Python
python_executable = sys.executable

os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RestaurantChainBronzeTest")
    .master("local[*]")
    .config("spark.pyspark.python", python_executable)
    .config("spark.pyspark.driver.python", python_executable)
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

print("Python Executable:", python_executable)
print("Spark Version:", spark.version)

data = [
    (1, "Cairo", 150.0),
    (2, "Mansoura", 200.0),
    (3, "Alexandria", 175.0)
]

columns = ["branch_id", "city", "sales"]

df = spark.createDataFrame(data, columns)

df.show()

print("Total Rows:", df.count())

spark.stop()

print("Spark Test Completed Successfully!")
