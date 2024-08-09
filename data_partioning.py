from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("Avoiding Shuffles with Data Partitioning").getOrCreate()

# Load dataset
data_df = spark.read.csv("weblog.csv", header=True, inferSchema=True)

# Repartition data based on a key column to avoid shuffles
partitioned_df = data_df.repartition("IP")

# Perform transformation and action
result_df = partitioned_df.filter(col("URL") > 200)
result_df.write.parquet("/home/sanitreddy/Python/PySpark/Output1/")
