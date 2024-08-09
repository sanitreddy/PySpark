from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Data Handling with Partitioning").getOrCreate()

# Read data from JSON file with 'permissive' mode to handle corrupt records
data_df = spark.read.json("json_to_parquet.json",  multiLine=True)

# Show the schema of the DataFrame
data_df.printSchema()

# Write data to Parquet format with partitioning
data_df.write.partitionBy("Country Name").parquet("/home/sanitreddy/Python/PySpark/Output1/")

# Read partitioned data
partitioned_data_df = spark.read.parquet("/home/sanitreddy/Python/PySpark/Output1/")

# Show the results
partitioned_data_df.show()
