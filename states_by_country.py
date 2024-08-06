from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Implementing SparkSQL in PySpark").getOrCreate()
states_by_country_df = spark.read.csv("states_by_country.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary SQL table
states_by_country_df.createOrReplaceTempView("states_by_country")

# Query the table using Spark SQL
result_df = spark.sql("SELECT * FROM states_by_country")

# Show the top 50 rows
result_df.show(50)

# Show all rows (practical for small datasets)
result_df.show(result_df.count(), truncate=False)
