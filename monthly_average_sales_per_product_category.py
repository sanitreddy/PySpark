from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, avg

# Initialize a SparkSession
spark = SparkSession.builder.appName("Monthly Average Sales per Product Category").getOrCreate()

# Load the dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Calculate monthly average sales per product category
monthly_avg_sales = sales_df.groupBy("YEAR_ID", "MONTH_ID", "PRODUCTLINE").agg(avg("SALES").alias("AVERAGE_SALES"))

# Show the results
monthly_avg_sales.show()

# Stop the SparkSession
spark.stop()
