from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, avg

# Initialize a SparkSession
spark = SparkSession.builder.appName("Running Total of Sales").getOrCreate()

# Load the dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Define the window specification
window_spec = Window.partitionBy("ORDERLINENUMBER").orderBy("ORDERDATE").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the running total
running_total = sales_df.withColumn("RUNNING_TOTAL", sum("PRICEEACH").over(window_spec))

# Calculate total sales in a year
total_sales_in_year = sales_df.groupBy("YEAR_ID").agg(sum("SALES").alias("TOTAL_SALES"))

# Calculate average sales in a year
average_sales_in_year = sales_df.groupBy("YEAR_ID").agg(avg("SALES").alias("AVERAGE_SALES"))

# Show the results
running_total.show()
total_sales_in_year.show()
average_sales_in_year.show()

# Stop the SparkSession
spark.stop()
