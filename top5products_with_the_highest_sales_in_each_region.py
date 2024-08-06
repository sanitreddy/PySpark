from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize a SparkSession
spark = SparkSession.builder.appName("Top Products by Sales in Each Region").getOrCreate()

# Load the dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Define the window specification
window_spec = Window.partitionBy("CITY").orderBy(col("SALES").desc())

# Calculate the row number for each product within its region
ranked_sales = sales_df.withColumn("RANK", row_number().over(window_spec))

# Filter the top 5 products for each region
top_products_by_region = ranked_sales.filter(col("RANK") <= 5)

# Select specific columns to display
selected_columns = top_products_by_region.select("CITY", "PRODUCTLINE", "SALES", "RANK")

# Show the results
selected_columns.show()

# Stop the SparkSession
spark.stop()
