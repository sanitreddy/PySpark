from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, row_number, to_date
from pyspark.sql.window import Window

# Initialize a SparkSession
spark = SparkSession.builder.appName("Most Popular Product by Month").getOrCreate()

# Load the dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Extract only the date part from the datetime column
sales_df = sales_df.withColumn("DATE", to_date(col("ORDERDATE"), "M/d/yyyy H:mm"))

# Add month and year columns
sales_df = sales_df.withColumn("MONTH", month(col("DATE"))).withColumn("YEAR", year(col("DATE")))

# Register the DataFrame as a temporary SQL table
sales_df.createOrReplaceTempView("sales")

# Execute the complex SQL query
monthly_sales_df = spark.sql("""
SELECT YEAR, MONTH, PRODUCTLINE, COUNT(*) AS NUM_SALES
FROM sales
GROUP BY YEAR, MONTH, PRODUCTLINE
ORDER BY YEAR, MONTH, NUM_SALES DESC
""")

# Define the window specification
window_spec = Window.partitionBy("YEAR", "MONTH").orderBy(col("NUM_SALES").desc())

# Calculate the row number for each product within its month
ranked_sales = monthly_sales_df.withColumn("RANK", row_number().over(window_spec))

# Filter the most popular product for each month
most_popular_product_by_month = ranked_sales.filter(col("RANK") == 1)

# Show the results
most_popular_product_by_month.show()

# Stop the SparkSession
spark.stop()
