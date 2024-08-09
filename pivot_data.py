from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Pivoting Data").getOrCreate()

# Load dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Pivot data to get total sales per product per year
pivoted_df = sales_df.groupBy("PRODUCTCODE").pivot("YEAR_ID").sum("SALES")

# Show the results
pivoted_df.show()
