from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("Complex SQL Query on Sales Data").getOrCreate()

# Load the dataset
sales_df = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary SQL table
sales_df.createOrReplaceTempView("sales")

# Execute the complex SQL query
multi_region_customers_df = spark.sql("""
SELECT CUSTOMERNAME, COUNT(CITY) AS NUM_REGIONS, SUM(SALES) AS TOTAL_SALES, COUNT(*) AS NUM_TRANSACTIONS
FROM sales
GROUP BY CUSTOMERNAME
HAVING NUM_REGIONS > 1                                     
ORDER BY TOTAL_SALES DESC
""")

# Show the results
multi_region_customers_df.show()

# Stop the SparkSession
spark.stop()
