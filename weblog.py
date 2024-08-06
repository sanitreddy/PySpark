from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("Web Logs Analysis").getOrCreate()

# Load the dataset
logs_df = spark.read.csv("weblog.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary SQL table
logs_df.createOrReplaceTempView("web_logs")

# Execute the SQL query to find the top 10 most visited URLs
top_urls_df = spark.sql("""
SELECT url, COUNT(*) AS visit_count
FROM web_logs
GROUP BY url
ORDER BY visit_count DESC
LIMIT 10
""")

# Show the results
top_urls_df.show()

# Stop the SparkSession
spark.stop()
