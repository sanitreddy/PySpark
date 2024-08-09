from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize SparkSession
spark = SparkSession.builder.appName("Handling Complex Nested Structures").getOrCreate()

# Load dataset with nested JSON
data_df = spark.read.json("geo_nested_data.json", multiLine=True)

# Show the schema of the DataFrame
data_df.printSchema()

# Explode nested structures
exploded_df = data_df.withColumn("nested_column", explode(col("features")))

# Select relevant columns from the exploded structure
result_df = exploded_df.select("nested_column.geometry.type", "nested_column.geometry.coordinates" )

# # Show the results
result_df.show(truncate=False)
