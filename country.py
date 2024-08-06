from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("Join Multiple DataFrames").getOrCreate()

# Load the datasets
country_df = spark.read.csv("country.csv", header=True, inferSchema=True)
country_full_df = spark.read.csv("country_full.csv", header=True, inferSchema=True)

# Function to rename columns to lowercase
def rename_columns_to_lowercase(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())
    return df

country_df = rename_columns_to_lowercase(country_df)

# Join the datasets
result_df = country_df.join(country_full_df, on="name", how="inner")

# Show the results
result_df.show()

# Stop the SparkSession
spark.stop()
