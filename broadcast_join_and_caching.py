from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize SparkSession
spark = SparkSession.builder.appName("Broadcast Join and Caching").getOrCreate()

# Load large and small datasets
country_df = spark.read.csv("country.csv", header=True, inferSchema=True)
country_full_df = spark.read.csv("country_full.csv", header=True, inferSchema=True)

# Function to rename columns to lowercase
def rename_columns_to_lowercase(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())
    return df

country_df = rename_columns_to_lowercase(country_df)

# Broadcast the smaller dataset
broadcasted_df = broadcast(country_full_df)

# Perform join operation
joined_df = country_df.join(broadcasted_df, "name")

# Cache the result for repeated use
joined_df.cache()

# Perform some actions
joined_df.show(joined_df.count())

# Unpersist the cached DataFrame
joined_df.unpersist()
