# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyEMRApp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()


schema = StructType([
   StructField("Date", StringType(), True),
   StructField("Open", FloatType(), True),
   StructField("High", FloatType(), True),
   StructField("Low", FloatType(), True),
   StructField("Close", FloatType(), True),
   StructField("AdjClose", FloatType(), True),
   StructField("Volume", IntegerType(), True)])

amazon_df = spark.read.csv("AMZN.csv", header=True, schema=schema)
google_df = spark.read.csv("GOOG.csv", header=True, schema=schema)
tesla_df = spark.read.csv("TSLA.csv", header=True, schema=schema)

# Average closing price per year for AMZN
avg_close_price_per_year_amzn_df = amazon_df \
    .select(year("Date").alias("year"), "AdjClose") \
    .groupby("year") \
    .agg({'AdjClose': 'avg'}) \
    .withColumnRenamed("avg(AdjClose)", "average_adj_close_price") \
    .sort("year")

print("Average closing price per year for AMZN:")
avg_close_price_per_year_amzn_df.show()


# Average closing price per year for GOOG
avg_close_price_per_year_google_df = google_df \
    .select(year("Date").alias("year"), "AdjClose") \
    .groupby("year") \
    .agg({'AdjClose': 'avg'}) \
    .withColumnRenamed("avg(AdjClose)", "average_adj_close_price") \
    .sort("year")

print("Average closing price per year for GOOG:")
avg_close_price_per_year_google_df.show()


# Average closing price per year for TSLA
avg_close_price_per_year_tesla_df = tesla_df \
    .select(year("Date").alias("year"), "AdjClose") \
    .groupby("year") \
    .agg({'AdjClose': 'avg'}) \
    .withColumnRenamed("avg(AdjClose)", "average_adj_close_price") \
    .sort("year")

print("Average closing price per year for TSLA:")
avg_close_price_per_year_tesla_df.show()

print("writing processed data to S3")
avg_close_price_per_year_amzn_df.write.csv("amazon_processed.csv", header=True, mode='overwrite')
avg_close_price_per_year_google_df.write.csv("google_processed.csv", header=True, mode='overwrite')
avg_close_price_per_year_tesla_df.write.csv("tesla_processed.csv", header=True, mode='overwrite')
