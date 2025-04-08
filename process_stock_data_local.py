# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, lit

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyEMRApp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()


schema = StructType([
   StructField("date", StringType(), True),
   StructField("open", FloatType(), True),
   StructField("high", FloatType(), True),
   StructField("low", FloatType(), True),
   StructField("close", FloatType(), True),
   StructField("adjclose", FloatType(), True),
   StructField("volume", IntegerType(), True)])

amazon_df = spark.read.csv("AMZN.csv", header=True, schema=schema)
google_df = spark.read.csv("GOOG.csv", header=True, schema=schema)
tesla_df = spark.read.csv("TSLA.csv", header=True, schema=schema)

def get_avg_price_per_year(df, stock_code):
    result = df.select(year("date").alias("year"), "adjclose") \
               .groupby("year") \
               .agg({'adjclose': 'avg'}) \
               .withColumnRenamed("avg(adjclose)", "average_adj_close_price") \
               .sort("year") 
    result = result.withColumn("stockcode", lit(stock_code))
    return result

avg_close_price_per_year_amzn_df = get_avg_price_per_year(amazon_df, "AMZN")
avg_close_price_per_year_google_df = get_avg_price_per_year(google_df, "GOOG")
avg_close_price_per_year_tesla_df = get_avg_price_per_year(tesla_df, "TSLA")

print("writing processed data to S3")
avg_close_price_per_year_amzn_df.write.csv("amazon_processed.csv", header=True, mode='overwrite')
avg_close_price_per_year_google_df.write.csv("google_processed.csv", header=True, mode='overwrite')
avg_close_price_per_year_tesla_df.write.csv("tesla_processed.csv", header=True, mode='overwrite')
