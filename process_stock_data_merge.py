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
   StructField("Date", StringType(), True),
   StructField("Open", FloatType(), True),
   StructField("High", FloatType(), True),
   StructField("Low", FloatType(), True),
   StructField("Close", FloatType(), True),
   StructField("AdjClose", FloatType(), True),
   StructField("Volume", IntegerType(), True)])

amazon_df = spark.read.csv("AMZN.csv", header=True, schema=schema)
amazon_df = amazon_df.withColumn("StockCode", lit("AMZN"))

google_df = spark.read.csv("GOOG.csv", header=True, schema=schema)
google_df = google_df.withColumn("StockCode", lit("GOOG"))

tesla_df = spark.read.csv("TSLA.csv", header=True, schema=schema)
tesla_df = tesla_df.withColumn("StockCode", lit("TSLA"))

merged_df = amazon_df.union(google_df).union(tesla_df)
merged_df.write.parquet("merged_stock_data.parquet", mode='overwrite')