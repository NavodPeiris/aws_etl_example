import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year

# Spark logger (writes to CloudWatch on EMR by default)
log4jLogger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("MyEMRApp")

start_time = time.time()

spark = SparkSession.builder \
    .master("yarn") \
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

amazon_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'AMZN'")

google_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'GOOG'")

tesla_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'TSLA'")


def get_avg_price_per_year(df, name):
    result = df.select(year("Date").alias("year"), "AdjClose") \
               .groupby("year") \
               .agg({'AdjClose': 'avg'}) \
               .withColumnRenamed("avg(AdjClose)", "average_adj_close_price") \
               .sort("year")
    logger.info(f"Computed average closing price per year for {name}")
    return result

avg_close_price_per_year_amzn_df = get_avg_price_per_year(amazon_df, "AMZN")
avg_close_price_per_year_google_df = get_avg_price_per_year(google_df, "GOOG")
avg_close_price_per_year_tesla_df = get_avg_price_per_year(tesla_df, "TSLA")

# 3. Write to Iceberg table partitioned by StockCode
avg_close_price_per_year_amzn_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .partitionedBy("StockCode") \
    .createOrReplace()

avg_close_price_per_year_google_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .partitionedBy("StockCode") \
    .createOrReplace()

avg_close_price_per_year_tesla_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .partitionedBy("StockCode") \
    .createOrReplace()


end_time = time.time()
elapsed_time = round(end_time - start_time, 2)

logger.info(f"Spark job exec_time={elapsed_time} seconds")