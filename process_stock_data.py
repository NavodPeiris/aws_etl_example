import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year, lit

# Spark logger (writes to CloudWatch on EMR by default)
log4jLogger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("MyEMRApp")

start_time = time.time()

spark = SparkSession.builder \
    .master("yarn") \
    .appName("MyEMRApp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://navod-iceberg-warehouse/") \
    .getOrCreate()

amazon_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'AMZN'")

google_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'GOOG'")

tesla_df = spark.read.table("iceberg_catalog.stock_db.stock_table") \
    .where("StockCode = 'TSLA'")


def get_avg_price_per_year(df, stock_code):
    result = df.select(year("date").alias("year"), "adjclose") \
               .groupby("year") \
               .agg({'adjclose': 'avg'}) \
               .withColumnRenamed("avg(adjclose)", "average_adj_close_price") \
               .sort("year") 
    result = result.withColumn("stockcode", lit(stock_code))
    logger.info(f"Computed average closing price per year for {stock_code}")
    return result

avg_close_price_per_year_amzn_df = get_avg_price_per_year(amazon_df, "AMZN")
avg_close_price_per_year_google_df = get_avg_price_per_year(google_df, "GOOG")
avg_close_price_per_year_tesla_df = get_avg_price_per_year(tesla_df, "TSLA")

# DDL to create an empty Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.stock_db.stock_processed_table (
    year INT,
    average_adj_close_price DOUBLE,
    stockcode STRING
)
USING ICEBERG
PARTITIONED BY (stockcode)
""")

# 3. Write to Iceberg table partitioned by StockCode
avg_close_price_per_year_amzn_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .append()

avg_close_price_per_year_google_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .append()

avg_close_price_per_year_tesla_df.writeTo("iceberg_catalog.stock_db.stock_processed_table") \
    .using("iceberg") \
    .append()

end_time = time.time()
elapsed_time = round(end_time - start_time, 2)

logger.info(f"Spark job exec_time={elapsed_time} seconds")