from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 1. Create Spark session with Iceberg catalog
spark = SparkSession.builder \
    .appName("WriteToIceberg") \
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://navod-iceberg-warehouse/") \
    .getOrCreate()

schema = StructType([
   StructField("date", StringType(), True),
   StructField("open", FloatType(), True),
   StructField("high", FloatType(), True),
   StructField("low", FloatType(), True),
   StructField("close", FloatType(), True),
   StructField("adjclose", FloatType(), True),
   StructField("volume", IntegerType(), True),
   StructField("stockcode", StringType(), True),])

df = spark.read.parquet("merged_stock_data.parquet", header=True, schema=schema)

# DDL to create an empty Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.stock_db.stock_table (
    date STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adjclose DOUBLE,
    volume INT,
    stockcode STRING
)
USING ICEBERG
PARTITIONED BY (stockcode)
""")

# 3. Write to Iceberg table partitioned by StockCode
df.writeTo("iceberg_catalog.stock_db.stock_table") \
    .using("iceberg") \
    .append()

print("Data written to Iceberg partitioned by stockcode.")
