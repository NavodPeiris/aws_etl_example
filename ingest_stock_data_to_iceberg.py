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
   StructField("Date", StringType(), True),
   StructField("Open", FloatType(), True),
   StructField("High", FloatType(), True),
   StructField("Low", FloatType(), True),
   StructField("Close", FloatType(), True),
   StructField("AdjClose", FloatType(), True),
   StructField("Volume", IntegerType(), True),
   StructField("StockCode", StringType(), True),])

df = spark.read.parquet("merged_stock_data.parquet", header=True, schema=schema)

# 3. Write to Iceberg table partitioned by StockCode
df.writeTo("iceberg_catalog.stock_db.stock_table") \
    .using("iceberg") \
    .partitionedBy("StockCode") \
    .createOrReplace()

print("Data written to Iceberg partitioned by StockCode.")
