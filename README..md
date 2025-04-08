#### Run ingest_stock_data_to_iceberg.py
```
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1 .\ingest_stock_data_to_iceberg.py
```