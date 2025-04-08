#### setup mwaa
- put 'emr_etl.py' file in dags folder in aws mwaa environment
- create aws role with policy 'mwaa_custom_policy.json'

#### setup glue database
- create stock_db database in warehouse location s3://your-ware-house-name

#### setup emr folder
- create emr folder
- create logs/, jobs/ folders inside it
- put 'process_stock_data.py' spark job file in jobs folder

#### configure aws cli
- install aws-cli
- create IAM role with AdminAccess privileges
- run 'aws configure'
- provide access key and secret access key

#### ingest stock data
Run ingest_stock_data_to_iceberg.py
```
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1 .\ingest_stock_data_to_iceberg.py
```