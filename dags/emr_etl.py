from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator


default_args = {
    'owner': 'navod',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

with DAG(
    'spark_job_dag',
    default_args=default_args,
    description='DAG to run ETL jobs on EMR',
    catchup=False,
) as dag:

    #Create an EC2 Key Pair: If you haven't already created an EC2 key pair, 

    JOB_FLOW_OVERRIDES={
        'Name': 'Spark Cluster',
        'LogUri': 's3://emr8-bucket/logs/',
        'ReleaseLabel': 'emr-7.0.0',
        "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Slave',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'emr-ec2-keypair',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    }


    #"EMR_EC2_DefaultRole","EMR_DefaultRole"
    
    create_job_flow_task = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default'
    )


    # Define PySpark step
    spark_step1 = {
        'Name': 'Process Stock Data',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '--conf', 'spark.executor.memory=8g',
                    '--conf', 'spark.executor.cores=4',
                    '--packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1',
                    's3://emr8-bucket/jobs/process_stock_data.py']
        }
    }


    submit_job1_task = EmrAddStepsOperator(
        task_id='submit_job1',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[spark_step1]
    )

    #This task is a sensor that waits for a specific step in the EMR cluster to complete. 
    sensor1_task = EmrStepSensor(
        task_id='watch_step_1',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='submit_job1', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )


    # Optionally add cluster termination:
    terminate_cluster_task = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    create_job_flow_task >> submit_job1_task >> sensor1_task >> terminate_cluster_task

