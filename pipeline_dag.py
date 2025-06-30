from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('big_data_pipeline_sim', start_date=datetime(2025,6,30), schedule_interval='@daily', catchup=False) as dag:
    process_data = BashOperator(
        task_id='run_spark_job',
        bash_command='spark-submit /path/to/process_data.py'
    )
