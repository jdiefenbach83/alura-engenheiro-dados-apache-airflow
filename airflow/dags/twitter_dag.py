from datetime import datetime
from os.path import join
from pathlib import Path

from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

args={
    'owner': 'Jefferson',
    'depends_on_past': False,
    'start_date': days_ago(6)
}

base_folder = join(
    Path('~').expanduser(),
    'datapipeline/datalake/{stage}/twitter_aluraonline/{partition}'
)
partition_folder = 'extract_date={{ ds }}'
timestamp_format = "%Y-%m-%dT%H:%M:%S.000Z"

with DAG(
    dag_id='twitter_dag', 
    default_args=args,
    schedule_interval='0 9 * * *',
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id='twitter_aluraonline',
        query='AluraOnline',
        file_path=join(
            base_folder.format(stage='bronze', partition=partition_folder),
            'AluraOnline_{{ ds_nodash }}.json'
        ),
        start_time=(
            "{{"f"execution_date.strftime('{timestamp_format}')""}}"
        ),
        end_time=(
            "{{"f"next_execution_date.strftime('{timestamp_format}')""}}"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id='transform_twitter_aluraonline',
        application = join(
            (Path(__file__).parents[2]),
            'spark/transformation.py'
        ),
        name='twitter_transformation',
        application_args=[
            '--src',            
            base_folder.format(stage='bronze', partition=partition_folder),
            '--dst',
            base_folder.format(stage='silver', partition=''),            
            '--process-date',
            '{{ ds }}'
        ]
    )

    twitter_operator # >> twitter_transform
