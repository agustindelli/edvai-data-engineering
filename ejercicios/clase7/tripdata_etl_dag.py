from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='tripdata_etl_dag',
    default_args=args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=True,
    tags=['ingest', 'transform']
) as dag:

    start_process = DummyOperator(task_id='start_process')

    ingest = BashOperator(
        task_id='ingest',
        bash_command='/usr/bin/sh /home/hadoop/scripts/clases/clase7/ingest.sh '
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clases/clase7/transform_tripdata.py '
    )

    end_process = DummyOperator(task_id='end_process')

    start_process >> ingest >> transform >> end_process