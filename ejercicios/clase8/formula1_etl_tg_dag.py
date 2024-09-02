from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='formula1_etl_tg_dag',
    default_args=args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ingest', 'transform']
) as dag:

    start_process = DummyOperator(task_id='start_process')
    
    with TaskGroup("ingest") as ingest:
        ingest_results = BashOperator(
            task_id='ingest_results',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clases/clase8/ingest_results.sh '
        )
        ingest_drivers = BashOperator(
            task_id='ingest_drivers',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clases/clase8/ingest_drivers.sh '
        )
        ingest_constructors = BashOperator(
            task_id='ingest_constructors',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clases/clase8/ingest_constructors.sh '
        )
        ingest_races = BashOperator(
            task_id='ingest_races',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clases/clase8/ingest_races.sh '
        )
 
    with TaskGroup("transform") as transform:
        transform_drivers = BashOperator(
            task_id='transform_drivers',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clases/clase8/transform_drivers.py '
        )
        transform_constructors = BashOperator(
            task_id='transform_constructors',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clases/clase8/transform_constructors.py '
        )

    end_process = DummyOperator(task_id='end_process')

    start_process >> ingest >> transform >> end_process