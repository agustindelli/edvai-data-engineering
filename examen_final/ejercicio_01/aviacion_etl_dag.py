from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aviacion_etl_dag",
    default_args=args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "transform"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    ingest = BashOperator(
        task_id="ingest",
        bash_command="/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio1/ingest.sh ",
    )

    with TaskGroup("transform") as transform:

        transform_1 = BashOperator(
            task_id="transform_1",
            bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio1/transform_1.py ",
        )

        transform_2 = BashOperator(
            task_id="transform_2",
            bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio1/transform_2.py ",
        )

    end_process = DummyOperator(task_id="end_process")

    start_process >> ingest >> transform >> end_process
