from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="car_rental_transform_dag",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["transform"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    transform = BashOperator(
        task_id="transform",
        bash_command="ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio2/transform_car_rental.py ",
    )

    end_process = DummyOperator(task_id="end_process")

    start_process >> transform >> end_process