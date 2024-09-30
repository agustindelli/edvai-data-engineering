from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="car_rental_ingest_dag",
    default_args=args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "transform_trigger"],
) as dag:

    start_process = DummyOperator(task_id="start_process")
    
    ingest = BashOperator(
        task_id="ingest",
        bash_command="/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio2/ingest.sh ",
    )

    transform_trigger = TriggerDagRunOperator(
        task_id="transform_trigger",
        trigger_dag_id="car_rental_transform_dag",
    )
    
    end_process = DummyOperator(task_id="end_process")

    start_process >> ingest >> transform_trigger >> end_process