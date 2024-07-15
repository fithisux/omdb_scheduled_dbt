from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag
from day_four.operators.fs_ingestion_operator import FileIngestionOperator, FinishIngestionOperator, StartIngestionOperator, TimeOriginOperator
from day_four.include.file_list import FILE_LIST


dag = DAG(
    "omdb_dataset_fetcher",
    default_args={"retries": 1},
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=["omdb"],
)

time_origin = TimeOriginOperator(task_id="time_origin", dag=dag)
start_this = StartIngestionOperator(task_id="start_ingestion", dag=dag)
finish_this = FinishIngestionOperator(task_id="finish_ingestion", dag=dag, outlets=[Dataset("s3://dataset-bucket/example1.csv")])
file_ingestion_tasks = [FileIngestionOperator(task_id=f"task_{data_file}", file_name=data_file, dag=dag) for data_file in FILE_LIST]

time_origin >> start_this

prev_task = start_this

for file_ingestion_task in file_ingestion_tasks:
    prev_task.set_downstream(file_ingestion_task)
    prev_task = file_ingestion_task

prev_task.set_downstream(finish_this)