from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from day_four.operators.fs_ingestion_operator import MarkVersionOperator, TimeOriginOperator, ToDBIngestionOperator, create_schema_facade
from day_four.include.file_list import FILE_LIST
from airflow.datasets import Dataset

dag = DAG(
    "omdb_db_loader",
    default_args={"retries": 1},
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["omdb"],
    schedule=[Dataset("s3://dataset-bucket/example1.csv")],
)

time_origin = TimeOriginOperator(task_id="time_origin", dag=dag)
make_schema = create_schema_facade(dag, schema_name="omdbs")
file_ingestion_tasks = [ToDBIngestionOperator(task_id=f"task_{data_file}", file_name=data_file, schema_name="omdbs", dag=dag) for data_file in FILE_LIST]
version_this = MarkVersionOperator(task_id="version_ingestion", schema_name="omdbs", dag=dag, outlets=[Dataset("s3://dataset-bucket/example2.csv")])

time_origin >> make_schema >> file_ingestion_tasks >> version_this