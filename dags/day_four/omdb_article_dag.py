from datetime import datetime

from cosmos import DbtDag, ProjectConfig, RenderConfig
from airflow import DAG
from day_four.include.profiles import airflow_db
from day_four.include.constants import imdb_project_path, venv_execution_config

dag: DAG = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(imdb_project_path),
    profile_config=airflow_db,
    execution_config=venv_execution_config,
    render_config =  RenderConfig(emit_datasets=False),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="omdb_dataset_dag",
    tags=["omdb"],
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
)
