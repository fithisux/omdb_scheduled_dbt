"""
An example DAG that uses Cosmos to render a dbt project.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from day_four.include.constants import venv_execution_config

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)

profile_config = ProfileConfig(
    profile_name="omdbprofile",
    target_name="dev",
    #profiles_yml_filepath='/usr/local/airflow/dbt/omdb_dbt_project/profiles.yml'
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="aws_aa_db",
        profile_args={'schema': 'dbt'}
    ),
)

# [START local_example]
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "omdb_dbt_project",
    ),
    profile_config=profile_config,
    execution_config=venv_execution_config,
    render_config = RenderConfig(emit_datasets=False),
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="omdb_dataset_dag",
    tags=["omdb"],
    default_args={"retries": 2},
)
# [END local_example]