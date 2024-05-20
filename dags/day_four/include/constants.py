"Contains constants used in the DAGs"

from pathlib import Path
from cosmos import ExecutionConfig
from cosmos.constants import InvocationMode

imdb_project_path = Path("/usr/local/airflow/dbt/omdb_dbt_project")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
    #invocation_mode=InvocationMode.DBT_RUNNER,
)
