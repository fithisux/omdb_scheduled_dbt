"Contains profile mappings used in the project"

from cosmos import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="omdbprofile",
    target_name="dev",
    #profiles_yml_filepath='/usr/local/airflow/dbt/omdb_manual_dataset/omdb_dbt_project/profiles.yml'
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="aws_aa_db",
        profile_args={'schema': 'dbt'}
    ),
)
