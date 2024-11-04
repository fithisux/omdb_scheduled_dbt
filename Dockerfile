FROM quay.io/astronomer/astro-runtime:12.2.0

# FROM apache/airflow:2.9.1
# USER root
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          libpq-dev \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*
# USER airflow
# RUN python -m pip install astronomer-cosmos

# install dbt into a virtual environment
RUN python -m pip install apache-airflow[virtualenv]
RUN python -m pip install pandas
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-core dbt-postgres && deactivate

# set a connection to the airflow metadata db to use for testing
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
ENV AIRFLOW_CONN_AWS_AA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres
ENV AIRFLOW_VAR_DATASET_URL=http://www.omdb.org/data/
ENV DBT_ROOT_PATH=/usr/local/airflow/dbt
ENV AIRFLOW_VAR_FILE_SAFE=/usr/local/airflow/dbt/omdbfiles
