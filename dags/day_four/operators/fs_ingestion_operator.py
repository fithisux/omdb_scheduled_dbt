import csv
import os
import shutil
from airflow.models import BaseOperator, Variable
from airflow.models.taskinstance import Context
import pendulum
import requests
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection, Variable
from airflow.models.taskinstance import Context
from typing import List
from sqlalchemy import create_engine
import sqlalchemy
import logging
import io
import bz2
from airflow.operators.python import PythonVirtualenvOperator

logger = logging.getLogger(__name__)


def cleanup_date(data_interval_start: pendulum.DateTime):
    file_safe: str = str(Variable.get("file_safe"))

    file_path = f"{file_safe}/{data_interval_start.to_date_string()}"

    if os.path.isdir(file_path):
        shutil.rmtree(file_path)

    os.mkdir(file_path)

def put_marker(data_interval_start: pendulum.DateTime, marker_name: str):
    file_safe: str = str(Variable.get("file_safe"))

    file_path = f"{file_safe}/{data_interval_start.to_date_string()}/{marker_name}.txt"

    with open(file_path, "w", newline="") as file:
        file.write(marker_name)


def ingest(data_interval_start: pendulum.DateTime, file_name: str, schema_name: str):
    file_safe: str = str(Variable.get("file_safe"))

    file_path = f"{file_safe}/{data_interval_start.to_date_string()}/{file_name}"
    
    with bz2.open(file_path, "rb") as file:
        content = file.read()

    text_file_content = content.decode('utf-8')
    text_file_content = text_file_content.replace('\\\"', "\\\'")

    multi_line_csv = io.StringIO(text_file_content)

    df = pd.read_csv(multi_line_csv, quotechar='"', quoting=csv.QUOTE_ALL, index_col=False, delimiter=',')

    connections: List[Connection] = BaseHook.get_connections("aws_aa_db")

    as_uri = connections[0].get_uri()
    as_uri = as_uri.replace("postgres://", "postgresql+psycopg2://")
    logger.info(f"DB URI IS {as_uri}")
    
    engine = create_engine(as_uri)

    table_name = file_name.split('.')[0]

    df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)

    del df


def mark_version(data_interval_start: pendulum.DateTime, schema_name: str):
    df = pd.DataFrame(data={'the_version': [data_interval_start.to_date_string()]})

    connections: List[Connection] = BaseHook.get_connections("aws_aa_db")

    as_uri = connections[0].get_uri()
    as_uri = as_uri.replace("postgres://", "postgresql+psycopg2://")
    logger.info(f"DB URI IS {as_uri}")
    
    engine = create_engine(as_uri)

    df.to_sql("dataset_version", engine, schema=schema_name, if_exists='replace')

    del df


class TimeOriginOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['data_interval_start']
        context['task_instance'].xcom_push(key="run_date", value=data_interval_start)

class StartIngestionOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['task_instance'].xcom_pull(key="run_date", task_ids="time_origin")
        cleanup_date(data_interval_start)
        put_marker(data_interval_start, "start")


class FinishIngestionOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['task_instance'].xcom_pull(key="run_date", task_ids="time_origin")
        put_marker(data_interval_start, "finish")


class FileIngestionOperator(BaseOperator):
    def __init__(self, file_name: str, *args, **kwargs):
        self.file_name = file_name
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['task_instance'].xcom_pull(key="run_date", task_ids="time_origin")
        dataset_url: str = str(Variable.get("dataset_url"))

        file_safe: str = str(Variable.get("file_safe"))

        file_path = f"{file_safe}/{data_interval_start.to_date_string()}/{self.file_name}"

        # https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
        # with requests.get(dataset_url+self.file_name, stream=True) as r:
        #     r.raise_for_status()
        #     with open(file_path, 'wb') as f:
        #         for chunk in r.iter_content(chunk_size=8192): 
        #             # If you have chunk encoded response uncomment if
        #             # and set chunk_size parameter to None.
        #             #if chunk: 
        #             f.write(chunk)


        with requests.get(dataset_url+self.file_name) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(r.content)



class ToDBIngestionOperator(BaseOperator):
    def __init__(self, file_name: str, schema_name: str, *args, **kwargs):
        self.file_name = file_name
        self.schema_name = schema_name
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['task_instance'].xcom_pull(key="run_date", task_ids="time_origin")
        ingest(data_interval_start, self.file_name, self.schema_name)


class MarkVersionOperator(BaseOperator):
    def __init__(self, schema_name: str, *args, **kwargs):
        self.schema_name = schema_name
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        data_interval_start: pendulum.DateTime = context['task_instance'].xcom_pull(key="run_date", task_ids="time_origin")
        mark_version(data_interval_start, self.schema_name)


# https://stackoverflow.com/questions/50927740/sqlalchemy-create-schema-if-not-exists
class CreateSchemaOperator(BaseOperator):
    def __init__(self, schema_name: str, *args, **kwargs):
        self.schema_name = schema_name
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        connections: List[Connection] = BaseHook.get_connections("aws_aa_db")

        as_uri = connections[0].get_uri()
        as_uri = as_uri.replace("postgres://", "postgresql+psycopg2://")
        logger.info(f"DB URI IS {as_uri}")
        engine = create_engine(as_uri)

        if engine.dialect.has_schema(engine, self.schema_name):
            engine.execute(f"DROP SCHEMA {self.schema_name} CASCADE;")
            
        engine.execute(sqlalchemy.schema.CreateSchema(self.schema_name))


def create_schema_callable(schema_name: str, db_airflow_connection: str):
    import sqlalchemy

    engine = sqlalchemy.create_engine(db_airflow_connection)

    inspector = sqlalchemy.inspect(engine)
    exists_schema = False
    if schema_name in inspector.get_schema_names():
        print(f"{schema_name} schema exists")
        exists_schema = True        

    with engine.connect() as connection:
        if exists_schema:
            _ = connection.execute(sqlalchemy.schema.DropSchema(schema_name, cascade=True))
        connection.execute(sqlalchemy.schema.CreateSchema(schema_name))
        connection.commit()


def create_schema_facade(dag, schema_name):
    connections: List[Connection] = BaseHook.get_connections("aws_aa_db")
    as_uri = connections[0].get_uri().replace("postgres://", "postgresql+psycopg2://")
    logger.info(f"DB URI IS {as_uri}")
    return PythonVirtualenvOperator(
        task_id="make_schema",
        python_callable=create_schema_callable,
        requirements=["SQLAlchemy==2.0.30", "psycopg2-binary==2.9.9"],
        op_kwargs={"schema_name": schema_name, "db_airflow_connection": as_uri}, 
        dag=dag,
        system_site_packages=False,
    )