from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.decorators import task_group

from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

from day_four.operators.fs_ingestion_operator import FileIngestionOperator, FinishIngestionOperator, MarkVersionOperator, StartIngestionOperator, TimeOriginOperator



dag = DAG(
    "imdb_dataset_fetcher",
    default_args={"retries": 1},
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@daily",
    tags=["imdb"],
)

def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print("KWARGS AS CONTEXT")
    pprint(kwargs)
    print("DS IS")
    print(ds)
    return "Whatever you return gets printed in the logs"


FILE_LIST = [
'all_casts.csv.bz2',                  
'all_categories.csv.bz2',             
'all_characters.csv.bz2',             
'all_episodes.csv.bz2',               
'all_movies.csv.bz2',                 
'all_movieseries.csv.bz2',            
'all_movie_aliases.csv.bz2',          
'all_movie_aliases_iso.csv.bz2',      
'all_people.csv.bz2',                 
'all_people_aliases.csv.bz2',         
'all_seasons.csv.bz2',                
'all_series.csv.bz2',                 
'all_votes.csv.bz2',                  
'category_names.csv.bz2',             
'image_ids.csv.bz2',                  
'image_licenses.csv.bz2',             
'job_names.csv.bz2',                  
'movie_abstracts_de.csv.bz2',         
'movie_abstracts_en.csv.bz2',         
'movie_abstracts_es.csv.bz2',         
'movie_abstracts_fr.csv.bz2',         
'movie_categories.csv.bz2',           
'movie_content_updates.csv.bz2',      
'movie_countries.csv.bz2',            
'movie_details.csv.bz2',              
'movie_keywords.csv.bz2',             
'movie_languages.csv.bz2',            
'movie_links.csv.bz2',                
'movie_references.csv.bz2',           
'people_links.csv.bz2',               
'trailers.csv.bz2'     
]



run_this = PythonOperator(task_id="print_the_context", python_callable=print_context, dag=dag)
time_origin = TimeOriginOperator(task_id="time_origin", dag=dag)
start_this = StartIngestionOperator(task_id="start_ingestion", dag=dag)
finish_this = FinishIngestionOperator(task_id="finish_ingestion", dag=dag)
file_ingestion_tasks = [FileIngestionOperator(task_id=f"task_{data_file}", file_name=data_file, dag=dag) for data_file in FILE_LIST]

run_this >> time_origin >> start_this

prev_task = start_this

for file_ingestion_task in file_ingestion_tasks:
    prev_task.set_downstream(file_ingestion_task)
    prev_task = file_ingestion_task

prev_task.set_downstream(finish_this)