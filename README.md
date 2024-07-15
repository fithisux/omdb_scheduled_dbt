# SCHEDULE OMDB FETCH

There is [article](https://fithis2001.medium.com/ingesting-the-omdb-dataset-fcd9ca53a36a).

## Getting started
which has submodules, so you need additionally

```
git submodule init
git submodule update
```

Just clone the repo and by use of the [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)
you can run the project. In my Windows machine I just use:

```
c:\tools\astro_1.27.1_windows_amd64.exe dev start
```

There are three dags that normally are meant to run one after the other.

The dataset fetcher DAG, aka **omdb_dataset_fetcher** that fetches the dataset to the local [omdbfiles](dbt/omdbfiles).
The dataset ingestion DAG, aka **dataset_to_db**  ingests the dataset to the postgres started by astronomer 
(see [here](dbt/imdb_dataset_article/profiles.yml) for connecting your dbeaver).
The dbt modeller dag, aka **omdb_dataset_dag**  models the dataset and runs some model tests.

Navigate to [Local Airflow UI](http:/localhost:8080) to run executions (username/password == admin/admin).


## Authors and acknowledgment
Code contributed by vasilis.anagnostopoulos@agileactors.com