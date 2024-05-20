with source_movie_countries as (

    select

    *
    
    FROM {{ source('external_source', 'movie_countries') }}

)

select * from source_movie_countries