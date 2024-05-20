with source_movie_abstracts_es as (

    select

    *
    
    FROM {{ source('external_source', 'movie_abstracts_es') }}

)

select * from source_movie_abstracts_es