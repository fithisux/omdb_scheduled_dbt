with source_movie_abstracts_en as (

    select

    *
    
    FROM {{ source('external_source', 'movie_abstracts_en') }}

)

select * from source_movie_abstracts_en