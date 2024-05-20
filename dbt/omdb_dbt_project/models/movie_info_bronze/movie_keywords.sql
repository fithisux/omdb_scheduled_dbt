with source_movie_keywords as (

    select

    *
    
    FROM {{ source('external_source', 'movie_keywords') }}

)

select * from source_movie_keywords