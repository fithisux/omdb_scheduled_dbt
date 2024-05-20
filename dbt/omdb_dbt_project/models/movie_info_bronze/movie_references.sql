with source_movie_references as (

    select

    *
    
    FROM {{ source('external_source', 'movie_references') }}

)

select * from source_movie_references