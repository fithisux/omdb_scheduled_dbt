with source_movie_links as (

    select

    *
    
    FROM {{ source('external_source', 'movie_links') }}

)

select * from source_movie_links