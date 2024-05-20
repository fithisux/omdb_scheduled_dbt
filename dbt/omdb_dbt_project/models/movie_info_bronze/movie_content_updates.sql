with source_movie_content_updates as (

    select

    *
    
    FROM {{ source('external_source', 'movie_content_updates') }}

)

select * from source_movie_content_updates