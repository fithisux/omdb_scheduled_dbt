with source_movie_categories as (

    select

    *
    
    FROM {{ source('external_source', 'movie_categories') }}

)

select * from source_movie_categories