with source_movie_details as (

    select

    *
    
    FROM {{ source('external_source', 'movie_details') }}

)

select * from source_movie_details