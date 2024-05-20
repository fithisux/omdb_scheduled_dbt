with source_movie_abstracts_fr as (

    select

    *
    
    FROM {{ source('external_source', 'movie_abstracts_fr') }}

)

select * from source_movie_abstracts_fr