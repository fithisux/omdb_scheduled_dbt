with source_movie_abstracts_de as (

    select

    *
    
    FROM {{ source('external_source', 'movie_abstracts_de') }}

)

select * from source_movie_abstracts_de