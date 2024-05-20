with source_all_movie_aliases_iso as (

    select

    *
    
    FROM {{ source('external_source', 'all_movie_aliases_iso') }}

)

select * from source_all_movie_aliases_iso