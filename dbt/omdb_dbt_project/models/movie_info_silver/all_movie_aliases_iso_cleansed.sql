with source_all_movie_aliases_iso_cleansed as (

    select

    *
    
    FROM {{ ref('all_movie_aliases_iso_with_types') }}

    WHERE  movie_id in (select id from {{ref('all_movies_with_types')}})

)

select * from source_all_movie_aliases_iso_cleansed