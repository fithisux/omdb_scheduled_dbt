with source_all_casts_cleansed as (

    select

    distinct *
    
    FROM {{ref('all_casts_with_types')}}

    where movie_id in (select id FROM {{ref('all_movies_with_types')}})
)

select * from source_all_casts_cleansed