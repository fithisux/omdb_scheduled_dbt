with source_all_movies_cleansed as (

    select

    *
    
    FROM {{ ref('all_movies_with_types') }}

    where parent_id in (select id FROM {{ref('all_movieseries_with_types')}})

)

select * from source_all_movies_cleansed