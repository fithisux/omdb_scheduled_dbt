with source_all_movies_cleansed as (

    select

    *
    
    FROM {{ ref('all_movieseries_with_types') }}

)

select * from source_all_movies_cleansed