with source_movie_categories_with_types as (

    select

    movie_id::bigint,
    category_id::bigint
    
    FROM {{ ref('movie_categories') }}

)

select * from source_movie_categories_with_types