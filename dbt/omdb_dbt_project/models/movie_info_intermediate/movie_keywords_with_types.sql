with source_movie_keywords_with_types as (

    select

    movie_id::bigint,
    category_id::bigint

    FROM {{ ref('movie_keywords') }}

)

select * from source_movie_keywords_with_types