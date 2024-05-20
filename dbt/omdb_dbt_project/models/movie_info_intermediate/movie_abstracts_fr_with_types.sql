with source_movie_abstracts_fr_with_types as (

    select

    movie_id::bigint,
    (case when abstract='\N' then NULL else abstract end)::varchar as abstract
    
    FROM {{ ref('movie_abstracts_fr') }}

)

select * from source_movie_abstracts_fr_with_types