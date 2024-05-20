with source_movie_details_with_types as (

    select

    movie_id::bigint,
    runtime::bigint,
    (case when budget='\N' then NULL else budget end)::bigint as budget,
    (case when revenue='\N' then NULL else revenue end)::bigint as revenue,
    (case when homepage='\N' then NULL else homepage end)::varchar as homepage
    
    FROM {{ ref('movie_details') }}

)

select * from source_movie_details_with_types