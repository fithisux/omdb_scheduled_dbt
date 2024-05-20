with source_movie_content_updates_with_types as (

    select

    movie_id::bigint,
    (case when last_update='\N' then NULL else last_update end)::timestamp as last_update
    
    FROM {{ ref('movie_content_updates') }}

)

select * from source_movie_content_updates_with_types