with source_all_casts_with_types as (

    select

    movie_id::bigint,
    person_id::bigint,
    job_id::bigint,
    case when role='\N' then NULL else role end as role,
    "position"::bigint
    
    FROM {{ref('all_casts')}}
)

select * from source_all_casts_with_types