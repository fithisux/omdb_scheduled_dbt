with source_all_people_aliases_with_types as (

    select

    person_id::bigint,
    (case when name='\N' then NULL else name end)::varchar as name
    
    FROM {{ ref('all_people_aliases') }}

)

select * from source_all_people_aliases_with_types