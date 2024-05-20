with source_all_characters_with_types as (

    select

    (case when name='\N' then NULL else name end)::varchar as name,
    id::bigint
    
    FROM {{ ref('all_characters') }}

)

select * from source_all_characters_with_types