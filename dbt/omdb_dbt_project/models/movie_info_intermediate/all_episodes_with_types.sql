with source_all_episodes_with_types as (

    select

    (case when name='\N' then NULL else name end)::varchar as name,
    (case when parent_id='\N' then NULL else parent_id end)::bigint as parent_id,
    (case when series_id='\N' then NULL else series_id end)::bigint as series_id,
    (case when date='\N' then NULL else date end)::date as date
    
    FROM {{ ref('all_episodes') }}

)

select * from source_all_episodes_with_types