with source_all_series_with_types as (

    select

    id::bigint,
    (case when name='\N' then NULL else name end)::varchar as name,
    (case when parent_id='\N' then NULL else parent_id end)::bigint as parent_id,
    (case when date='\N' then NULL else date end)::date as date
    
    FROM {{ ref('all_series') }}

)

select * from source_all_series_with_types