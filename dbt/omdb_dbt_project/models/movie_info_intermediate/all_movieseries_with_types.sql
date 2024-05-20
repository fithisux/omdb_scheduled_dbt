with source_all_movieseries_with_types as (

    select

    id::bigint,
    (case when name='\N' then NULL else name end)::varchar as name,
    (case when parent_id='\N' then NULL else parent_id end)::bigint as parent_id,
    (case when date='\N' then NULL else date end)::date as date
    
    FROM {{ ref('all_movieseries') }}

)

select * from source_all_movieseries_with_types