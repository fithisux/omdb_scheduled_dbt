with source_category_names_with_types as (

    select

    image_id::bigint,
    (case when object_id='\N' then NULL else object_id end)::bigint as object_id,
    (case when object_type='\N' then NULL else object_type end)
    
    FROM {{ ref('image_ids') }}
)

select * from source_category_names_with_types