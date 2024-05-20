with source_image_licenses_with_types as (

    select

    image_id::bigint,
    license_id::bigint,
    (case when source='\N' then NULL else source end)::varchar as source,
    (case when author='\N' then NULL else author end)::varchar as author
    
    FROM {{ ref('image_licenses') }}
)

select * from source_image_licenses_with_types