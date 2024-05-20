with source_image_licenses as (

    select

    *
    
    FROM {{ source('external_source', 'image_licenses') }}

)

select * from source_image_licenses