with source_image_ids as (

    select

    *
    
    FROM {{ source('external_source', 'image_ids') }}

)

select * from source_image_ids