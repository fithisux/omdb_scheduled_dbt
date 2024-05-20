with source_category_names as (

    select

    *
    
    FROM {{ source('external_source', 'category_names') }}

)

select * from source_category_names