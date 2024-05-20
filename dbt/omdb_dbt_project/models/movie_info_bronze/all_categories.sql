with source_all_categories as (

    select

    *
    
    FROM {{ source('external_source', 'all_categories') }}

)

select * from source_all_categories