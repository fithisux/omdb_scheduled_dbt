with source_all_characters as (

    select

    *
    
    FROM {{ source('external_source', 'all_characters') }}

)

select * from source_all_characters