with source_people_links as (

    select

    *
    
    FROM {{ source('external_source', 'people_links') }}

)

select * from source_people_links