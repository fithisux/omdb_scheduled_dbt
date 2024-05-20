with source_all_people as (

    select

    *
    
    FROM {{ source('external_source', 'all_people') }}

)

select * from source_all_people