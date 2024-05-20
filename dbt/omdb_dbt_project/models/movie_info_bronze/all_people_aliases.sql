with source_all_people_aliases as (

    select

    *
    
    FROM {{ source('external_source', 'all_people_aliases') }}

)

select * from source_all_people_aliases