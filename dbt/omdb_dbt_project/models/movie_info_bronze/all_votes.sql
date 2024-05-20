with source_all_votes as (

    select

    *
    
    FROM {{ source('external_source', 'all_votes') }}

)

select * from source_all_votes