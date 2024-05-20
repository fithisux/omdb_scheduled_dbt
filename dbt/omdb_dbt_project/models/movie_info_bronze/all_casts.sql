with source_all_casts as (

    select

    *
    
    FROM {{ source('external_source', 'all_casts') }}

)

select * from source_all_casts