with source_all_series as (

    select

    *
    
    FROM {{ source('external_source', 'all_series') }}

)

select * from source_all_series