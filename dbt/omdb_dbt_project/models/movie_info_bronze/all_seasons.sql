with source_all_seasons as (

    select

    *
    
    FROM {{ source('external_source', 'all_seasons') }}

)

select * from source_all_seasons