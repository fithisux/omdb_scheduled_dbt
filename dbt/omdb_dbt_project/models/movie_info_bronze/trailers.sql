with source_trailers as (

    select

    *
    
    FROM {{ source('external_source', 'trailers') }}

)

select * from source_trailers