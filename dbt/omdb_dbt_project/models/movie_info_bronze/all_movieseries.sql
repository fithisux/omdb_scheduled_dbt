with source_all_movieseries as (

    select

    *
    
    FROM {{ source('external_source', 'all_movieseries') }}

)

select * from source_all_movieseries