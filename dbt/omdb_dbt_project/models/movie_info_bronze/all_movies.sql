with source_all_movies as (

    select

    *
    
    FROM {{ source('external_source', 'all_movies') }}

)

select * from source_all_movies