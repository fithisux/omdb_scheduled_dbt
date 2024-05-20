with source_all_episodes as (

    select

    *
    
    FROM {{ source('external_source', 'all_episodes') }}

)

select * from source_all_episodes