with source_all_episodes_cleansed as (

    select

    *
    
    FROM {{ ref('all_episodes_with_types') }}

    where parent_id in (select * from {{ref('all_seasons_series_ids')}})

)

select * from source_all_episodes_cleansed