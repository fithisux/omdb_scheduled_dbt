with source_all_seasons_series_ids as (

    select id from {{ ref('all_series_with_types') }}
    UNION
    select id from {{ ref('all_seasons_with_types') }}

)

select * from source_all_seasons_series_ids