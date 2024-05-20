with source_all_characters_cleansed as (

    select

    *
    
    FROM {{ ref('all_characters_with_types') }}

)

select * from source_all_characters_cleansed