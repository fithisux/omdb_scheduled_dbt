with source_all_categories_cleansed as (

    select

   *
    
    FROM {{ref('all_categories_with_types')}}
)

select * from source_all_categories_cleansed