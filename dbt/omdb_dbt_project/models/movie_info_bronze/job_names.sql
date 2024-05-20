with source_job_names as (

    select

    *
    
    FROM {{ source('external_source', 'job_names') }}

)

select * from source_job_names