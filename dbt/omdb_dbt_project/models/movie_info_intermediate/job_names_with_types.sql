with source_job_names_with_types as (

    select

    job_id::bigint,
    (case when name='\N' then NULL else name end)::varchar as name,
    (case when language_iso_639_1='\N' then NULL else language_iso_639_1 end)::varchar as language_iso_639_1
    
    FROM {{ ref('job_names') }}

)

select * from source_job_names_with_types