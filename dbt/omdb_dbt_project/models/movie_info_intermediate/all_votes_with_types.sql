with source_all_votes_with_types as (

    select

    movie_id::bigint,
    vote_average::float,
    votes_count::bigint

    FROM {{ ref('all_votes') }}

)

select * from source_all_votes_with_types