with main_recommend as (
    select
        ir.id as id,
        ir.title,
        ir.overview,
        ir.release_year,
        ir.runtime,
        ir.genres,
        ir.vote_average,
        r.cosine_similarity
    FROM {{ ref('infor_rating') }} ir
    join {{ source('movies', 'recommendations') }} r using (id) -- Join theo id
)

select 
        id,
        title,
         overview,
         release_year,
         runtime,
         genres,
         vote_average

from main_recommend
order by cosine_similarity desc
limit 100
