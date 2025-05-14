
with main_info as(
    select 
    mi.id as id,
    mi.title,
    mi.overview,
    mi.release_date,
    mi.runtime,
    mi.genres,
    rt.vote_average,
    rt.vote_count

    from {{source('movies','movies_infor')}} mi 
    join {{source('movies','movies_rating')}} rt ON mi.id = rt.id

) 

select
        id,
        title,
         overview,
         extract(year from release_date) as release_year,
         runtime,
         genres,
         vote_average

from main_info
