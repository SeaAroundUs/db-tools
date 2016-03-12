-- Run this query against the Web database
with base as (
      select  area_key, year, sum(catch_sum) as total_catch
      from v_fact_data
      --where marine_layer_id in (1,2)
      group by area_key, year),
  stats as (
    select b.area_key, sqrt(variance(b.total_catch))/avg(b.total_catch) as coevar
    from base b
    GROUP BY b.area_key
  ),
  resolve_area_key as (
  select s.*, a.marine_layer_id, a.main_area_id
  from stats s INNER JOIN area a on s.area_key = a.area_key
  )
select naming.name, r.main_area_id as area_id,
       naming.layer_name,
       r.coevar
from resolve_area_key r, web.lookup_entity_name_by_entity_layer(r.marine_layer_id, array[r.main_area_id]) as naming
order by coevar desc;