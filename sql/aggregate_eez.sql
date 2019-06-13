select 'INSERT INTO web.v_fact_data_test(
          main_area_id,sub_area_id,marine_layer_id, data_layer_id, fishing_entity_id,gear_id,time_key,year,taxon_key,area_key,
          catch_type_id,catch_status,reporting_status_id,reporting_status,sector_type_id,end_use_type_id,score,catch_sum,real_value,primary_production_required,
          catch_trophic_level,catch_max_length
        )

with 
catch(main_area_id, sub_area_id, year, taxon_key, data_layer_id, fishing_entity_id, gear_id, catch_type_id, reporting_status_id, sector_type_id, end_use_type_id, total_catch, eez_id) as (
select
  ar.eez_id,
  ar.fao_area_id,
  ad.year,
  ad.taxon_key,
  ad.data_layer_id,
  ad.fishing_entity_id,
  ad.gear_type_id,
  ad.catch_type_id,
  ad.reporting_status_id,
  ad.sector_type_id,
  case
    when ad.catch_type_id = 2 then 4
    else coalesce (e.end_use_type_id ,
    1)
  end as end_use_type_id,
  case
    when ad.catch_type_id = 2 then (ar.total_catch)
    else ar.total_catch * (coalesce (e.end_use_percentage,
    null,
    1))
  end as catch,
  ar.eez_id
from
  allocation.allocation_result_eez ar
join allocation.allocation_data ad on
  (ad.universal_data_id = ar.universal_data_id)
left join web.end_use e on
  (e.fishing_entity_id = ad.fishing_entity_id)
  and e.year = ad.year
  and e.taxon_key = ad.taxon_key
  and e.sector_type_id = ad.sector_type_id
  and e.reporting_status_id = ad.reporting_status_id
  and e.gear_type_id = ad.gear_type_id
  and e.catch_type_id = ad.catch_type_id
where 
  ar.eez_id = ' || t.eez_id || 
        '
),


uncert(eez_id, sector_type_id, period_id, score, layer, period2, year_range) as ( 
select *
from 
  web.uncertainty_eez ue
left join 
  web.uncertainty_time_period utp on (ue.period_id = utp.period_id)
)

select
  ' || t.eez_id || ',
  c.sub_area_id, 
  1 as marine_layer_id, 
  c.data_layer_id, 
  c.fishing_entity_id, 
  c.gear_id, 
  tm.time_key, 
  c.year, 
  c.taxon_key, 
  a.area_key, 
  c.catch_type_id,
  max(ct.abbreviation) catch_type,
  c.reporting_status_id,
  max(rs.abbreviation) reporting_status,
  c.sector_type_id,
  c.end_use_type_id,
  coalesce(u.score,
    null,
    1) score, 
  sum(c.total_catch) total_catch,
  sum(coalesce (p.price,
    null,
    1466) * c.total_catch) real_value,
  web.ppr(sum(c.total_catch),
  max(t.tl)) ppr,
  sum(c.total_catch * t.tl) tl,
  sum(c.total_catch * t.sl_max) sl
from
  catch c
join web.cube_dim_taxon t on (t.taxon_key = c.taxon_key)
join web.area a on (a.marine_layer_id = 1
  and a.main_area_id = ' || t.eez_id || ' and a.sub_area_id = c.sub_area_id)
join web.catch_type ct on (ct.catch_type_id = c.catch_type_id)
join web.reporting_status rs on (rs.reporting_status_id = c.reporting_status_id)
join web.time tm on (tm.time_business_key = c.year)
left join allocation.price p on
  c.fishing_entity_id = p.fishing_entity_id
  and c.year = p.year
  and c.taxon_key = p.taxon_key
  and c.end_use_type_id = p.end_use_type_id
left join uncert u on u.eez_id = c.eez_id
  and u.sector_type_id = c.sector_type_id
  and u.layer = c.data_layer_id
  and  c.year <@ u.year_range
group by
  c.main_area_id,
  c.sub_area_id,
  marine_layer_id,
  c.data_layer_id,
  c.fishing_entity_id,
  c.gear_id,
  tm.time_key,
  c.year,
  c.taxon_key,
  a.area_key,
  c.sector_type_id,
  c.catch_type_id,
  c.reporting_status_id,
  c.end_use_type_id,
  u.score'
 from (select distinct ar.eez_id from allocation.allocation_result_eez ar) as t
  order by t.eez_id;
