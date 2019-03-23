with base(universal_data_id,
area_id,
fao_area_id,
total_catch,
eez_id) as (
select
	r.universal_data_id,
	cr.area_id,
	asa.fao_area_id,
	sum(r.allocated_catch*cr.water_area / c.water_area) as totalcatch,
	asa.inherited_att_belongs_to_reconstruction_eez_id
from
	allocation.simple_area_cell_assignment_raw as cr
join allocation.cell as c on
	(c.cell_id = cr.cell_id)
join allocation.allocation_result as r on
	(r.cell_id = cr.cell_id)
join allocation.allocation_simple_area asa on
	asa.allocation_simple_area_id = r.allocation_simple_area_id
where
	cr.marine_layer_id = 19
	and cr.area_id = 103
group by
	r.universal_data_id,
	cr.area_id,
	asa.fao_area_id,
	asa.inherited_att_belongs_to_reconstruction_eez_id ),
allocation_result_meo as (
select
	universal_data_id,
	area_id as meow_id,
	sum(total_catch) total_catch,
	b.eez_id
from
	base b
join fao_meow f on
	b.area_id = f.ecoregion_id
	and b.fao_area_id = f.fao_area_id
group by
	universal_data_id,
	area_id,
	eez_id ),
	
catch(main_area_id,
year,
taxon_key,
data_layer_id,
fishing_entity_id,
gear_id,
catch_type_id,
reporting_status_id,
sector_type_id,
end_use_type_id,
total_catch,
eez_id) as (
select
	ar.meow_id,
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
	--change ad.unit_price with the price table with end use
	-- included eez column for uncertainty score
 ar.eez_id
from
	allocation_result_meo ar
join allocation.allocation_data ad on
	(ad.universal_data_id = ar.universal_data_id)
left join web.end_use e on
	e.fishing_entity_id = ad.fishing_entity_id
	and e.year = ad.year
	and e.taxon_key = ad.taxon_key
	and e.sector_type_id = ad.sector_type_id
	and e.reporting_status_id = ad.reporting_status_id
	and e.gear_type_id = ad.gear_type_id
	and e.catch_type_id = ad.catch_type_id
where
	ar.meow_id = 103 ),
	
uncert(eez_id, sector_type_id, period_id, score, layer, period2, year_range)
as ( select *
from web.uncertainty_eez ue
left join web.uncertainty_time_period utp on ue.period_id = utp.period_id)
	
select
	103 meow_id,
	0 sub_area_id,
	19 marine_layer_id,
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
--changed c.unit_price to p.price
 sum(coalesce (p.price,
		null,
		1466) * c.total_catch) real_value,
	web.ppr(sum(c.total_catch),
	max(t.tl)) ppr,
	sum(c.total_catch * t.tl) tl,
	sum(c.total_catch * t.sl_max) sl
into table web.test_vfact
from
	catch c
join web.cube_dim_taxon t on
	(t.taxon_key = c.taxon_key)
join web.area a on
	(a.marine_layer_id = 19
	and a.main_area_id = c.main_area_id)
join web.catch_type ct on
	(ct.catch_type_id = c.catch_type_id)
join web.reporting_status rs on
	(rs.reporting_status_id = c.reporting_status_id)
join web.time tm on
	(tm.time_business_key = c.year)
-- added join for new price table to include end use
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
	meow_id,
	sub_area_id,
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
	u.score

