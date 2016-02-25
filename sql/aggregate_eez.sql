select 'INSERT INTO web.v_fact_data(
          main_area_id,sub_area_id,marine_layer_id,data_layer_id,fishing_entity_id,time_key,year,taxon_key,area_key,
          catch_type_id,catch_status,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required,
          catch_trophic_level,catch_max_length
        )
        WITH catch(main_area_id,sub_area_id,data_layer_id,year,taxon_key,fishing_entity_id, catch_type_id,catch_status,reporting_status,sector_type_id,total_catch,unit_price) AS (
          SELECT ar.eez_id, 
                 ar.fao_area_id,
                 ad.data_layer_id,
                 ad.year,
                 ad.taxon_key, 
                 ad.fishing_entity_id,                                                         
                 ad.catch_type_id,
                 (case when ad.catch_type_id in (1, 3) then ''R'' when ad.catch_type_id = 2 then ''D'' else null end)::CHAR(1), 
                 (case when ad.catch_type_id = 1 then ''R'' when ad.catch_type_id in (2, 3) then ''U'' else null end)::CHAR(1), 
                 ad.sector_type_id,
                 ar.total_catch, 
                 ad.unit_price
		    FROM allocation.allocation_result_eez ar
		    JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)
		   WHERE ar.eez_id = ' || t.eez_id || 
        ')
        SELECT ' || t.eez_id || ', 
               c.sub_area_id,
               1,
               c.data_layer_id,
               c.fishing_entity_id,                                                         
               tm.time_key,
               c.year,
               c.taxon_key, 
               a.area_key,
               c.catch_type_id,
               c.catch_status,
               c.reporting_status,
               c.sector_type_id,
               sum(c.total_catch), 
               sum(c.unit_price * c.total_catch),
               web.ppr(sum(c.total_catch), MAX(t.tl)),
               sum(c.total_catch * t.tl),
               sum(c.total_catch * t.sl_max)
		  FROM catch c
          JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)
          JOIN web.area a ON (a.marine_layer_id = 1 AND a.main_area_id = c.main_area_id AND a.sub_area_id = c.sub_area_id)
		  JOIN web.time tm ON (tm.time_business_key = c.year)
         GROUP BY c.sub_area_id, c.data_layer_id, c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.catch_status, c.reporting_status'
  from (select distinct ar.eez_id from allocation.allocation_result_eez ar) as t
  order by t.eez_id;

