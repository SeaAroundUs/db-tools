select 'INSERT INTO web.v_fact_data(
          main_area_id,sub_area_id,marine_layer_id, data_layer_id, fishing_entity_id,time_key,year,taxon_key,area_key,
          catch_type_id,catch_status,reporting_status_id,reporting_status,sector_type_id,catch_sum,real_value,primary_production_required,
          catch_trophic_level,catch_max_length
        )
        WITH catch(main_area_id,year,taxon_key, data_layer_id, fishing_entity_id, catch_type_id,reporting_status_id,sector_type_id,total_catch,unit_price) AS (
          SELECT am.meow_id,
                 ad.year,
                 ad.taxon_key, 
                 ad.data_layer_id,
                 ad.fishing_entity_id,                                                         
                 ad.catch_type_id,
                 ad.reporting_status_id,
                 ad.sector_type_id,
                 am.total_catch,
                 ad.unit_price
		    FROM allocation.allocation_result_meow am
		    JOIN allocation.allocation_data ad ON (ad.universal_data_id = am.universal_data_id)
		   WHERE am.meow_id = ' || meow.meow_id || 
        ')
        SELECT ' || meow.meow_id || ', 
               0,
               19,
               c.data_layer_id,
               c.fishing_entity_id,                                                         
               tm.time_key,
               c.year,
               c.taxon_key, 
               a.area_key,
               c.catch_type_id,
               MAX(ct.abbreviation),
               c.reporting_status_id,
               MAX(rs.abbreviation),
               c.sector_type_id,
               sum(c.total_catch), 
               sum(c.unit_price * c.total_catch),
               web.ppr(sum(c.total_catch), MAX(t.tl)),
               sum(c.total_catch * t.tl),
               sum(c.total_catch * t.sl_max)
		  FROM catch c
          JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)
          JOIN web.area a ON (a.marine_layer_id = 19 AND a.main_area_id = c.main_area_id)
          JOIN web.catch_type ct ON (ct.catch_type_id = c.catch_type_id)
          JOIN web.reporting_status rs ON (rs.reporting_status_id = c.reporting_status_id)
		  JOIN web.time tm ON (tm.time_business_key = c.year)
         GROUP BY c.data_layer_id, c.fishing_entity_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.reporting_status_id'
  from web.meow
 order by meow_id; 
