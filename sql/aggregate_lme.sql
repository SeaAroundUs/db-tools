select 'INSERT INTO web.v_fact_data(
          main_area_id,sub_area_id,marine_layer_id, data_layer_id, fishing_entity_id,gear_id,time_key,year,taxon_key,area_key,
          catch_type_id,catch_status,reporting_status_id,reporting_status,sector_type_id,end_use_type_id, score, catch_sum,real_value,primary_production_required,
          catch_trophic_level,catch_max_length
        )
        with catch(main_area_id,
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
                   unit_price,
                   eez_id) 
        AS ( SELECT ar.lme_id,
                    ad.year,
                    ad.taxon_key,
                    ad.data_layer_id,
                    ad.fishing_entity_id,
                    ad.gear_type_id,
                    ad.catch_type_id,
                    ad.reporting_status_id,
                    ad.sector_type_id,
                    coalesce (e.end_use_type_id ,1) as end_use_type_id,
                    ar.total_catch *  (coalesce (e.end_use_percentage,null, 1)),
                    ad.unit_price,
                    ar.eez_id
            FROM allocation.allocation_result_lme ar
            JOIN allocation.allocation_data ad ON (ad.universal_data_id = ar.universal_data_id)
            left join web.end_use e ON e.fishing_entity_id = ad.fishing_entity_id 
            and e.year = ad.year 
            and e.taxon_key = ad.taxon_key 
            and e.sector_type_id = ad.sector_type_id 
            and e.reporting_status_id = ad.reporting_status_id
            and e.gear_type_id = ad.gear_type_id
            and e.catch_type_id = ad.catch_type_id
            WHERE ar.lme_id = ' || lme.lme_id || '
        ),

        uncert(eez_id, sector_type_id, period_id, score, layer, period2, year_range)
        as ( select *
        from web.uncertainty_eez ue
        left join web.uncertainty_time_period utp on ue.period_id = utp.period_id)

        SELECT ' || lme.lme_id || ', 
               0,
               3,
               c.data_layer_id,
               c.fishing_entity_id,
               c.gear_id,
               tm.time_key,
               c.year,
               c.taxon_key, 
               a.area_key,
               c.catch_type_id,
               MAX(ct.abbreviation),
               c.reporting_status_id,
               MAX(rs.abbreviation),
               c.sector_type_id,
               c.end_use_type_id,
               coalesce(u.score,null,1) score,
               sum(c.total_catch), 
               sum(c.unit_price * c.total_catch),
               web.ppr(sum(c.total_catch), MAX(t.tl)),
               sum(c.total_catch * t.tl),
               sum(c.total_catch * t.sl_max)
          FROM catch c
          JOIN web.cube_dim_taxon t ON (t.taxon_key = c.taxon_key)
          JOIN web.area a ON (a.marine_layer_id = 3 AND a.main_area_id = c.main_area_id)
          JOIN web.catch_type ct ON (ct.catch_type_id = c.catch_type_id)
          JOIN web.reporting_status rs ON (rs.reporting_status_id = c.reporting_status_id)
          JOIN web.time tm ON (tm.time_business_key = c.year)
          JOIN web.gear g ON (g.gear_id = c.gear_id)
          left join uncert u on u.eez_id = c.eez_id
          and u.sector_type_id = c.sector_type_id
          and u.layer = c.data_layer_id
          and  c.year <@ u.year_range
         GROUP BY c.end_use_type_id ,c.data_layer_id, c.fishing_entity_id, c.gear_id, tm.time_key, c.year, c.taxon_key, a.area_key, c.sector_type_id, c.catch_type_id, c.reporting_status_id, u.score'
  from web.lme
 order by lme_id; 
