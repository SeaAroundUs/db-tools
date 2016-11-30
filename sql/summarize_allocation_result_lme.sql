select format('insert into allocation.allocation_result_lme
               with base as (select r.universal_data_id, cr.area_id, asa.fao_area_id, sum(r.allocated_catch*cr.water_area/c.water_area) as TotalCatch
                 from allocation.simple_area_cell_assignment_raw as cr
                 join allocation.cell as c on (c.cell_id = cr.cell_id)
                 join allocation_partition.allocation_result_%s as r on (r.cell_id = cr.cell_id)
                 join allocation.allocation_simple_area asa on asa.allocation_simple_area_id = r.allocation_simple_area_id
                where cr.marine_layer_id = 3
                group by r.universal_data_id, cr.area_id, asa.fao_area_id),
               final as (select universal_data_id, area_id as LME_ID, sum(TotalCatch)
                 from base b join fao_lme f on b.area_id = f.lme_number and b.fao_area_id = f.fao_area_id
                   group by universal_data_id, area_id
                 )
               select *
               from final', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;

