select format('insert into allocation.allocation_result_ifa
              
              with base as (
                select
                  r.universal_data_id as universal_data_id,
                  cr.area_id as eez_id,
                  cr.fao_area_id as fao_area_id,
                  sum(r.allocated_catch*cr.water_area / c.water_area) as total_catch
                from
                  allocation.simple_area_cell_assignment_raw as cr
                join allocation.cell as c on
                  (c.cell_id = cr.cell_id)
                join allocation_partition.allocation_result_%s as r on
                  (r.cell_id = cr.cell_id)
                where
                  cr.marine_layer_id = 14
                group by
                  r.universal_data_id,
                  cr.area_id,
                  cr.fao_area_id)

               select *
               from base', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;

