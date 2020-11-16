select format('insert into allocation.allocation_result_lme
                with base (universal_data_id, area_id, fao_area_id, total_catch, eez_id) as (
                select
                  r.universal_data_id,
                  cr.area_id,
                  asa.fao_area_id,
                  sum(r.allocated_catch*cr.water_area / c.water_area) as total_catch,
                  asa.inherited_att_belongs_to_reconstruction_eez_id
                from
                  allocation.simple_area_cell_assignment_raw as cr
                join allocation.cell as c on
                  (c.cell_id = cr.cell_id)
                join allocation_partition.allocation_result_%s as r on
                  (r.cell_id = cr.cell_id)
                join allocation.allocation_simple_area asa on
                  asa.allocation_simple_area_id = r.allocation_simple_area_id
                where
                  cr.marine_layer_id = 3
                group by
                  r.universal_data_id,
                  cr.area_id,
                  asa.fao_area_id,
                  asa.inherited_att_belongs_to_reconstruction_eez_id),

                  
                final as (
                select
                  b.universal_data_id,
                  b.area_id as lme_id,
                  sum (b.total_catch) total_catch,
                  b.eez_id
                from
                  base b
                join geo.fao_lme f on
                  b.area_id = f.lme_number
                  and b.fao_area_id = f.fao_area_id
                group by
                  b.universal_data_id,
                  b.area_id,
                  b.eez_id )
                  
               select *
               from final', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;

--                 final as (
--                select
--                  b.universal_data_id,
--                  b.area_id as lme_id,
--                  sum (b.total_catch) total_catch,
--                  b.eez_id
