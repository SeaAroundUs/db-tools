select format('insert into allocation.allocation_result_meow
              with base as (
                select
                  r.universal_data_id as universal_data_id,
                  cr.area_id as area_id,
                  asa.fao_area_id as fao_area_id,
                  sum(r.allocated_catch*cr.water_area / c.water_area) as total_catch,
                  asa.inherited_att_belongs_to_reconstruction_eez_id as eez_id
                from
                  allocation.simple_area_cell_assignment_raw as cr
                join allocation.cell as c on
                  (c.cell_id = cr.cell_id)
                join allocation_partition.allocation_result_%s as r on
                  (r.cell_id = cr.cell_id)
                join allocation.allocation_simple_area asa on
                  asa.allocation_simple_area_id = r.allocation_simple_area_id
                where
                  cr.marine_layer_id = 19
                group by
                  r.universal_data_id,
                  cr.area_id,
                  asa.fao_area_id,
                  asa.inherited_att_belongs_to_reconstruction_eez_id ),

                final as (select
                  universal_data_id,
                  area_id as meow_id,
                  sum(b.total_catch) total_catch
                from
                  base b
                join geo.meow_fao_combo f on
                  b.area_id = f.meow_id
                  and b.fao_area_id = f.fao_area_id
                group by
                  universal_data_id,
                  area_id,
                  eez_id
                 )

               select *
               from final', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;
 
--                 final as (select
--                  universal_data_id,
--                  area_id as meow_id,
--                  sum(b.total_catch) total_catch,
--                  b.eez_id

