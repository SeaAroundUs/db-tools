select format('insert into allocation.allocation_result_rfmo
               select r.universal_data_id, cr.area_id, asa.inherited_att_belongs_to_reconstruction_eez_id, sum(r.allocated_catch*cr.water_area/c.water_area)
                 from allocation.simple_area_cell_assignment_raw as cr 
                 join allocation.cell as c on (c.cell_id = cr.cell_id)
                 join allocation_partition.allocation_result_%s as r on (r.cell_id = cr.cell_id)
                 join allocation.allocation_simple_area asa on asa.allocation_simple_area_id = r.allocation_simple_area_id
                where cr.marine_layer_id = 4 
                group by r.universal_data_id, cr.area_id, asa.inherited_att_belongs_to_reconstruction_eez_id', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;
