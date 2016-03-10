select format('insert into allocation.allocation_result_eez(universal_data_id,eez_id,fao_area_id,total_catch)
               select r.universal_data_id, a.inherited_att_belongs_to_reconstruction_eez_id, a.fao_area_id, sum(r.allocated_catch)
                 from allocation.allocation_simple_area a 
                 join allocation_partition.allocation_result_%s as r on (r.allocation_simple_area_id = a.allocation_simple_area_id)
                where a.inherited_att_belongs_to_reconstruction_eez_id > 0 
                group by r.universal_data_id, a.inherited_att_belongs_to_reconstruction_eez_id, a.fao_area_id', m.partition_id)
  from allocation.allocation_result_partition_map m
 order by m.partition_id;
