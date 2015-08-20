select 'insert into allocation.allocation_result_eez
        SELECT distinct r.universal_data_id, 
               a.inherited_att_belongs_to_reconstruction_eez_id, 
               a.fao_area_id, 
               sum(r.allocated_catch)
          FROM allocation.allocation_result r 
          join allocation.allocation_simple_area a on (r.allocation_simple_area_id = a.allocation_simple_area_id)
         where a.inherited_att_belongs_to_reconstruction_eez_id = ' || e.eez_id || 
       ' group by r.universal_data_id, a.inherited_att_belongs_to_reconstruction_eez_id, a.fao_area_id'
  from web.eez e
  left join allocation.allocation_result_eez ar on (ar.eez_id = e.eez_id)
 where ar.eez_id is null
 order by e.eez_id;
select 'insert into allocation.allocation_result_eez
        SELECT distinct r.universal_data_id, 
               0, 
               a.fao_area_id, 
               sum(r.allocated_catch)
          FROM allocation.allocation_result r 
          join allocation.allocation_simple_area a on (r.allocation_simple_area_id = a.allocation_simple_area_id)
         where a.inherited_att_belongs_to_reconstruction_eez_id = 0 
           and a.fao_area_id = ' || f.fao_area_id || 
       ' group by r.universal_data_id, a.fao_area_id'
  from web.fao_area f
  left join allocation.allocation_result_eez ar on (ar.fao_area_id = f.fao_area_id and ar.eez_id = 0)
 where ar.fao_area_id is null
 order by f.fao_area_id;
