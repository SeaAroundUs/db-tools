select 'insert into allocation.allocation_result_rfmo
        select r.universal_data_id,
               cr.area_id, 
               sum(r.allocated_catch*cr.water_area/(c.water_area))
          from allocation.simple_area_cell_assignment_raw cr 
          join allocation.cell c on (c.cell_id = cr.cell_id) 
          join allocation.allocation_result r on (r.cell_id = c.cell_id)
         where cr.marine_layer_id = 4 and cr.area_id = ' || r.rfmo_id || 
       ' group by r.universal_data_id, cr.area_id'
  from web.rfmo r
  left join allocation.allocation_result_rfmo ar on (ar.rfmo_id = r.rfmo_id)
 where ar.rfmo_id is null
 order by r.rfmo_id;
