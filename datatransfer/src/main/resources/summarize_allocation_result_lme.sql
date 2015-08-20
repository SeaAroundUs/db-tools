select 'insert into allocation.allocation_result_lme
        select r.universal_data_id,
               cr.area_id, 
               sum(r.allocated_catch*cr.water_area/(c.water_area))
          from allocation.simple_area_cell_assignment_raw cr 
          join allocation.cell c on (c.cell_id = cr.cell_id) 
          join allocation.allocation_result r on (r.cell_id = c.cell_id)
         where cr.marine_layer_id = 3 and cr.area_id = ' || l.lme_id || 
       ' group by r.universal_data_id, cr.area_id'
  from web.lme l
  left join allocation.allocation_result_lme ar on (ar.lme_id = l.lme_id)
 where ar.lme_id is null
 order by l.lme_id;
