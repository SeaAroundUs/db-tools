select 'insert into allocation.allocation_result_global
        SELECT universal_data_id, 
               1, 
               sum(total_catch)
          FROM allocation.allocation_result_eez
         where eez_id = ' || eez_id ||  
       ' group by universal_data_id'
  from web.eez
 order by eez_id;
select 'insert into allocation.allocation_result_global
        SELECT universal_data_id,
               2, 
               sum(total_catch) 
          FROM allocation.allocation_result_eez
         where eez_id = 0
           and fao_area_id = ' || fao_area_id ||
       ' group by universal_data_id'
  from web.fao_area
 order by fao_area_id;
