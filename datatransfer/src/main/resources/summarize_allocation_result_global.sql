select 'insert into allocation.allocation_result_global
        SELECT universal_data_id, 
               1, 
               sum(total_catch)
          FROM allocation.allocation_result_eez
         where eez_id != 0
           and universal_data_id between ' || (g.idx+1) || ' and ' || (g.idx+200000) ||
       ' group by universal_data_id'
  from generate_series((select min(universal_data_id)-1 from allocation_result_eez where eez_id != 0), (select max(universal_data_id) from allocation_result_eez where eez_id != 0), 200000) as g(idx);
select 'insert into allocation.allocation_result_global
        SELECT universal_data_id,
               2, 
               sum(total_catch) 
          FROM allocation.allocation_result_eez
         where eez_id = 0
           and universal_data_id between ' || (g.idx+1) || ' and ' || (g.idx+200000) ||
       ' group by universal_data_id'
  from generate_series((select min(universal_data_id)-1 from allocation_result_eez where eez_id = 0), (select max(universal_data_id) from allocation_result_eez where eez_id = 0), 200000) as g(idx);
