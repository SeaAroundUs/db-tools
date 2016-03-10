select format('delete from allocation_partition.allocation_result_%1$s where allocated_catch = 0', partition_id) 
  from allocation_result_partition_map; 
