select format('insert into allocation_data_partition.allocation_data_%s(universal_data_id,fishing_entity_id,year,taxon_key,sector_type_id,catch_type_id,reporting_status_id,gear_type_id)
               select ad.universal_data_id,ad.fishing_entity_id,ad.year,ad.taxon_key,ad.sector_type_id,ad.catch_type_id,ad.reporting_status_id,ad.gear_type_id
                 from allocation.allocation_data ad
                where ad.year = %1$s',
              t.time_business_key) 
  from web.time t
 order by t.time_business_key;
select format('vacuum analyze allocation_data_partition.allocation_data_%s', t.time_business_key) 
  from web.time t
 order by t.time_business_key;
 