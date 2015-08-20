select 'update allocation.allocation_data ad set unit_price = p.price from allocation.price p where ad.unit_price is null and ad.year = p.year and ad.fishing_entity_id = p.fishing_entity_id and ad.taxon_key = p.taxon_key and ad.year = ' || t.time_business_key
  from time t
 order by t.time_business_key;
