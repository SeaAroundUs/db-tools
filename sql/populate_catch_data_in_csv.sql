select format('TRUNCATE TABLE web_cache.catch_csv_4_%s;', rfmo_id) ||
  format('insert into web_cache.catch_csv_4_%s(entity_layer_id, entity_id, csv_data) select 4, %1$s, web.f_catch_data_in_csv(array[%1$s], 4)', rfmo_id) ||
  format(';VACUUM ANALYZE  web_cache.catch_csv_4_%s;', rfmo_id)
  from web.rfmo
 where rfmo_id != 9;
