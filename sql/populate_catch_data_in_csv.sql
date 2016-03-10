select 'insert into web_cache.catch_csv_4_' || rfmo_id || '(entity_layer_id, entity_id, csv_data) select 4, ' || rfmo_id || ', f_catch_data_in_csv(array[' || rfmo_id || '],4)' 
  from rfmo 
 where rfmo_id != 9;
