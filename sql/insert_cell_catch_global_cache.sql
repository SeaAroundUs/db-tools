SELECT
  format(
    'INSERT INTO web.cell_catch_global_cache(year,result) SELECT %s, f_spatial_catch_json(array[%1$s])',
    t.time_business_key
  )
  FROM web.time t;
