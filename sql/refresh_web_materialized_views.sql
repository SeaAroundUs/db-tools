select 'refresh materialized view web.' || table_name from matview_v('web') where table_name not like 'TOTAL%';
