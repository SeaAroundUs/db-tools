select web_partition.maintain_cell_catch_indexes(table_name) 
from schema_v('web_partition')
where table_name not like 'TOTALS%';
