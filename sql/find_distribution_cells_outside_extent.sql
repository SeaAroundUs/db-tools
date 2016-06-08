select format( 
  'insert into log.distribution_outside_extent
   select td.taxon_key, array_agg(g.id)
     from distribution.taxon_distribution td
     join distribution.grid g on (g.id = td.cell_id)
     join distribution.taxon_extent te on (te.taxon_key = td.taxon_key and not st_dwithin(te.geom, g.geom, 0.1))
    where td.taxon_key = %s
    group by td.taxon_key', t.taxon_key)
from (select distinct taxon_key from distribution.taxon_distribution where not is_backfilled) as t;
