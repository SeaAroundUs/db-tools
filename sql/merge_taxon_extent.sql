select format('update log.extent_merge m set geom = (select st_multi(st_union(e.geom)) from distribution.taxon_extent e where e.taxon_key = %s) where taxon_key = %1$s', m.taxon_key)  
  from log.extent_merge m
 order by m.taxon_key;
