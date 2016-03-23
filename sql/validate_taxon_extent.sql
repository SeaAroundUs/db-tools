select format('insert into rfmo.invalid_taxon_extent 
        select gid, taxon_key 
          from distribution.taxon_extent
         where not st_isvalid(geom)
           and gid between %s and %s', g.id+1, g.id+100) 
  from generate_series((select min(gid)-1 from distribution.taxon_extent), (select max(gid) from distribution.taxon_extent), 100) as g(id);
