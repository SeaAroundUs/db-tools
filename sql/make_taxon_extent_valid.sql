select 'update taxon_extent set geom=st_makevalid(geom) where not st_isvalid(geom) and taxon_key = ' || taxon_key from distribution.taxon_extent;
