select format(
'with dist as (
  select array_agg(td.cell_id) as cell_ids
    from distribution.taxon_distribution td
   where td.is_backfilled
     and td.taxon_key = %s
),
adjacent_cells as (
  select array_agg(ac.seq) neighbor_ids
    from dist d
    join geo.worldsq dc on (dc.seq = any(d.cell_ids))
    join geo.worldsq ac on (ac.seq != dc.seq and st_intersects(ac.geom, dc.geom))
)
insert into distribution.taxon_extent(taxon_key, is_reversed_engineered, geom)
select %1$s, true, st_multi(st_union(w.geom))
  from dist d
  join adjacent_cells ac on (true)
  join geo.worldsq w on (w.seq = any(d.cell_ids || ac.neighbor_ids))', ere.taxon_key)
from log.extent_reverse_eng ere
order by catch_sum desc;
