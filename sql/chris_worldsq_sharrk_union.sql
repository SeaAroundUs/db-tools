Select
'Insert into sw_by_shark_grid
Select s.shark_gid, st_union(sw_intersection)::geometry(MultiPolygon,4326) 
from shark_worldsq_int_s
where s.shark_gid
and s.gid between ' || g.id+1 || ' and ' || g.id+5
group by s.shark_gid
from generate_series(0,(select max(shark_gid) from shark_worldsq_int), 5) g(id);