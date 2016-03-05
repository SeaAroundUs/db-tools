Select
'Insert into sw_by_shark_grid
Select s.shark_gid, st_union(sw_intersection)::geometry(MultiPolygon,4326) 
from shark_worldsq_int s
where s.shark_gid between ' || g.id+1 || ' and ' || g.id+5 ||
' group by s.shark_gid'
from generate_series(0,(select max(shark_gid) from shark_worldsq_int), 5) g(id);\

Select
'Insert into sw_by_world_gid
Select s.world_gid, st_union(sw_intersection)::geometry(MultiPolygon,4326) 
from shark_worldsq_int s
where s.world_gid between ' || g.id+1 || ' and ' || g.id+5 ||
' group by s.world_gid'
from generate_series(0,(select max(world_gid) from shark_worldsq_int), 5) g(id);