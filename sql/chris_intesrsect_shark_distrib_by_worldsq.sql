Select
'Insert into shark_worldsq_int 
Select w.gid world_gid, s.gid shark_gid, st_multi(ST_intersection(w.geom, s.geom))::geometry(MultiPolygon,4326) sw_intersection
from worldsq w, shark_distribution s
where ST_intersects(w.geom, s.geom)
and s.gid = 2402
and w.gid between ' || g.id+1 || ' and ' || g.id+200
from generate_series(0,(select max(gid) from worldsq), 200) g(id);