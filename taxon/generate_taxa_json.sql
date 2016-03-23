\o d:/seaaroundus/db-tools/taxon/pisces.json
select json_build_object('name', 'Pisces', 'key', '', 'lineage', 'Pisces.6',
                         'children', (select replace(t.d::text, ',"children":[]', '') from log.taxon_child_tree('Pisces.6'::ltree) as t(d))::json
                        );
\o
\o d:/seaaroundus/db-tools/taxon/slb.json
with slb_top(lineage) as (
  select distinct subpath(lineage, 0, 1) 
    from log.taxon 
   where cla_code::text like '90%' 
   order by 1
)
select json_build_object('name', 'SLB', 'key', '', 'lineage', 'SLB',
                         'children', (select json_agg(replace(t.d::text, ',"children":[]', '')::json) 
                                        from slb_top s
                                        join lateral log.taxon_child_tree(s.lineage) as t(d) on (true))
                        );
\o

