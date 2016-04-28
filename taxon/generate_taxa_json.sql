refresh materialized view distribution.v_taxon_with_extent; 
refresh materialized view distribution.v_taxon_with_distribution; 
vacuum analyze distribution.v_taxon_with_extent; 
vacuum analyze distribution.v_taxon_with_distribution;

\a
\t
\o d:/seaaroundus/db-tools/taxon/fish_base.json
select json_build_object('name', 'Fishbase',
                         'children', 
                         (select json_agg((replace(master.taxon_child_tree(p.lineage)::text, ',"children":[]', ''))::json) 
                            from log.taxon_catalog p 
                           where p.taxon_level_id = 1 and not p.is_retired and p.taxon_key::varchar like '10%')
                        );
\o
  
\a
\t
\o d:/seaaroundus/db-tools/taxon/pisces.json
select json_build_object('name', 'Pisces', 'key', '', 'lineage', 'Pisces.6',
                         'children', (select replace(t.d::text, ',"children":[]', '') from master.taxon_child_tree('Pisces.6'::ltree) as t(d))::json
                        );
\o

\o d:/seaaroundus/db-tools/taxon/slb.json
with slb_top(lineage) as (
  select distinct subpath(lineage, 0, 1) 
    from master.taxon 
   where cla_code::text like '90%' 
   order by 1
)
select json_build_object('name', 'SLB', 'key', '', 'lineage', 'SLB',
                         'children', (select json_agg(replace(t.d::text, ',"children":[]', '')::json) 
                                        from slb_top s
                                        join lateral master.taxon_child_tree(s.lineage) as t(d) on (true))
                        );
\o

fb img:http://fishbase.org/Photos/ThumbnailsSummary.php?Genus=Thunnus&Species=albacares

\a
\t
\o d:/seaaroundus/db-tools/taxon/treemap/fishing_entity.json
with catch as (
  select v.fishing_entity_id,sum(v.catch_sum) as catch,sum(v.real_value) as value 
    from v_fact_data v where v.marine_layer_id in (1,2) group by v.fishing_entity_id
),
tally as (
  select min(catch) min_catch, max(catch) max_catch, min(value) min_value, max(value) max_value
    from catch c
)
select json_build_object(
         'id', 0, 
         'name', 'All Fishing Entities',
         'children', (select json_agg(c.*)
                        from (select v.fishing_entity_id as id, fe.name, 
                                     v.catch::numeric(30,2), (width_bucket(v.catch, t.min_catch, t.max_catch, 20) - 1) catch_bucket,  
                                     v.value::numeric(30,2), (width_bucket(v.value, t.min_value, t.max_value, 20) - 1) value_bucket 
                                from catch v, fishing_entity fe, tally t 
                               where v.fishing_entity_id=fe.fishing_entity_id) as c)
       );
\o
