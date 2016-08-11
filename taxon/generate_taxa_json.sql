refresh materialized view distribution.v_taxon_with_extent; 
refresh materialized view distribution.v_taxon_with_distribution; 
vacuum analyze distribution.v_taxon_with_extent; 
vacuum analyze distribution.v_taxon_with_distribution;

\a      
\t
\o d:/seaaroundus/db-tools/taxon/fishes.json
select 
  replace(json_build_object('name', 'Fishes', 'key', 'N/A', 'lineage', 'root',
                            'children', 
                            (select json_agg(t.*) 
                               from (select p.taxon_key as key, p.common_name as name, p.level, p.lineage, p.parent,
                                            p.is_distribution_available as is_dist, p.is_extent_available as is_extent,
                                            to_char(p.total_catch, '999,999,999,999,999.99') as total_catch,
                                            to_char(p.total_value, '999,999,999,999,999.99') as total_value, master.taxon_child_tree(p.lineage) as children 
                                       from master.v_taxon_lineage p    
                                      where p.level = 1 and p.taxon_key::varchar like '10%'
                                      order by p.common_name) as t
                            )
          )::text, 
          ',"children":[]',            
          ''
         );
\o
  
\o d:/seaaroundus/db-tools/taxon/nonfishes.json
select 
  replace(json_build_object('name', 'Non-Fishes', 'key', 'N/A', 'lineage', 'root',
                            'children', 
                            (select json_agg(t.*) 
                               from (select p.taxon_key as key, p.common_name as name, p.level, p.lineage, p.parent,
                                            p.is_distribution_available as is_dist, p.is_extent_available as is_extent,
                                            to_char(p.total_catch, '999,999,999,999,999.99') as total_catch,
                                            to_char(p.total_value, '999,999,999,999,999.99') as total_value, master.taxon_child_tree(p.lineage) as children 
                                       from master.v_taxon_lineage p    
                                      where p.level = 1 and p.taxon_key::varchar like '19%'
                                      order by p.common_name) as t
                            )
          )::text, 
          ',"children":[]',            
          ''
         );
\o

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
