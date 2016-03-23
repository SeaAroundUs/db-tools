SELECT
  format(
    'WITH catch(fishing_entity_id, cell_id, sector_type_id, catch_type_id, catch_sum) AS (
       SELECT ad.fishing_entity_id, ar.cell_id, ad.sector_type_id, ad.catch_type_id,sum(ar.allocated_catch)::numeric
         FROM allocation.allocation_data ad
         JOIN allocation.v_allocation_result_eez_unique_universal_data_id AS vu ON (vu.universal_data_id = ad.universal_data_id)
         JOIN allocation.allocation_result ar ON (ar.universal_data_id = ad.universal_data_id)
        WHERE ad.year = %s
          AND ad.taxon_key = %s
        GROUP BY ad.fishing_entity_id, ar.cell_id, ad.sector_type_id, ad.catch_type_id
    )
    INSERT INTO web_partition.cell_catch_p%1$s 
    (
      year,
      taxon_key,
      fishing_entity_id,
      cell_id,
      commercial_group_id,              
      functional_group_id,
      sector_type_id,
      catch_status,
      reporting_status,
      catch_sum
    )
    SELECT %1$s, %2$s, c.fishing_entity_id, c.cell_id, cdt.commercial_group_id, cdt.functional_group_id, c.sector_type_id,
           (case when c.catch_type_id in (1, 3) then ''R'' when c.catch_type_id = 2 then ''D'' else null end)::CHAR(1), 
           (case when c.catch_type_id = 1 then ''R'' when c.catch_type_id in (2, 3) then ''U'' else null end)::CHAR(1), 
           catch_sum
      FROM catch c
      JOIN web.cube_dim_taxon cdt ON (cdt.taxon_key = %2$s)',
    t.time_business_key,
    taxon.taxon_key
  )
  FROM web.time t, web.v_web_taxon taxon
 ORDER BY t.time_business_key;
