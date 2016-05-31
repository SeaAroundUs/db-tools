insert into recon.validation_result(rule_id, id)
select 1, * from recon.v_raw_catch_amount_zero_or_negative
union all
select 2, * from recon.v_raw_catch_fishing_entity_and_eez_not_aligned              
union all
select 3, * from recon.v_raw_catch_input_reconstructed_reporting_status_reported
union all
select 4, * from recon.v_raw_catch_input_not_reconstructed_reporting_staus_unreported
union all
select 5, * from recon.v_raw_catch_layer_not_in_range
union all
select 6, * from recon.v_raw_catch_lookup_mismatch
union all
select 7, * from recon.v_raw_catch_missing_required_field                          
union all
select 8, * from recon.v_raw_catch_taxa_is_rare
union all
select 100, * from recon.v_raw_catch_layer_2_or_3_and_sector_not_industrial   
union all
select 101, * from recon.v_raw_catch_amount_greater_than_threshold
union all
select 102, * from recon.v_raw_catch_fao_21_nafo_null                                
union all
select 103, * from recon.v_raw_catch_fao_27_ices_null
union all
select 104, * from recon.v_raw_catch_original_country_fishing_not_null
union all
select 105, * from recon.v_raw_catch_original_sector_not_null
union all
select 106, * from recon.v_raw_catch_original_taxon_not_null
union all
select 107, * from recon.v_raw_catch_peru_catch_amount_greater_than_threshold
union all
select 108, * from recon.v_raw_catch_subsistence_and_layer_not_1
union all
select 109, * from recon.v_raw_catch_year_max
;
