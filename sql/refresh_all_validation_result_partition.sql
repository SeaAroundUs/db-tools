select 'select recon.refresh_validation_result_partition(' || rule_id || ')' from recon.validation_rule;
select 'vacuum analyze validation_partition.' || table_name from schema_v('validation_partition') where table_name not like 'TOTALS%';
