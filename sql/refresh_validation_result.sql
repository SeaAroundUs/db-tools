select format('select recon.refresh_validation_result_partition(%s)', rule_id) from recon.validation_rule 
where rule_id between 400 and 403
; 
