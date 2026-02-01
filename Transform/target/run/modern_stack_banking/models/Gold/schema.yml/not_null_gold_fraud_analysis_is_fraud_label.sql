
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_fraud_label
from "argus_vault"."main"."gold_fraud_analysis"
where is_fraud_label is null



  
  
      
    ) dbt_internal_test