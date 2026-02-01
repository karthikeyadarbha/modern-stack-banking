
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from "argus_vault"."main"."gold_fraud_analysis"
where amount is null



  
  
      
    ) dbt_internal_test