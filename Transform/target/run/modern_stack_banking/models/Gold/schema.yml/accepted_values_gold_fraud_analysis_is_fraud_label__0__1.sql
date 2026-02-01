
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_fraud_label as value_field,
        count(*) as n_records

    from "argus_vault"."main"."gold_fraud_analysis"
    group by is_fraud_label

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test