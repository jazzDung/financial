select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ticker
from "financial_data"."sources"."cash_flow"
where ticker is null



      
    ) dbt_internal_test