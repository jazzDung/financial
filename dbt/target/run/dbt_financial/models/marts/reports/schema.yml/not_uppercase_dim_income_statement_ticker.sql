select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    

with validation as (

    select
        ticker as test_field

    from "financial_data"."financial_clean"."dim_income_statement"

),

validation_errors as (

    select
        test_field
    from validation
    
    where test_field != UPPER(test_field)

)

select *
from validation_errors


      
    ) dbt_internal_test