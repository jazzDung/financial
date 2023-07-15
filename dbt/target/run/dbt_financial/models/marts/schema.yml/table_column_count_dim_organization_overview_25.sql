select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



with validation as (

    select count(table_name) as test_field
    from information_schema.columns
    where table_name = model_name

),

validation_errors as (

    select
        test_field
    from validation
    
    where test_field != count

)

select *
from validation_errors


      
    ) dbt_internal_test