



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

