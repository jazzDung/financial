

with validation as (

    select
        ticker as test_field

    from "financial_data"."sources"."balance_sheet"

),

validation_errors as (

    select
        test_field
    from validation
    
    where LENGTH(test_field) != 3

)

select *
from validation_errors

