

    

with validation as (

    select
        ticker as test_field

    from "financial_data"."marts"."dim_general_rating"

),

validation_errors as (

    select
        test_field
    from validation
    
    where test_field != UPPER(test_field)

)

select *
from validation_errors
