




    with grouped_expression as (
    select
        
        
    
  ticker is not null as expression


    from "financial_data"."financial_raw"."general_rating"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors



