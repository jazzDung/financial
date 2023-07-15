





with validation_errors as (

    select
        ticker, year
    from "financial_data"."financial_raw"."income_statement"
    group by ticker, year
    having count(*) > 1

)

select *
from validation_errors


