





with validation_errors as (

    select
        ticker, quarter, year
    from "financial_data"."sources"."cash_flow"
    group by ticker, quarter, year
    having count(*) > 1

)

select *
from validation_errors


