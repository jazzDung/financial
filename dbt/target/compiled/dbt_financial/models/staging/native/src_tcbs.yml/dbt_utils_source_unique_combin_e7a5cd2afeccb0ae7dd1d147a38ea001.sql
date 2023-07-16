





with validation_errors as (

    select
        ticker, year
    from "financial_data"."financial_raw"."balance_sheet"
    group by ticker, year
    having count(*) > 1

)

select *
from validation_errors

