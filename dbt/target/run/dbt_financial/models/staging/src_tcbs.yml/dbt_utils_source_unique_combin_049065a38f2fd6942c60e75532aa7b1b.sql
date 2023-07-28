select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        ticker, quarter, year
    from "financial_data"."sources"."balance_sheet"
    group by ticker, quarter, year
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test