





with validation_errors as (

    select
        ticker, _airbyte_normalized_at
    from "financial_data"."financial_raw"."general_rating"
    group by ticker, _airbyte_normalized_at
    having count(*) > 1

)

select *
from validation_errors

