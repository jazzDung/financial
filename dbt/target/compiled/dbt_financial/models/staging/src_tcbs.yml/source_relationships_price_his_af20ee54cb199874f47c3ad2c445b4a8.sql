

    


with parent as (

    select
        ticker as id

    from "financial_data"."sources"."organization"

),

child as (

    select
        ticker as id

    from "financial_data"."sources"."price_history"

)

select *
from child
where id is not null
  and id not in (select id from parent)

