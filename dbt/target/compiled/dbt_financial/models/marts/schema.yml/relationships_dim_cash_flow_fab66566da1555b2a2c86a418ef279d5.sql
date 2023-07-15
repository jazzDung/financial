

    


with parent as (

    select
        ticker as id

    from "financial_data"."financial_clean"."dim_organization_overview"

),

child as (

    select
        ticker as id

    from "financial_data"."financial_clean"."dim_cash_flow"

)

select *
from child
where id is not null
  and id not in (select id from parent)

