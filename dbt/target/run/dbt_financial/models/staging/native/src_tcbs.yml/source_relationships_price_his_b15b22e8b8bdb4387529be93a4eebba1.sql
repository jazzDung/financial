select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    


with parent as (

    select
        ticker as id

    from "financial_data"."financial_clean"."dim_organization_overview"

),

child as (

    select
        ticker as id

    from "financial_data"."financial_raw"."price_history"

)

select *
from child
where id is not null
  and id not in (select id from parent)


      
    ) dbt_internal_test