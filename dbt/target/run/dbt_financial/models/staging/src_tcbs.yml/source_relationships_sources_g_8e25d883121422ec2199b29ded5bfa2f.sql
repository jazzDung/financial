select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    


with parent as (

    select
        ticker as id

    from "financial_data"."sources"."organization"

),

child as (

    select
        ticker as id

    from "financial_data"."sources"."general_rating"

)

select *
from child
where id is not null
  and id not in (select id from parent)


      
    ) dbt_internal_test