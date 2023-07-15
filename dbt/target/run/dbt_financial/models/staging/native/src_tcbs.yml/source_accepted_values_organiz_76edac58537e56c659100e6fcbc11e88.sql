select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        exchange as value_field,
        count(*) as n_records

    from "financial_data"."financial_raw"."organization_overview"
    group by exchange

)

select *
from all_values
where value_field not in (
    'UPCOM','HNX'
)



      
    ) dbt_internal_test