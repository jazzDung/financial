select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select ticker as from_field
    from "financial_data"."financial_clean"."dim_price_history"
    where ticker is not null
),

parent as (
    select ticker as to_field
    from "financial_data"."financial_clean"."dim_organization"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test